import argparse
import json
import hashlib
import os
from typing import Any, Iterator, Optional

from google.cloud import bigquery


top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))


def get_domain_path_parts(domain: str) -> tuple[str, str, str]:
    domain_hash = hashlib.sha1(domain.encode("utf8")).hexdigest()
    parts = (domain_hash[0:2], domain_hash[2:4], domain_hash[4:])
    return parts


def write_domain(date: str, output_dir: str, domain: str, global_rank: int, local_rank: str):
    path_parts = get_domain_path_parts(domain)
    output_path = os.path.join(output_dir, *path_parts[:2])
    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, path_parts[-1]) + ".json"
    if os.path.exists(output_file):
        with open(output_file) as f:
            data = json.load(f)
    else:
        data = [domain, {}]

    assert data[0] == domain

    data[1][date] = [global_rank, local_rank]

    with open(output_file, "w") as f:
        json.dump(data, f)


def get_current_metadata(meta_path: str) -> Optional[dict[str, Any]]:
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            try:
                return json.load(f)
            except Exception:
                pass

    return None


def get_latest_metadata(client: bigquery.Client) -> dict[str, Any]:
    query = r"""SELECT
  tables.table_name AS crux_date
FROM
  `chrome-ux-report.all.INFORMATION_SCHEMA.TABLES` AS tables
WHERE
  tables.table_schema = "all"
  AND REGEXP_CONTAINS(tables.table_name, r"20\d\d\d\d")
ORDER BY
  table_name DESC
LIMIT
  1"""

    rows = list(client.query(query).result())

    if not rows:
        raise ValueError("Expected one CrUX table row, got 0")

    date_str = rows[0]["crux_date"]
    try:
        date = int(date_str)
    except ValueError as e:
        raise ValueError(f"Can't convert {date_str} to int") from e

    return {"date": date}


def get_ranks(client: bigquery.Client, yyyymm: int) -> Iterator[tuple[tuple[int, int], str, int]]:
    query = rf"""SELECT
  `moz-fx-dev-dschubert-wckb.webcompat_knowledge_base.WEBCOMPAT_HOST`(host) AS host,
    global_rank,
    local_rank
FROM
  `moz-fx-dev-dschubert-wckb.crux_imported.host_min_ranks`
WHERE
  yyyymm = {yyyymm} AND
  global_rank < 1000000
"""

    result = client.query(query).result()
    total_rows = result.total_rows
    print(f"Have {total_rows} total domains")
    for i, row in enumerate(result):
        yield (i, total_rows), row["host"], row["global_rank"], row["local_rank"]


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Force update of existing data")
    parser.add_argument("--yyyymm", type=int, help="CrUX data to load")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()

    client = bigquery.Client()

    meta_path = os.path.join(top_dir, "v2", "ranks", "latest.json")
    if not os.path.exists(meta_path):
        os.makedirs(os.path.dirname(meta_path))
    output_path = os.path.join(top_dir, "v2", "ranks", "domains")

    current_metadata = get_current_metadata(meta_path)
    new_metadata = get_latest_metadata(client)
    target_date = args.yyyymm if args.yyyymm else new_metadata["date"]

    if not args.force and current_metadata and current_metadata["date"] >= target_date:
        print(f"Already up to date with CrUX data from {current_metadata['date']}")
        return

    last_progress_update = 0
    for progress, host, global_rank, local_rank in get_ranks(client, target_date):
        progress_percent = int(100 * progress[0] / progress[1])
        if progress_percent != last_progress_update:
            print(f"{progress_percent}%")
            last_progress_update = progress_percent
        write_domain(str(target_date), output_path, host, global_rank, local_rank)

    with open(meta_path, "w") as f:
        json.dump(new_metadata, f, indent=2)


if __name__ == "__main__":
    main()
