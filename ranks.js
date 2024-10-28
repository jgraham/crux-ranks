(function () {
    const trancoScriptSrc = new URL(document.currentScript.src);

    window.getCruxUrl = async function(domain) {
        const msg = new TextEncoder().encode(domain);
        const hashBuffer = await crypto.subtle.digest("SHA-1", msg);
        const sha1 = Array.from(new Uint8Array(hashBuffer))
              .map((b) => b.toString(16).padStart(2, "0"))
              .join("");
        return `${trancoScriptSrc.protocol}//${trancoScriptSrc.hostname}/crux-ranks/ranks/domains/${sha1.slice(0,2)}/${sha1.slice(2,4)}/${sha1.slice(4)}.json`;
    };
})();
