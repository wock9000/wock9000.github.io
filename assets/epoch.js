// epoch.js — cross-chain clock (values only; captions on each segment via title)
// Delimiter: one character, centered in an odd-width slot (default 3ch via CSS).
(function () {
  var BTC_GENESIS = 1231006505;
  var ETH_MERGE = 1663224179;
  var SOL_GENESIS = 1584368400;
  var BTC_INTERVAL = 600;
  var ETH_SLOT = 12;
  var SOL_SLOT = 0.46;

  var LS_KEY = "epochDelimiter";
  var DEFAULT_DELIM = String.fromCodePoint(0x10fed9); // U+10FED9 (Supplementary Private Use Area B)

  function getDelimiter() {
    var s = localStorage.getItem(LS_KEY);
    if (s === null || s === "") return DEFAULT_DELIM;
    var cp = s.codePointAt(0);
    if (cp === undefined) return DEFAULT_DELIM;
    return String.fromCodePoint(cp);
  }

  function buildTicker(el) {
    while (el.firstChild) el.removeChild(el.firstChild);

    function seg(text, title) {
      var s = document.createElement("span");
      s.className = "epoch-seg";
      s.setAttribute("title", title);
      s.textContent = text;
      return s;
    }

    function sep() {
      var s = document.createElement("span");
      s.className = "epoch-sep";
      s.setAttribute("aria-hidden", "true");
      s.textContent = getDelimiter();
      return s;
    }

    var t = Date.now() / 1000;
    var unix = Math.floor(t);
    var btc = Math.floor((t - BTC_GENESIS) / BTC_INTERVAL);
    var eth = Math.floor((t - ETH_MERGE) / ETH_SLOT);
    var sol = Math.floor((t - SOL_GENESIS) / SOL_SLOT);

    el.appendChild(seg(String(unix), "unix — seconds since 1970-01-01 00:00:00 UTC (exact)"));
    el.appendChild(sep());
    el.appendChild(seg(String(btc), "btc — estimated block height from genesis interval (avg 10 min)"));
    el.appendChild(sep());
    el.appendChild(seg(String(eth), "eth — estimated slot height post-merge (12 s slots)"));
    el.appendChild(sep());
    el.appendChild(seg(String(sol), "sol — estimated slot height (avg 460 ms)"));
  }

  function tick() {
    var el = document.getElementById("epoch-ticker");
    if (el) buildTicker(el);
  }

  window.epochTick = tick;
  tick();
  setInterval(tick, 1000);

  window.addEventListener("storage", function (e) {
    if (e.key === LS_KEY) tick();
  });
})();
