// epoch.js — cross-chain clock
// unix is exact; btc / eth / sol are estimates derived from genesis constants.
(function () {
  var BTC_GENESIS  = 1231006505;  // block 0,      Jan 03 2009 18:15:05 UTC
  var ETH_MERGE    = 1663224179;  // slot 0 PoS,   Sep 15 2022 06:42:59 UTC
  var SOL_GENESIS  = 1584368400;  // mainnet-beta, Mar 16 2020 ~11:00 UTC
  var BTC_INTERVAL = 600;         // ~10 min avg
  var ETH_SLOT     = 12;          // 12 s exactly (post-merge)
  var SOL_SLOT     = 0.46;        // ~460 ms avg (skipped slots included)

  function tick() {
    var t    = Date.now() / 1000;
    var unix = Math.floor(t);
    var btc  = Math.floor((t - BTC_GENESIS)  / BTC_INTERVAL);
    var eth  = Math.floor((t - ETH_MERGE)    / ETH_SLOT);
    var sol  = Math.floor((t - SOL_GENESIS)  / SOL_SLOT);
    var el   = document.getElementById('epoch-ticker');
    if (el) el.textContent =
      'unix\u00b7' + unix +
      '\u2002btc\u00b7\u007e' + btc +
      '\u2002eth\u00b7\u007e' + eth +
      '\u2002sol\u00b7\u007e' + sol;
  }

  tick();
  setInterval(tick, 1000);
}());
