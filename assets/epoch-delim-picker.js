// epoch-delim-picker.js — Shift+click: viewport is partitioned into TOTAL = 1024×1086 cells (exact product).
// Each cell index maps to a unique scalar via (index * K) mod TOTAL (gcd(K,TOTAL)=1).
// Coordinates use the visible viewport (clientX/Y, innerWidth/innerHeight). Disabled on narrow screens.
// Shift avoids stealing normal clicks. O(1) per pick.
(function () {
  var TOTAL = 0x110000 - 0x800;
  var GRID_COLS = 1024;
  var GRID_ROWS = 1086;
  if (GRID_COLS * GRID_ROWS !== TOTAL) throw new Error("epoch-delim-picker: grid mismatch");

  function gcd(a, b) {
    a = Math.abs(a);
    b = Math.abs(b);
    while (b) {
      var t = b;
      b = a % b;
      a = t;
    }
    return a;
  }

  function pickMultiplier() {
    var candidates = [2654435769, 1597334677, 1000003, 1103515245, 7919];
    for (var i = 0; i < candidates.length; i++) {
      var k = candidates[i] % TOTAL;
      if (k <= 0) k = 1;
      if (gcd(k, TOTAL) === 1) return k;
    }
    return 1;
  }

  var MULT_K = pickMultiplier();
  var LS_KEY = "epochDelimiter";

  function indexToScalar(i) {
    if (i < 0 || i >= TOTAL) return -1;
    if (i < 0xd800) return i;
    return i + 0x800;
  }

  function pixelToScalarIndex(cellIndex) {
    return ((cellIndex * MULT_K) % TOTAL + TOTAL) % TOTAL;
  }

  function isDesktop() {
    return window.matchMedia("(min-width: 641px)").matches;
  }

  function onPick(ev) {
    if (!isDesktop()) return;
    if (!ev.shiftKey) return;
    var t = ev.target;
    if (t && t.closest) {
      if (t.closest("a, button, input, textarea, select, summary, label, [contenteditable='true']"))
        return;
    }
    var w = window.innerWidth;
    var h = window.innerHeight;
    if (w < 1 || h < 1) return;

    var cw = w / GRID_COLS;
    var ch = h / GRID_ROWS;
    var cx = Math.floor(ev.clientX / cw);
    var cy = Math.floor(ev.clientY / ch);
    if (cx < 0) cx = 0;
    if (cy < 0) cy = 0;
    if (cx >= GRID_COLS) cx = GRID_COLS - 1;
    if (cy >= GRID_ROWS) cy = GRID_ROWS - 1;

    var cellIndex = cy * GRID_COLS + cx;
    var si = pixelToScalarIndex(cellIndex);
    var sc = indexToScalar(si);
    if (sc < 0) return;

    localStorage.setItem(LS_KEY, String.fromCodePoint(sc));
    if (typeof window.epochTick === "function") window.epochTick();
  }

  document.addEventListener("click", onPick, true);
})();
