/**
 * Time-of-day background: UTC day split into three 8h intervals; which image
 * appears in each interval is a random permutation of [0,1,2] that changes
 * once per UTC day (not a fixed morning/afternoon/evening cycle).
 *
 * Primitives (bounded, auditable):
 * - Mulberry32: compact 32-bit PRNG (common JS snippet; good enough for shuffles).
 * - Fisher–Yates shuffle: uniform random permutation when draws are uniform.
 *
 * Alternatives you could swap in with the same seed/interval split:
 * - LCG (Numerical Recipes / glibc-style): s = (s * 1664525 + 1013904223) >>> 0
 * - xorshift32: s ^= s << 13; s ^= s >>> 17; s ^= s << 5
 * - SplitMix32 / MurmurHash3 finalizer for mixing a day seed
 */
(function () {
  "use strict";

  var NAMES = [
    "egg-babylon-print.png",
    "egg-hanging-gardens.png",
    "egg-assyrian-relief.png",
  ];

  function mulberry32(seed) {
    return function () {
      var t = (seed += 0x6d2b79f5);
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  /** Uniform permutation of [0, 1, 2] from daily seed. */
  function dailyPermutation(utcDayIndex) {
    var arr = [0, 1, 2];
    var rand = mulberry32(utcDayIndex >>> 0);
    for (var i = 2; i > 0; i--) {
      var j = Math.floor(rand() * (i + 1));
      var tmp = arr[i];
      arr[i] = arr[j];
      arr[j] = tmp;
    }
    return arr;
  }

  /** Interval 0, 1, or 2: three equal slices of the UTC day (8h each). */
  function utcIntervalIndex() {
    var sec = Math.floor(Date.now() / 1000);
    var pos = sec % 86400;
    if (pos < 28800) return 0;
    if (pos < 57600) return 1;
    return 2;
  }

  function utcDayIndex() {
    return Math.floor(Date.now() / 86400000);
  }

  function run() {
    var base = new URL(".", document.currentScript.src);
    var perm = dailyPermutation(utcDayIndex());
    var slot = utcIntervalIndex();
    var imageIndex = perm[slot];
    var url = new URL(NAMES[imageIndex], base).href;
    document.documentElement.style.setProperty(
      "--egg-bg-image",
      'url("' + url + '")'
    );
  }

  run();
})();
