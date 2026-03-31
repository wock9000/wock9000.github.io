# Attribution

## Web fonts (epoch ticker delimiters)

These families are **self-hosted** under `assets/fonts/` (WOFF2 from [google-webfonts-helper](https://gwfh.mranftl.com/fonts), same upstream as Google Fonts):

- **Noto Sans Symbols 2** — `noto-sans-symbols-2-latin-symbols.woff2` (latin + symbols subset).
- **Noto Color Emoji** — `noto-color-emoji.woff2` (emoji subset).

`@font-face` rules live in **`assets/epoch-fonts.css`**, which is linked only from pages that include the epoch footer (so other musings stay free of these downloads). They apply to **`.epoch-sep`** only (delimiter characters between clock values). Body and numeric segments (`.epoch-seg`) remain **Times New Roman** / **Courier** as before.

**License:** Noto fonts are published under the **SIL Open Font License 1.1** (OFL). See the [Noto project](https://github.com/notofonts) and [Google Fonts](https://developers.google.com/fonts) licensing.

---

## Open-source tooling (reference — not bundled in this repo)

These projects help with **subsetting**, **`unicode-range`**, and **pan-Unicode** workflows on the web. None are required to build this static site; they are listed for the “grand scheme” ambition of tighter Unicode coverage.

| Project | Role |
|--------|------|
| [**fonttools**](https://github.com/fonttools/fonttools) (Python) | `pyftsubset`, TTX — subset fonts by Unicode ranges, strip tables, produce smaller WOFF2. |
| [**glyphhanger**](https://github.com/zachleat/glyphhanger) | Crawl pages or pass glyphs, subset fonts to only used code points; pairs with Puppeteer. |
| [**subset-font**](https://github.com/papandreou/subset-font) (Node) | Programmatic subsetting pipeline. |
| [**google-webfonts-helper**](https://github.com/majodev/google-webfonts-helper) | UI to download Google Fonts with **unicode-range** snippets for self-hosting. |
| [**Brotli WOFF2**](https://github.com/google/woff2) | Encode fonts efficiently for the web. |
| [**Noto source / notofonts**](https://github.com/notofonts) | Upstream Noto builds and per-script releases. |

**Standards:** CSS [`unicode-range`](https://developer.mozilla.org/en-US/docs/Web/CSS/@font-face/unicode-range) and multiple `@font-face` blocks are defined by the CSS Fonts specification; browsers match code points to the first font that covers the range.

---

## Removing web fonts

Remove **`assets/epoch-fonts.css`** (and its `<link>` from epoch pages), the Noto entries from `.epoch-sep`’s `font-family` in `style.css`, and the files under `assets/fonts/` if you like; use a purely system stack on `.epoch-sep` if you prefer zero font bytes from the repo.
