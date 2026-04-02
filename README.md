# wock9000.github.io

Static personal site on **GitHub Pages**. No build step.

**Security:** do not commit secrets — see [`SECURITY.md`](SECURITY.md).

- **Root:** `index.html` — musings list.
- **Pieces:** `musings/*.html` (plain HTML; `_template.html` for new entries).

Preview locally over HTTP (not `file://`), e.g. `python3 -m http.server 8000 --bind 127.0.0.1` → `http://127.0.0.1:8000/`.

**Deploy:** push branch **`trunk`**; **Settings → Pages** → source **trunk** / **/** (root).
