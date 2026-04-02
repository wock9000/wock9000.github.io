# Security

This repo is a **static site** (HTML/CSS) on GitHub Pages. There is **no server-side execution** of secrets at publish time; anything committed is **public**.

- **Do not commit** API keys, tokens, passwords, private keys, or `.env` files. See `.gitignore` — keep secrets in **host dashboards only** (e.g. Render) or local env, never in the tree.
- **Reporting:** If you find a committed credential or other security issue, open a **private** GitHub advisory or contact the repo owner as appropriate; rotate exposed credentials immediately.

Draft musings under `musings/_draft-*` are not indexed for search (`robots.txt`, `noindex`) but remain fetchable by URL if the repo is public.
