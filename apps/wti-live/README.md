# WTI Live (full dashboard)

**Public embeds:** The static site does not link live dashboard URLs; run or deploy this app for private/local use.

Heavier Dash app from `~/wti-dashboard`: **FLOWS** (geo + pipeline model) and **CHART** (1m bars), DuckDB, optional Databento + EIA.

- **Deploy:** `apps/wti-live` as a second Render web service (see root `render.yaml`).
- **Secrets (optional):** set `DATABENTO_API_KEY`, `EIA_API_KEY` in the Render dashboard; without them the app falls back to yfinance where coded.
- **Local:** `pip install -r requirements.txt` then `python app.py` → http://127.0.0.1:8050

The smaller candlestick-only app stays in `apps/wti-intraday/`.
