# WTI intraday dashboard (Dash)

**Public embeds:** This repo does **not** advertise live Render/Fly URLs in site pages (reduces third-party surface on the static site). Deploy privately if you use this app; use placeholders in docs, not fixed hostnames.

NYMEX WTI front-month (`CL=F` via yfinance), candlesticks + volume + session VWAP. Optional deploy via **[Render](https://render.com)** (root [`render.yaml`](../../render.yaml)) or **Fly.io** (see below).

The **full** pipeline/geo dashboard lives in **[`../wti-live/`](../wti-live/)** as a second service in `render.yaml`, not in this folder.

## Render vs Fly (which is “better”?)

| | **Render (free web service)** | **Fly.io (small VM)** |
|---|---|---|
| **Cold start** | Yes — free tier **spins down** after ~15 min idle; first request often **~30s** | **No forced sleep** if you keep `auto_stop_machines = "off"` and `min_machines_running = 1` |
| **Speed** | Fine when warm; slow only on wake | **Not inherently “slow”** — shared CPU **bursts** for spikes; steady 100% load gets throttled (irrelevant for a 60s refresh dashboard) |
| **RAM** | More headroom on free tier in practice | `fly.toml` defaults to **512MB** for Dash+Pandas+Plotly headroom (may bill slightly more on pay-as-you-go than 256MB) |
| **Friction** | `render.yaml` + Git connect | Install [`flyctl`](https://fly.io/docs/hands-on/install-flyctl/), `fly launch` / `fly deploy` |

**Summary:** Render free is **easier** and **good enough** if you tolerate cold starts. For **persistent** hosting without that iframe lag, **Fly is the better fit** — it is not a “slow server” for this workload; shared CPU is fine for personal charts.

## Memory budget (rough)

Nothing in this repo runs automated profiling; treat these as **order-of-magnitude** for **one Gunicorn worker** after imports:

| Piece | Typical RSS contribution |
|--------|---------------------------|
| Python + Gunicorn + Flask/Dash | ~80–150 MB |
| NumPy / Pandas (small OHLCV frame) | ~30–80 MB |
| Plotly (figure objects + renderer paths) | ~40–120 MB |
| yfinance / HTTP + parsing spike | +10–40 MB transient |

**Expect ~200–350 MB resident** during a request on a warm process; **peaks** can brush higher on first load after cold start when everything is imported at once. **`fly.toml` uses 512MB** so you stay out of OOM territory on small tiers; trim to **256MB** only if you accept tighter margins.

**Measure yourself:** `docker stats` (Fly Docker), `fly ssh console` + `ps aux` / `cat /proc/$PID/status`, or run locally with `/usr/bin/time -v python app.py` and watch `Maximum resident set size`.

## Other hosts (free / cheap / self‑host)

| Option | Notes |
|--------|--------|
| **Oracle Cloud “Always Free”** | Generous **ARM** VMs (e.g. Ampere A1); often **1 GB+ RAM** per instance—comfortable for this app if you’re OK managing a Linux box. |
| **Fly / Render / Railway / Koyeb** | Managed PaaS; trade money or cold starts for less ops. |
| **Hetzner / OVH / small VPS** | Not free; **€4–6/mo** gets 2 GB RAM and zero platform sleep games. |
| **Self‑hosted PaaS (open source)** | **[Coolify](https://coolify.io/)**, **[CapRover](https://caprover.com/)**, **[Dokku](https://dokku.com/)** on any VPS—Dockerfile in this folder drops in cleanly; you own restarts, TLS, and backups. |
| **GitHub** | **No** always‑on Python web app on free GitHub alone—**Pages is static**; Actions are CI, not a long‑lived server. |

## “On-chain” / Solana

Solana’s **max account data size** is on the order of **10 MiB** per account (`MAX_ACCOUNT_DATA_LEN` in the protocol)—your recollection is in the right ballpark.

That limit is for **account data** (programs, state blobs), **not** for running a **Python Dash server**. You cannot host an interactive Plotly web app **inside** a Solana account: no HTTP server, no yfinance, no iframe in the usual sense. At most you’d store **serialized metrics or hashes** on-chain (expensive, odd fit for OHLCV). For a live dashboard, use a normal host; use chain **only** if you have a separate product reason (attestation, audit trail), not for compute.

## Local

```bash
cd apps/wti-intraday
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Open <http://127.0.0.1:8050>.

## Deploy on Render (free)

1. Push this repo to GitHub.
2. In [Render](https://render.com): **New +** → **Blueprint** → connect the repo (root `render.yaml`).
3. Or **Web Service**: **Root Directory** `apps/wti-intraday`, build `pip install -r requirements.txt`, start `gunicorn --bind 0.0.0.0:$PORT app:server`.
4. Set the musing iframe `src` to your `https://YOUR-SERVICE.onrender.com` URL.

**Cold start:** First hit after idle can take ~30s.

## Deploy on Fly.io (persistent)

1. Install `flyctl`, run `fly auth login`.
2. From repo: `cd apps/wti-intraday`
3. If you have no Fly app yet: `fly launch` (accept or edit `fly.toml`; this repo already includes one).
4. `fly deploy`
5. Your app is at `https://YOUR-APP.fly.dev` (see `fly.toml` `app` name). Use that URL only in private embeds or local preview — not required for the public Pages site.

`fly.toml` sets **`auto_stop_machines = "off"`** so a machine stays up — embeds load without Render-style wake-up delay.

**Billing:** Fly’s model is pay-as-you-go with a card; legacy orgs may still have small included allowances — check [current pricing](https://fly.io/docs/about/pricing/). A tiny always-on VM is usually **on the order of a few dollars/month** if not covered by credits; still far cheaper than “real” observability SaaS.

## CSP / iframe

`app.py` sets `Content-Security-Policy: frame-ancestors` for GitHub Pages (adjust the hostname if your site URL differs). Add your Fly or Render hostname only if you need stricter locking; the **parent** of the iframe is GitHub Pages, not the API host.

## Disclaimer

Market data via yfinance/Yahoo is unofficial; for trading decisions use your broker or data vendor. This is for learning / personal dashboards only.
