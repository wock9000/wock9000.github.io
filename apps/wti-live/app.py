"""
WTI Intraday Dashboard
Data: Databento GLBX.MDP3 CL MBP-1 (primary) → yf.download (fallback)
      EIA API for weekly inventory cross-validation
      DuckDB in-process for tick aggregation + persistence
Stack: Dash + Plotly, exchange-grade CME/NYMEX tick data
Run:  python app.py  →  http://localhost:8050
"""

from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import os
import random
import requests
import threading
import time

# ── Constants ─────────────────────────────────────────────────────────────────

TICKER      = "CL=F"
INTERVAL    = "1m"
PERIOD      = "1d"
STALE_TIMEOUT = 30
DATA_DIR    = Path(__file__).parent / ".data"
DB_PATH     = DATA_DIR / "wti.duckdb"

DB_DATASET    = "GLBX.MDP3"
DB_SYMBOL     = "CL.c.0"
DB_STYPE      = "continuous"
DB_SCHEMA     = "mbp-1"
DB_API_KEY    = os.environ.get("DATABENTO_API_KEY", "")

EIA_API_KEY   = os.environ.get("EIA_API_KEY", "")
EIA_INVENTORY_URL  = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"
EIA_PIPE_MOVES_URL = "https://api.eia.gov/v2/petroleum/move/pipe/data/"
EIA_REFUTIL_URL    = "https://api.eia.gov/v2/petroleum/pnp/wiup/data/"
EIA_IMPORTS_URL    = "https://api.eia.gov/v2/petroleum/move/imp/data/"
EIA_EXPORTS_URL    = "https://api.eia.gov/v2/petroleum/move/exp/data/"

# PADD codes → route mapping for calibrating flows with real EIA movement data
PADD_FOR_NODE = {
    "Cushing": "P2", "Patoka": "P2", "Whiting": "P2", "Wood River": "P2",
    "Stanley": "P2", "Guernsey": "P4",
    "Houston": "P3", "Midland": "P3", "Crane": "P3", "Corpus Christi": "P3",
    "Port Arthur": "P3", "Baytown": "P3", "Deer Park": "P3", "Texas City": "P3",
    "Garyville": "P3", "Baton Rouge": "P3", "Lake Charles": "P3",
    "St. James": "P3", "Nederland": "P3", "Ingleside": "P3", "Freeport": "P3",
    "Bryan Mound": "P3", "Big Hill": "P3", "West Hackberry": "P3",
    "Bayou Choctaw": "P3",
    "Linden": "P1", "NYMEX": "P1",
    "El Segundo": "P5", "Martinez": "P5",
    "Hardisty": "CA", "Edmonton": "CA",
}

COLORS = {
    "bg":         "#f7f8fa",
    "panel":      "#ffffff",
    "border":     "#e2e8f0",
    "text":       "#1a202c",
    "muted":      "#718096",
    "up":         "#16a34a",
    "down":       "#dc2626",
    "neutral":    "#64748b",
    "accent":     "#d97706",
    "candle_wick":"#94a3b8",
    "vol_up":     "rgba(22,163,74,0.35)",
    "vol_down":   "rgba(220,38,38,0.35)",
    "grid":       "rgba(0,0,0,0.06)",
    "inflow":     "#3b82f6",
    "outflow":    "#d97706",
    "gas":        "#8b5cf6",
    "gas_in":     "#a78bfa",
    "gas_out":    "#7c3aed",
}

FONT_MONO = "JetBrains Mono, Fira Mono, monospace"
FONT_SANS = "'DM Sans', sans-serif"

NODE_SYMBOLS = {
    "hub":        "circle",
    "production": "circle",
    "refinery":   "diamond",
    "storage":    "square",
    "export":     "triangle-up",
    "exchange":   "star",
    "junction":   "circle",
    "lng":        "triangle-down",
    "gas-hub":    "hexagon2",
    "gas-prod":   "pentagon",
    "spr":        "star-square",
}

# ── Geographic infrastructure ────────────────────────────────────────────────

WTI_NODES = [
    # ── OIL ── Tier 1 — delivery hub
    {"name": "Cushing",        "role": "WTI Delivery Hub",           "lat": 35.985, "lon": -96.767,  "size": 20, "tier": 1, "kind": "hub",        "commodity": "oil"},

    # ── OIL ── Tier 2 — major hubs / production
    {"name": "Hardisty",       "role": "Pipeline Hub · AB",          "lat": 52.67,  "lon": -111.67,  "size": 10, "tier": 2, "kind": "production", "commodity": "oil", "tp": "top right"},
    {"name": "Edmonton",       "role": "Upgrading & Refining · AB",  "lat": 53.55,  "lon": -113.49,  "size": 9,  "tier": 2, "kind": "production", "commodity": "oil", "tp": "top left"},
    {"name": "Midland",        "role": "Permian Basin Oil",          "lat": 31.997, "lon": -102.078, "size": 9,  "tier": 2, "kind": "production", "commodity": "oil", "tp": "bottom center"},
    {"name": "Houston",        "role": "Refining & Trading Hub",     "lat": 29.760, "lon": -95.370,  "size": 10, "tier": 2, "kind": "hub",        "commodity": "both", "tp": "bottom right"},
    {"name": "NYMEX",          "role": "NYMEX / CME Exchange",       "lat": 40.713, "lon": -74.006,  "size": 9,  "tier": 2, "kind": "exchange",   "commodity": "both", "tp": "top center"},

    # ── OIL ── Tier 3 — storage / junctions / export / production
    {"name": "St. James",      "role": "Storage · 70M bbl",          "lat": 30.010, "lon": -90.860,  "size": 7,  "tier": 3, "kind": "storage",    "commodity": "oil", "tp": "top right"},
    {"name": "Corpus Christi", "role": "Oil Export Terminal",         "lat": 27.801, "lon": -97.396,  "size": 7,  "tier": 3, "kind": "export",     "commodity": "oil", "tp": "bottom center"},
    {"name": "Patoka",         "role": "Pipeline Junction · IL",     "lat": 38.757, "lon": -89.000,  "size": 7,  "tier": 3, "kind": "junction",   "commodity": "oil", "tp": "top center"},
    {"name": "Stanley",        "role": "Bakken · DAPL Origin",       "lat": 48.31,  "lon": -102.39,  "size": 7,  "tier": 3, "kind": "production", "commodity": "oil", "tp": "top center"},
    {"name": "Crane",          "role": "Delaware Basin Hub",          "lat": 31.40,  "lon": -102.35,  "size": 7,  "tier": 3, "kind": "production", "commodity": "oil", "tp": "top left"},
    {"name": "Nederland",      "role": "ET Terminal · 33M bbl",     "lat": 29.97,  "lon": -94.00,   "size": 7,  "tier": 3, "kind": "storage",    "commodity": "oil", "tp": "top center"},
    {"name": "Ingleside",      "role": "Moda Oil Export",            "lat": 27.83,  "lon": -97.21,   "size": 6,  "tier": 3, "kind": "export",     "commodity": "oil", "tp": "bottom right"},
    {"name": "Freeport",       "role": "Phillips 66 Oil Export",     "lat": 28.94,  "lon": -95.36,   "size": 6,  "tier": 3, "kind": "export",     "commodity": "oil", "tp": "bottom left"},
    {"name": "Guernsey",       "role": "Pony Express Junction · WY", "lat": 42.27,  "lon": -104.74,  "size": 6,  "tier": 3, "kind": "junction",   "commodity": "oil", "tp": "top right"},

    # ── OIL ── Refineries — Gulf Coast
    {"name": "Port Arthur",    "role": "Motiva · 636 kb/d",         "lat": 29.90,  "lon": -93.93,   "size": 8,  "tier": 3, "kind": "refinery",   "commodity": "oil", "tp": "top right"},
    {"name": "Baytown",        "role": "ExxonMobil · 584 kb/d",     "lat": 29.73,  "lon": -94.98,   "size": 7,  "tier": 3, "kind": "refinery",   "commodity": "oil", "tp": "bottom right"},
    {"name": "Deer Park",      "role": "Shell · 340 kb/d",          "lat": 29.67,  "lon": -95.12,   "size": 6,  "tier": 3, "kind": "refinery",   "commodity": "oil", "tp": "bottom left"},
    {"name": "Texas City",     "role": "Marathon · 593 kb/d",       "lat": 29.38,  "lon": -94.90,   "size": 7,  "tier": 3, "kind": "refinery",   "commodity": "oil", "tp": "bottom center"},
    {"name": "Garyville",      "role": "Marathon · 596 kb/d",       "lat": 30.06,  "lon": -90.62,   "size": 7,  "tier": 3, "kind": "refinery",   "commodity": "oil", "tp": "bottom left"},
    {"name": "Baton Rouge",    "role": "ExxonMobil · 520 kb/d",     "lat": 30.45,  "lon": -91.19,   "size": 7,  "tier": 3, "kind": "refinery",   "commodity": "oil", "tp": "top left"},
    {"name": "Lake Charles",   "role": "Refining Complex · 800+ kb/d", "lat": 30.23, "lon": -93.22,  "size": 7,  "tier": 3, "kind": "refinery",   "commodity": "oil", "tp": "top center"},

    # ── OIL ── Refineries — Midwest
    {"name": "Whiting",        "role": "BP · 435 kb/d",             "lat": 41.68,  "lon": -87.49,   "size": 7,  "tier": 3, "kind": "refinery",   "commodity": "oil", "tp": "top right"},
    {"name": "Wood River",     "role": "Phillips 66 · 346 kb/d",    "lat": 38.87,  "lon": -90.10,   "size": 6,  "tier": 3, "kind": "refinery",   "commodity": "oil", "tp": "bottom right"},

    # ── OIL ── East Coast
    {"name": "Linden",         "role": "Bayway / NY Harbor",         "lat": 40.63,  "lon": -74.25,   "size": 7,  "tier": 3, "kind": "refinery",   "commodity": "oil", "tp": "bottom right"},

    # ── OIL ── West Coast
    {"name": "El Segundo",     "role": "Chevron · 269 kb/d",        "lat": 33.92,  "lon": -118.41,  "size": 7,  "tier": 3, "kind": "refinery",   "commodity": "oil", "tp": "top left"},
    {"name": "Martinez",       "role": "PBF · 157 kb/d",            "lat": 38.02,  "lon": -122.13,  "size": 6,  "tier": 3, "kind": "refinery",   "commodity": "oil", "tp": "top left"},

    # ── OIL ── Strategic Petroleum Reserve
    {"name": "Bryan Mound",    "role": "SPR · 247M bbl",            "lat": 29.05,  "lon": -95.63,   "size": 5,  "tier": 4, "kind": "spr",        "commodity": "oil", "tp": "bottom left"},
    {"name": "Big Hill",       "role": "SPR · 170M bbl",            "lat": 29.69,  "lon": -94.35,   "size": 5,  "tier": 4, "kind": "spr",        "commodity": "oil", "tp": "bottom right"},
    {"name": "West Hackberry", "role": "SPR · 220M bbl",            "lat": 30.20,  "lon": -93.32,   "size": 5,  "tier": 4, "kind": "spr",        "commodity": "oil", "tp": "bottom left"},
    {"name": "Bayou Choctaw",  "role": "SPR · 76M bbl",             "lat": 30.42,  "lon": -91.37,   "size": 5,  "tier": 4, "kind": "spr",        "commodity": "oil", "tp": "bottom right"},

    # ── GAS ── Henry Hub benchmark
    {"name": "Henry Hub",      "role": "NG Benchmark · Erath, LA",  "lat": 29.96,  "lon": -92.18,   "size": 14, "tier": 1, "kind": "gas-hub",    "commodity": "gas"},

    # ── GAS ── Major pricing hubs
    {"name": "Waha Hub",       "role": "Permian Gas · 12+ Bcf/d",   "lat": 31.52,  "lon": -103.16,  "size": 8,  "tier": 2, "kind": "gas-hub",    "commodity": "gas", "tp": "bottom right"},
    {"name": "Katy Hub",       "role": "Gulf Coast Gas Hub",         "lat": 29.79,  "lon": -95.82,   "size": 7,  "tier": 3, "kind": "gas-hub",    "commodity": "gas", "tp": "top left"},
    {"name": "Dominion South", "role": "Appalachian Gas Hub · PA",   "lat": 39.90,  "lon": -80.18,   "size": 7,  "tier": 2, "kind": "gas-hub",    "commodity": "gas", "tp": "top right"},
    {"name": "Opal Hub",       "role": "Rocky Mountain Gas · WY",   "lat": 41.77,  "lon": -110.33,  "size": 6,  "tier": 3, "kind": "gas-hub",    "commodity": "gas", "tp": "top center"},
    {"name": "Chicago Citygate", "role": "Midwest Gas Demand",      "lat": 41.88,  "lon": -87.63,   "size": 7,  "tier": 3, "kind": "gas-hub",    "commodity": "gas", "tp": "top left"},
    {"name": "AECO Hub",       "role": "Alberta Gas Benchmark",      "lat": 51.05,  "lon": -114.07,  "size": 7,  "tier": 3, "kind": "gas-hub",    "commodity": "gas", "tp": "top right"},

    # ── GAS ── Production basins
    {"name": "Marcellus",      "role": "Marcellus Shale · ~35 Bcf/d", "lat": 41.50, "lon": -76.80,  "size": 8,  "tier": 2, "kind": "gas-prod",   "commodity": "gas", "tp": "top right"},
    {"name": "Haynesville",    "role": "Haynesville Shale · ~16 Bcf/d", "lat": 32.50, "lon": -93.75, "size": 7,  "tier": 2, "kind": "gas-prod",   "commodity": "gas", "tp": "top right"},

    # ── GAS ── LNG export terminals
    {"name": "Sabine Pass",    "role": "Cheniere LNG · 30 MTPA",    "lat": 29.76,  "lon": -93.86,   "size": 8,  "tier": 2, "kind": "lng",        "commodity": "gas", "tp": "bottom right"},
    {"name": "Cameron LNG",    "role": "Sempra LNG · 15 MTPA",     "lat": 29.78,  "lon": -93.33,   "size": 7,  "tier": 3, "kind": "lng",        "commodity": "gas", "tp": "bottom center"},
    {"name": "Freeport LNG",   "role": "Freeport LNG · 15 MTPA",   "lat": 28.95,  "lon": -95.31,   "size": 7,  "tier": 3, "kind": "lng",        "commodity": "gas", "tp": "top right"},
    {"name": "CC LNG",         "role": "Cheniere LNG · 25 MTPA",   "lat": 27.85,  "lon": -97.47,   "size": 7,  "tier": 3, "kind": "lng",        "commodity": "gas", "tp": "bottom left"},
    {"name": "Cove Point",     "role": "Dominion LNG · 5.25 MTPA", "lat": 38.40,  "lon": -76.38,   "size": 6,  "tier": 3, "kind": "lng",        "commodity": "gas", "tp": "bottom right"},
    {"name": "Elba Island",    "role": "Kinder Morgan LNG · 2.5 MTPA", "lat": 32.09, "lon": -80.90, "size": 5,  "tier": 3, "kind": "lng",        "commodity": "gas", "tp": "bottom right"},
]

FLOW_ROUTES = [
    # ── OIL ── Canadian inflows
    {"name": "Enbridge Feed",   "from": "Edmonton",       "to": "Hardisty",       "cap": 900, "type": "in",  "commodity": "oil"},
    {"name": "Keystone",        "from": "Hardisty",       "to": "Cushing",        "cap": 590, "type": "in",  "commodity": "oil"},
    {"name": "Enbridge Mainline", "from": "Hardisty",     "to": "Whiting",        "cap": 2850, "type": "in", "commodity": "oil"},
    # ── OIL ── Permian → Cushing
    {"name": "Basin",           "from": "Midland",        "to": "Cushing",        "cap": 700, "type": "in",  "commodity": "oil"},
    # ── OIL ── Bakken / Rockies → Midcontinent
    {"name": "DAPL",            "from": "Stanley",        "to": "Patoka",         "cap": 750, "type": "in",  "commodity": "oil"},
    {"name": "Pony Express",    "from": "Guernsey",       "to": "Cushing",        "cap": 320, "type": "in",  "commodity": "oil"},
    # ── OIL ── Cushing takeaway
    {"name": "Seaway",          "from": "Cushing",        "to": "Houston",        "cap": 850, "type": "out", "commodity": "oil"},
    {"name": "Ozark",           "from": "Cushing",        "to": "Patoka",         "cap": 400, "type": "out", "commodity": "oil"},
    # ── OIL ── Midcontinent distribution
    {"name": "Capline (rev.)",  "from": "Patoka",         "to": "St. James",      "cap": 1200, "type": "out", "commodity": "oil"},
    {"name": "Flanagan South",  "from": "Patoka",         "to": "Cushing",        "cap": 600, "type": "in",  "commodity": "oil"},
    # ── OIL ── Permian takeaway (direct to Gulf)
    {"name": "Permian Hwy Oil", "from": "Midland",        "to": "Houston",        "cap": 600, "type": "out", "commodity": "oil"},
    {"name": "Cactus II",       "from": "Midland",        "to": "Corpus Christi", "cap": 670, "type": "out", "commodity": "oil"},
    {"name": "Gray Oak",        "from": "Crane",          "to": "Corpus Christi", "cap": 980, "type": "out", "commodity": "oil"},
    {"name": "EPIC",            "from": "Crane",          "to": "Ingleside",      "cap": 600, "type": "out", "commodity": "oil"},
    # ── OIL ── Gulf Coast distribution
    {"name": "LOCAP",           "from": "Houston",        "to": "St. James",      "cap": 500, "type": "out", "commodity": "oil"},
    {"name": "Nederland Spur",  "from": "Houston",        "to": "Nederland",      "cap": 500, "type": "out", "commodity": "oil"},
    {"name": "Export Link",     "from": "Corpus Christi", "to": "Ingleside",      "cap": 400, "type": "out", "commodity": "oil"},
    # ── OIL ── Refinery feeds
    {"name": "Ship Channel",    "from": "Houston",        "to": "Baytown",        "cap": 600, "type": "out", "commodity": "oil"},
    {"name": "TX City Feed",    "from": "Houston",        "to": "Texas City",     "cap": 500, "type": "out", "commodity": "oil"},
    {"name": "Garyville Feed",  "from": "St. James",      "to": "Garyville",      "cap": 600, "type": "out", "commodity": "oil"},
    {"name": "Baton Rouge Feed", "from": "St. James",     "to": "Baton Rouge",    "cap": 520, "type": "out", "commodity": "oil"},
    # ── OIL ── Refined products / long-haul
    {"name": "Colonial",        "from": "Houston",        "to": "Linden",         "cap": 2500, "type": "out", "commodity": "oil"},

    # ── GAS ── Production → hubs
    {"name": "Marcellus Gathering", "from": "Marcellus",    "to": "Dominion South", "cap": 25000, "type": "in",  "commodity": "gas"},
    {"name": "Haynesville Lateral", "from": "Haynesville",  "to": "Henry Hub",      "cap": 8000,  "type": "in",  "commodity": "gas"},
    {"name": "AECO Lateral",        "from": "AECO Hub",     "to": "Opal Hub",       "cap": 4000,  "type": "in",  "commodity": "gas"},
    # ── GAS ── Permian associated gas takeaway
    {"name": "Permian Hwy Gas", "from": "Waha Hub",       "to": "Katy Hub",       "cap": 2100,  "type": "out", "commodity": "gas"},
    {"name": "Gulf Coast Express", "from": "Waha Hub",    "to": "Katy Hub",       "cap": 2000,  "type": "out", "commodity": "gas"},
    # ── GAS ── Major interstate pipelines
    {"name": "Transco",         "from": "Katy Hub",        "to": "Linden",         "cap": 17000, "type": "out", "commodity": "gas"},
    {"name": "Rockies Express", "from": "Opal Hub",        "to": "Dominion South", "cap": 1800,  "type": "out", "commodity": "gas"},
    {"name": "Midcontinent Gas", "from": "Henry Hub",      "to": "Chicago Citygate", "cap": 5000, "type": "out", "commodity": "gas"},
    # ── GAS ── LNG feed gas
    {"name": "Sabine Feed",     "from": "Henry Hub",       "to": "Sabine Pass",    "cap": 4500,  "type": "out", "commodity": "gas"},
    {"name": "Cameron Feed",    "from": "Henry Hub",       "to": "Cameron LNG",    "cap": 2100,  "type": "out", "commodity": "gas"},
    {"name": "Freeport Feed",   "from": "Katy Hub",        "to": "Freeport LNG",   "cap": 2100,  "type": "out", "commodity": "gas"},
    {"name": "CC LNG Feed",     "from": "Corpus Christi",  "to": "CC LNG",         "cap": 3400,  "type": "out", "commodity": "gas"},
    {"name": "Cove Point Feed", "from": "Dominion South",  "to": "Cove Point",     "cap": 750,   "type": "out", "commodity": "gas"},
]

# ── MarketDB (DuckDB in-process analytics) ───────────────────────────────────

class MarketDB:
    """In-process DuckDB for tick storage, aggregation, and persistence."""

    def __init__(self, db_path: Path):
        db_path.parent.mkdir(exist_ok=True)
        self._conn = duckdb.connect(str(db_path))
        self._lock = threading.Lock()
        self.last_tick_time = 0.0
        self.source = "none"
        self._init_schema()

    def _init_schema(self):
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_ticks (
                    ts      TIMESTAMP DEFAULT current_timestamp,
                    price   DOUBLE,
                    size    INTEGER DEFAULT 0,
                    bid     DOUBLE DEFAULT 0,
                    ask     DOUBLE DEFAULT 0,
                    action  VARCHAR(1) DEFAULT 'Q',
                    source  VARCHAR(16) DEFAULT 'databento'
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_bars (
                    date    DATE PRIMARY KEY,
                    open    DOUBLE,
                    high    DOUBLE,
                    low     DOUBLE,
                    close   DOUBLE,
                    volume  BIGINT,
                    source  VARCHAR(16) DEFAULT 'yfinance'
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS eia_inventory (
                    period     DATE PRIMARY KEY,
                    value_kbbl DOUBLE,
                    delta_kbbl DOUBLE
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS eia_padd_moves (
                    period    DATE,
                    from_padd VARCHAR(8),
                    to_padd   VARCHAR(8),
                    value_kb  DOUBLE,
                    PRIMARY KEY (period, from_padd, to_padd)
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS eia_refinery_util (
                    period          DATE,
                    padd            VARCHAR(8),
                    utilization_pct DOUBLE,
                    PRIMARY KEY (period, padd)
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS eia_crude_trade (
                    period    DATE,
                    flow      VARCHAR(8),
                    region    VARCHAR(32),
                    value_kb  DOUBLE,
                    PRIMARY KEY (period, flow, region)
                )
            """)

    def insert_tick(self, price: float, size: int = 0,
                    bid: float = 0, ask: float = 0,
                    action: str = "Q", source: str = "databento"):
        with self._lock:
            self._conn.execute(
                "INSERT INTO raw_ticks (price, size, bid, ask, action, source) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [price, size, bid, ask, action, source],
            )
        self.last_tick_time = time.time()
        self.source = source

    def upsert_eia(self, period: str, value_kbbl: float, delta_kbbl: float):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO eia_inventory (period, value_kbbl, delta_kbbl) "
                "VALUES (?, ?, ?)",
                [period, value_kbbl, delta_kbbl],
            )

    def bars_1m(self) -> pd.DataFrame:
        """1-minute OHLCV bars from trade ticks."""
        with self._lock:
            return self._conn.execute("""
                SELECT time_bucket(INTERVAL '1 minute', ts) AS bucket,
                       first(price) AS "Open",
                       max(price)   AS "High",
                       min(price)   AS "Low",
                       last(price)  AS "Close",
                       sum(size)    AS "Volume"
                FROM raw_ticks
                WHERE action = 'T' AND price > 0
                GROUP BY bucket
                ORDER BY bucket
            """).fetchdf().set_index("bucket")

    def session(self) -> dict:
        """Session-level stats for the stat bar and flow model."""
        with self._lock:
            row = self._conn.execute("""
                SELECT first(price ORDER BY ts)  AS first,
                       last(price ORDER BY ts)   AS last,
                       max(price)                AS hi,
                       min(price)                AS lo,
                       sum(size)                 AS vol,
                       count(*) FILTER (WHERE action = 'T') AS ticks
                FROM raw_ticks
                WHERE price > 0
                  AND ts >= current_date
            """).fetchone()
        if not row or row[0] is None:
            return {}
        first, last, hi, lo, vol, ticks = row
        vol = vol or 0
        ticks = ticks or 0
        chg = last - first
        pct = (chg / first * 100) if first else 0
        # VWAP from trades only
        with self._lock:
            vwap_row = self._conn.execute("""
                SELECT sum(price * size) / nullif(sum(size), 0)
                FROM raw_ticks
                WHERE action = 'T' AND price > 0 AND ts >= current_date
            """).fetchone()
        vwap = vwap_row[0] if vwap_row and vwap_row[0] else last
        # Latest BBO
        with self._lock:
            bbo = self._conn.execute("""
                SELECT bid, ask FROM raw_ticks
                WHERE bid > 0 AND ask > 0
                ORDER BY ts DESC LIMIT 1
            """).fetchone()
        bid = bbo[0] if bbo else 0
        ask = bbo[1] if bbo else 0
        spread = ask - bid if (bid and ask) else 0
        return dict(last=last, first=first, hi=hi, lo=lo,
                    chg=chg, pct=pct, vwap=vwap, vol=int(vol),
                    bid=bid, ask=ask, spread=spread, ticks=int(ticks))

    def eia_latest(self) -> dict:
        with self._lock:
            row = self._conn.execute("""
                SELECT period, value_kbbl, delta_kbbl
                FROM eia_inventory ORDER BY period DESC LIMIT 1
            """).fetchone()
        if not row:
            return {}
        return {"period": str(row[0]), "value_kbbl": row[1], "delta_kbbl": row[2]}

    def tick_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT count(*) FROM raw_ticks WHERE ts >= current_date"
            ).fetchone()
        return row[0] if row else 0

    def bootstrap_yf(self):
        """Seed with yfinance 1-min bars if no ticks exist yet today."""
        if self.tick_count() > 0:
            return
        try:
            df = yf.download(TICKER, period=PERIOD, interval=INTERVAL,
                             progress=False, auto_adjust=True)
            if df.empty:
                return
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
            with self._lock:
                for ts, row in df.iterrows():
                    self._conn.execute(
                        "INSERT INTO raw_ticks (ts, price, size, action, source) "
                        "VALUES (?, ?, ?, 'T', 'yfinance')",
                        [ts, float(row["Close"]), int(row["Volume"])],
                    )
            self.last_tick_time = time.time()
            self.source = "yf-poll"
            print(f"  [yf] seeded {len(df)} intraday bars", flush=True)
        except Exception as exc:
            print(f"  [yf] intraday bootstrap error: {exc}", flush=True)

    def backfill_daily(self):
        """Pull up to 1 year of daily OHLCV from yfinance into daily_bars."""
        with self._lock:
            existing = self._conn.execute(
                "SELECT count(*) FROM daily_bars"
            ).fetchone()[0]
        if existing > 200:
            print(f"  [yf] daily_bars already has {existing} rows — skipping backfill", flush=True)
            return
        try:
            df = yf.download(TICKER, period="1y", interval="1d",
                             progress=False, auto_adjust=True)
            if df.empty:
                return
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
            with self._lock:
                for ts, row in df.iterrows():
                    dt = pd.Timestamp(ts).date()
                    self._conn.execute(
                        "INSERT OR REPLACE INTO daily_bars "
                        "(date, open, high, low, close, volume, source) "
                        "VALUES (?, ?, ?, ?, ?, ?, 'yfinance')",
                        [dt, float(row["Open"]), float(row["High"]),
                         float(row["Low"]), float(row["Close"]),
                         int(row["Volume"])],
                    )
            print(f"  [yf] backfilled {len(df)} daily bars (1y)", flush=True)
        except Exception as exc:
            print(f"  [yf] daily backfill error: {exc}", flush=True)

    def daily_ohlcv(self, days: int = 252) -> pd.DataFrame:
        """Historical daily OHLCV from daily_bars table."""
        with self._lock:
            return self._conn.execute("""
                SELECT date AS "Date",
                       open AS "Open", high AS "High",
                       low AS "Low", close AS "Close",
                       volume AS "Volume"
                FROM daily_bars
                ORDER BY date DESC
                LIMIT ?
            """, [days]).fetchdf().set_index("Date").sort_index()

    def settlements(self, days: int = 60) -> pd.DataFrame:
        """Settlement differentials: close-to-close change and overnight gaps."""
        with self._lock:
            return self._conn.execute("""
                SELECT date,
                       close,
                       close - lag(close) OVER (ORDER BY date) AS close_chg,
                       open  - lag(close) OVER (ORDER BY date) AS gap,
                       (close - lag(close) OVER (ORDER BY date))
                           / lag(close) OVER (ORDER BY date) * 100 AS close_chg_pct
                FROM daily_bars
                ORDER BY date DESC
                LIMIT ?
            """, [days]).fetchdf().sort_values("date")

    def day_summary(self, date_str: str) -> dict:
        """OHLCV summary for a single historical date, with change vs prior close."""
        with self._lock:
            rows = self._conn.execute("""
                SELECT date, open, high, low, close, volume
                FROM daily_bars
                WHERE date <= ?
                ORDER BY date DESC
                LIMIT 2
            """, [date_str]).fetchall()
        if not rows:
            return {}
        _, o, h, l, c, vol = rows[0]
        prev_close = rows[1][4] if len(rows) > 1 else o
        chg = c - prev_close
        pct = (chg / prev_close * 100) if prev_close else 0
        return dict(last=c, first=o, hi=h, lo=l, chg=chg, pct=pct,
                    vwap=c, vol=int(vol or 0), bid=0, ask=0, spread=0, ticks=0)

    def correlation_data(self) -> pd.DataFrame:
        """Join daily closes with weekly EIA inventory for correlation analysis."""
        with self._lock:
            return self._conn.execute("""
                WITH weekly_close AS (
                    SELECT date,
                           close,
                           close - lag(close, 5) OVER (ORDER BY date) AS wk_chg
                    FROM daily_bars
                )
                SELECT w.date, w.close, w.wk_chg,
                       e.value_kbbl, e.delta_kbbl
                FROM weekly_close w
                LEFT JOIN eia_inventory e
                    ON e.period BETWEEN w.date - INTERVAL 3 DAY AND w.date + INTERVAL 3 DAY
                WHERE w.wk_chg IS NOT NULL
                ORDER BY w.date
            """).fetchdf()


    # ── Supply chain data methods ──

    def upsert_padd_move(self, period: str, from_padd: str, to_padd: str, value_kb: float):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO eia_padd_moves (period, from_padd, to_padd, value_kb) "
                "VALUES (?, ?, ?, ?)",
                [period, from_padd, to_padd, value_kb],
            )

    def upsert_refinery_util(self, period: str, padd: str, utilization_pct: float):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO eia_refinery_util (period, padd, utilization_pct) "
                "VALUES (?, ?, ?)",
                [period, padd, utilization_pct],
            )

    def upsert_crude_trade(self, period: str, flow: str, region: str, value_kb: float):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO eia_crude_trade (period, flow, region, value_kb) "
                "VALUES (?, ?, ?, ?)",
                [period, flow, region, value_kb],
            )

    def latest_padd_moves(self) -> dict:
        """Latest inter-PADD pipeline movements → {('P2','P3'): kb, ...}."""
        with self._lock:
            rows = self._conn.execute("""
                SELECT from_padd, to_padd, value_kb
                FROM eia_padd_moves
                WHERE period = (SELECT max(period) FROM eia_padd_moves)
            """).fetchall()
        return {(r[0], r[1]): r[2] for r in rows}

    def latest_refinery_util(self) -> dict:
        """Latest refinery utilization → {'P1': 92.3, 'P2': 88.1, ...}."""
        with self._lock:
            rows = self._conn.execute("""
                SELECT padd, utilization_pct
                FROM eia_refinery_util
                WHERE period = (SELECT max(period) FROM eia_refinery_util)
            """).fetchall()
        return {r[0]: r[1] for r in rows}

    def latest_crude_trade(self) -> dict:
        """Latest crude trade → {'import_CA': kb, 'export_total': kb, ...}."""
        with self._lock:
            rows = self._conn.execute("""
                SELECT flow, region, value_kb
                FROM eia_crude_trade
                WHERE period = (SELECT max(period) FROM eia_crude_trade)
            """).fetchall()
        return {f"{r[0]}_{r[1]}": r[2] for r in rows}

    def supply_chain_summary(self) -> dict:
        """Aggregate supply chain snapshot for the flow model."""
        moves = self.latest_padd_moves()
        util = self.latest_refinery_util()
        trade = self.latest_crude_trade()
        return {"padd_moves": moves, "refinery_util": util, "crude_trade": trade}


mdb: MarketDB | None = None


def _get_mdb() -> MarketDB:
    global mdb
    if mdb is None:
        mdb = MarketDB(DB_PATH)
    return mdb

# ── Feed daemons ──────────────────────────────────────────────────────────────

def _run_databento(market: MarketDB):
    """Primary feed: Databento GLBX.MDP3 CL.c.0 MBP-1."""
    try:
        import databento as db
    except ImportError:
        print("  [databento] library not installed — skipping", flush=True)
        return

    if not DB_API_KEY:
        print("  [databento] no DATABENTO_API_KEY — skipping live feed", flush=True)
        return

    PRICE_SCALE = 1e-9

    while True:
        try:
            print(f"  [databento] connecting {DB_DATASET}/{DB_SYMBOL}/{DB_SCHEMA} …", flush=True)
            live = db.Live(key=DB_API_KEY)
            live.subscribe(
                dataset=DB_DATASET,
                schema=DB_SCHEMA,
                symbols=[DB_SYMBOL],
                stype_in=DB_STYPE,
            )
            for rec in live:
                if isinstance(rec, db.SymbolMappingMsg):
                    print(f"  [databento] {rec.stype_in_symbol} → id {rec.instrument_id}", flush=True)
                elif isinstance(rec, db.MBP1Msg):
                    price = rec.price * PRICE_SCALE
                    bid, ask = 0.0, 0.0
                    if rec.levels and len(rec.levels) > 0:
                        lvl = rec.levels[0]
                        bid = lvl.bid_px * PRICE_SCALE
                        ask = lvl.ask_px * PRICE_SCALE
                        if ask - bid > 5.0 or bid <= 0 or ask <= 0:
                            bid, ask = 0.0, 0.0
                    if rec.action == "T" and rec.size > 0:
                        market.insert_tick(price, rec.size, bid, ask, "T")
                    elif bid > 0 and ask > 0:
                        mid = (bid + ask) / 2
                        market.insert_tick(mid, 0, bid, ask, "Q")
                elif isinstance(rec, db.ErrorMsg):
                    print(f"  [databento] error: {rec.err}", flush=True)
        except Exception as exc:
            print(f"  [databento] disconnected: {exc} — reconnecting in 5s", flush=True)
        time.sleep(5)


def _run_eia_poll(market: MarketDB):
    """EIA weekly Cushing inventory — full backfill on first run, then refresh."""
    if not EIA_API_KEY:
        print("  [eia] no EIA_API_KEY — register free at https://www.eia.gov/opendata/register.php")
        return
    first_run = True
    while True:
        try:
            fetch_len = 104 if first_run else 4
            params: dict = {
                "api_key": EIA_API_KEY,
                "frequency": "weekly",
                "data[0]": "value",
                "facets[product][]": "EPC0",
                "facets[process][]": "SAX",
                "facets[duoarea][]": "R20",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": fetch_len,
            }
            resp = requests.get(EIA_INVENTORY_URL, params=params, timeout=30)
            if resp.ok:
                data = resp.json().get("response", {}).get("data", [])
                inserted = 0
                for i, row in enumerate(data):
                    val = float(row.get("value", 0))
                    prev_val = float(data[i + 1]["value"]) if i + 1 < len(data) else 0
                    delta = val - prev_val if prev_val else 0
                    market.upsert_eia(row["period"], val, delta)
                    inserted += 1
                if data:
                    latest = data[0]
                    val = float(latest["value"])
                    prev = float(data[1]["value"]) if len(data) > 1 else 0
                    scope = f"{inserted} weeks" if first_run else "latest"
                    print(f"  [eia] Cushing: {val:,.0f} kbbl "
                          f"(Δ {val - prev:+,.0f}) as of {latest['period']} [{scope}]",
                          flush=True)
                first_run = False
            else:
                print(f"  [eia] HTTP {resp.status_code}", flush=True)
        except Exception as exc:
            print(f"  [eia] error: {exc}", flush=True)
        time.sleep(1800)


def _run_eia_supply_poll(market: MarketDB):
    """Fetch EIA supply chain data: PADD pipeline movements, refinery utilization, trade."""
    if not EIA_API_KEY:
        return
    base_headers = {"api_key": EIA_API_KEY}

    def _fetch(url: str, params: dict) -> list:
        params["api_key"] = EIA_API_KEY
        resp = requests.get(url, params=params, timeout=30)
        if resp.ok:
            return resp.json().get("response", {}).get("data", [])
        print(f"  [eia-supply] HTTP {resp.status_code} from {url}", flush=True)
        return []

    def _norm_date(p: str) -> str:
        """Normalize EIA period to YYYY-MM-DD (monthly comes as YYYY-MM)."""
        if len(p) == 7:
            return p + "-01"
        return p

    first_run = True
    while True:
        n = 36 if first_run else 6

        # 1) Inter-PADD crude pipeline movements (monthly, thousand barrels)
        #    duoarea format: "R{receiver}-R{source}" e.g. R30-R20 = PADD 3 receives from PADD 2
        padd_map_eia = {"R10": "P1", "R20": "P2", "R30": "P3",
                        "R40": "P4", "R50": "P5"}
        try:
            data = _fetch(EIA_PIPE_MOVES_URL, {
                "frequency": "monthly",
                "data[0]": "value",
                "facets[product][]": "EPC0",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 5000 if first_run else 200,
            })
            move_ct = 0
            for row in data:
                area = row.get("duoarea", "")
                val = row.get("value")
                period = row.get("period", "")
                if val is None or not area or "-" not in area:
                    continue
                parts = area.split("-")
                if len(parts) != 2:
                    continue
                to_padd = padd_map_eia.get(parts[0])
                from_padd = padd_map_eia.get(parts[1])
                if not to_padd or not from_padd:
                    continue
                market.upsert_padd_move(_norm_date(period), from_padd, to_padd, float(val))
                move_ct += 1
            if move_ct:
                print(f"  [eia-supply] pipeline movements: {move_ct} records", flush=True)
        except Exception as exc:
            print(f"  [eia-supply] pipe moves error: {exc}", flush=True)

        # 2) Refinery utilization by PADD (weekly, percent)
        try:
            data = _fetch(EIA_REFUTIL_URL, {
                "frequency": "weekly",
                "data[0]": "value",
                "facets[process][]": "YUP",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": n * 5,
            })
            util_ct = 0
            padd_xlat = {"R10": "P1", "R20": "P2", "R30": "P3",
                         "R40": "P4", "R50": "P5", "NUS": "US"}
            for row in data:
                area = row.get("duoarea", "")
                val = row.get("value")
                period = row.get("period", "")
                padd = padd_xlat.get(area)
                if val is None or not padd:
                    continue
                market.upsert_refinery_util(_norm_date(period), padd, float(val))
                util_ct += 1
            if util_ct:
                latest = data[0] if data else {}
                p3_util = next((r["value"] for r in data
                                if r.get("duoarea") == "R30" and r.get("value")), None)
                label = f"PADD 3 {p3_util}%" if p3_util else f"{util_ct} records"
                print(f"  [eia-supply] refinery util: {label} "
                      f"as of {latest.get('period', '?')}", flush=True)
        except Exception as exc:
            print(f"  [eia-supply] refutil error: {exc}", flush=True)

        # 3) Crude imports (monthly, thousand barrels)
        try:
            data = _fetch(EIA_IMPORTS_URL, {
                "frequency": "monthly",
                "data[0]": "value",
                "facets[product][]": "EPC0",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": n * 10,
            })
            imp_ct = 0
            for row in data:
                val = row.get("value")
                period = row.get("period", "")
                origin = row.get("originName", row.get("originId", "total"))
                if val is None:
                    continue
                market.upsert_crude_trade(_norm_date(period), "import", str(origin)[:32], float(val))
                imp_ct += 1
            if imp_ct:
                print(f"  [eia-supply] crude imports: {imp_ct} records", flush=True)
        except Exception as exc:
            print(f"  [eia-supply] imports error: {exc}", flush=True)

        # 4) Crude exports (monthly, thousand barrels)
        try:
            data = _fetch(EIA_EXPORTS_URL, {
                "frequency": "monthly",
                "data[0]": "value",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": n * 5,
            })
            exp_ct = 0
            for row in data:
                val = row.get("value")
                period = row.get("period", "")
                dest = row.get("destinationName", row.get("destinationId", "total"))
                if val is None:
                    continue
                market.upsert_crude_trade(_norm_date(period), "export", str(dest)[:32], float(val))
                exp_ct += 1
            if exp_ct:
                print(f"  [eia-supply] crude exports: {exp_ct} records", flush=True)
        except Exception as exc:
            print(f"  [eia-supply] exports error: {exc}", flush=True)

        first_run = False
        time.sleep(14400)  # 4 hours — data is weekly/monthly


# ── Flow model ────────────────────────────────────────────────────────────────

def _build_padd_utilization(supply: dict | None) -> dict:
    """Pre-compute per-route utilization from EIA inter-PADD pipeline movements.

    Groups routes by (from_PADD, to_PADD) and distributes EIA-reported monthly
    kb volumes proportionally across routes by capacity.
    Returns {route_name: estimated_utilization}.
    """
    if not supply:
        return {}
    moves = supply.get("padd_moves", {})
    if not moves:
        return {}

    # Group oil routes by PADD pair
    padd_groups: dict[tuple, list] = {}
    for r in FLOW_ROUTES:
        if r.get("commodity") != "oil":
            continue
        sp = PADD_FOR_NODE.get(r["from"])
        dp = PADD_FOR_NODE.get(r["to"])
        if not sp or not dp or sp == dp:
            continue
        padd_groups.setdefault((sp, dp), []).append(r)

    route_util = {}
    for (sp, dp), routes in padd_groups.items():
        monthly_kb = moves.get((sp, dp), 0)
        if monthly_kb <= 0:
            continue
        daily_kbd = monthly_kb / 30.0
        total_cap = sum(r["cap"] for r in routes)
        if total_cap <= 0:
            continue
        group_util = daily_kbd / total_cap
        for r in routes:
            route_util[r["name"]] = max(0.40, min(0.98, group_util))
    return route_util


def simulate_flows(s: dict, supply: dict | None = None) -> list:
    """Derive pipeline throughput from settlement economics + EIA supply chain data.

    Oil routes: base from EIA PADD pipeline movements (actual volumes) or
    refinery utilization, shifted by CL=F price.
    Gas routes: base utilization with minor jitter.
    """
    chg = s.get("chg", 0.0)
    implied_net = -chg * 300
    cushing_cap = sum(
        r["cap"] for r in FLOW_ROUTES
        if r.get("commodity") == "oil"
        and (r["to"] == "Cushing" or r["from"] == "Cushing")
    )
    oil_shift = implied_net / cushing_cap if cushing_cap else 0

    ref_util = (supply or {}).get("refinery_util", {})
    padd_util = _build_padd_utilization(supply)
    DEFAULT_BASE = 0.87

    flows = []
    for r in FLOW_ROUTES:
        if r.get("commodity") == "gas":
            util = max(0.75, min(0.95, DEFAULT_BASE + random.uniform(-0.01, 0.01)))
        else:
            # Priority: EIA actual PADD movements > refinery utilization > default
            if r["name"] in padd_util:
                base = padd_util[r["name"]]
            else:
                dest_padd = PADD_FOR_NODE.get(r["to"])
                src_padd = PADD_FOR_NODE.get(r["from"])
                padd_util_pct = ref_util.get(dest_padd) or ref_util.get(src_padd)
                base = (padd_util_pct / 100.0) if padd_util_pct else DEFAULT_BASE
                base = max(0.60, min(0.98, base))

            adj = oil_shift if r["type"] == "in" else -oil_shift
            util = max(0.40, min(0.98, base + adj + random.uniform(-0.005, 0.005)))
        flows.append({**r, "rate": int(r["cap"] * util)})
    return flows


def flow_summary(flows):
    cin  = sum(f["rate"] for f in flows if f["to"] == "Cushing")
    cout = sum(f["rate"] for f in flows if f["from"] == "Cushing")
    net  = cin - cout
    return {"inflow": cin, "outflow": cout, "net": net,
            "signal": "BUILD" if net >= 0 else "DRAW",
            "delta_bbl": net * 1000}


def flow_dot_positions(lat1, lon1, lat2, lon2, n, n_dots=4):
    phase = (n % 120) / 120
    lats, lons = [], []
    for i in range(n_dots):
        t = ((i / n_dots) + phase) % 1.0
        lats.append(lat1 + t * (lat2 - lat1))
        lons.append(lon1 + t * (lon2 - lon1))
    return lats, lons


# ── Chart ─────────────────────────────────────────────────────────────────────

def build_figure(df: pd.DataFrame) -> go.Figure:
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.78, 0.22], vertical_spacing=0.02,
    )

    if df.empty:
        fig.update_layout(
            paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
            font=dict(color=COLORS["muted"]),
            annotations=[dict(text="No data — market may be closed",
                              xref="paper", yref="paper", x=0.5, y=0.5,
                              showarrow=False,
                              font=dict(size=14, color=COLORS["muted"]))],
        )
        return fig

    candle_colors = [COLORS["up"] if c >= o else COLORS["down"]
                     for c, o in zip(df["Close"], df["Open"])]

    fig.add_trace(go.Candlestick(
        x=df.index, open=df["Open"], high=df["High"],
        low=df["Low"], close=df["Close"],
        increasing_line_color=COLORS["up"],
        decreasing_line_color=COLORS["down"],
        increasing_fillcolor=COLORS["up"],
        decreasing_fillcolor=COLORS["down"],
        line=dict(width=1), whiskerwidth=0.3,
        showlegend=False, name="WTI",
    ), row=1, col=1)

    if "Volume" in df.columns and df["Volume"].sum() > 0:
        df = df.copy()
        df["vwap"] = (df["Close"] * df["Volume"]).cumsum() / df["Volume"].cumsum()
        fig.add_trace(go.Scatter(
            x=df.index, y=df["vwap"],
            line=dict(color=COLORS["accent"], width=1.2, dash="dot"),
            name="VWAP", showlegend=False,
        ), row=1, col=1)
        fig.add_trace(go.Bar(
            x=df.index, y=df["Volume"],
            marker_color=candle_colors, marker_opacity=0.55,
            showlegend=False, name="Volume",
        ), row=2, col=1)

    ax = dict(showgrid=True, gridcolor=COLORS["grid"], gridwidth=1,
              zeroline=False,
              tickfont=dict(family=FONT_MONO, size=11, color=COLORS["muted"]),
              linecolor=COLORS["border"])

    fig.update_layout(
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
        margin=dict(l=12, r=12, t=8, b=8),
        xaxis=dict(**ax, rangeslider_visible=False, showspikes=True,
                   spikecolor=COLORS["muted"], spikethickness=1, spikedash="dot"),
        xaxis2=dict(**ax, showticklabels=True),
        yaxis=dict(**ax, side="right", tickprefix="$", tickformat=".2f"),
        yaxis2=dict(**ax, side="right", tickformat=".2s"),
        hovermode="x unified",
        hoverlabel=dict(bgcolor=COLORS["panel"], bordercolor=COLORS["border"],
                        font=dict(family=FONT_MONO, size=12, color=COLORS["text"])),
        dragmode="zoom", newshape_line_color=COLORS["accent"],
    )
    return fig


# ── Geo map ──────────────────────────────────────────────────────────────────

def build_geo_figure(s: dict, flows: list, n: int) -> go.Figure:
    color = COLORS["up"] if s.get("chg", 0) >= 0 else COLORS["down"]
    price_str = f"${s['last']:.2f}" if s else "—"

    fig = go.Figure()
    lookup = {nd["name"]: nd for nd in WTI_NODES}

    # Pipeline lines — width proportional to utilization, colored by commodity
    routes = flows or FLOW_ROUTES
    for r in routes:
        na, nb = lookup[r["from"]], lookup[r["to"]]
        rate = r.get("rate", r["cap"] * 0.87)
        util = rate / r["cap"] if r["cap"] else 0.87
        is_gas = r.get("commodity") == "gas"
        w = 0.8 + util * 2.0 if is_gas else 0.8 + util * 2.5
        if is_gas:
            line_color = COLORS["gas_in"] if r["type"] == "in" else COLORS["gas_out"]
        else:
            line_color = COLORS["inflow"] if r["type"] == "in" else COLORS["outflow"]
        fig.add_trace(go.Scattergeo(
            lon=[na["lon"], nb["lon"]], lat=[na["lat"], nb["lat"]],
            mode="lines",
            line=dict(width=w, color=line_color),
            opacity=0.20 if is_gas else 0.25,
            showlegend=False, hoverinfo="skip",
        ))

    # Animated flow dots — separate batches for oil and gas
    dot_groups = [
        ("oil", "in",  COLORS["inflow"]),
        ("oil", "out", COLORS["outflow"]),
        ("gas", "in",  COLORS["gas_in"]),
        ("gas", "out", COLORS["gas_out"]),
    ]
    for commodity, ftype, fcolor in dot_groups:
        all_lats, all_lons = [], []
        for r in (flows or []):
            r_comm = r.get("commodity", "oil")
            if r_comm != commodity or r["type"] != ftype:
                continue
            na, nb = lookup[r["from"]], lookup[r["to"]]
            lats, lons = flow_dot_positions(
                na["lat"], na["lon"], nb["lat"], nb["lon"], n,
            )
            all_lats.extend(lats)
            all_lons.extend(lons)
        if all_lats:
            fig.add_trace(go.Scattergeo(
                lon=all_lons, lat=all_lats,
                mode="markers",
                marker=dict(size=4 if commodity == "gas" else 4.5,
                            color=fcolor, opacity=0.80),
                showlegend=False, hoverinfo="skip",
            ))

    # Infrastructure nodes by kind
    for kind, symbol in NODE_SYMBOLS.items():
        nodes = [nd for nd in WTI_NODES if nd.get("kind") == kind and nd["tier"] > 1]
        if not nodes:
            continue
        is_gas_kind = kind in ("gas-hub", "gas-prod", "lng")
        border_color = COLORS["gas"] if is_gas_kind else COLORS["muted"]
        fig.add_trace(go.Scattergeo(
            lon=[nd["lon"] for nd in nodes],
            lat=[nd["lat"] for nd in nodes],
            mode="markers+text",
            marker=dict(
                size=[nd["size"] for nd in nodes],
                symbol=symbol,
                color=COLORS["panel"],
                line=dict(width=1.2 if is_gas_kind else 1, color=border_color),
            ),
            text=[nd["name"] for nd in nodes],
            textposition=[nd.get("tp", "top center") for nd in nodes],
            textfont=dict(family=FONT_MONO, size=7,
                          color=COLORS["gas"] if is_gas_kind else COLORS["muted"]),
            customdata=[nd["role"] for nd in nodes],
            hovertemplate="%{text}<br>%{customdata}<extra></extra>",
            showlegend=False,
        ))

    # Cushing hub — glow + price
    ch = lookup["Cushing"]
    for sz, op in [(48, 0.08), (30, 0.18)]:
        fig.add_trace(go.Scattergeo(
            lon=[ch["lon"]], lat=[ch["lat"]], mode="markers",
            marker=dict(size=sz, color=color, opacity=op),
            showlegend=False, hoverinfo="skip",
        ))

    fig.add_trace(go.Scattergeo(
        lon=[ch["lon"]], lat=[ch["lat"]], mode="markers+text",
        marker=dict(size=14, color=color, line=dict(width=2, color=color)),
        text=[price_str], textposition="top center",
        textfont=dict(family=FONT_MONO, size=16, color=color),
        hovertemplate="Cushing, OK<br>WTI Delivery Hub<br>" + price_str + "<extra></extra>",
        showlegend=False,
    ))
    fig.add_trace(go.Scattergeo(
        lon=[ch["lon"]], lat=[ch["lat"]], mode="text",
        text=["CUSHING"], textposition="bottom center",
        textfont=dict(family=FONT_MONO, size=9, color=COLORS["muted"]),
        showlegend=False, hoverinfo="skip",
    ))

    fig.update_layout(
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
        margin=dict(l=0, r=0, t=0, b=0),
        geo=dict(
            showframe=False, bgcolor=COLORS["bg"],
            landcolor="#edf0f4", lakecolor=COLORS["bg"],
            showlakes=True, showland=True,
            showcoastlines=True, coastlinecolor=COLORS["border"],
            showsubunits=True, subunitcolor="#d4dae3",
            showcountries=True, countrycolor=COLORS["border"], countrywidth=1.5,
            lonaxis=dict(range=[-118, -72], showgrid=False),
            lataxis=dict(range=[26, 56], showgrid=False),
        ),
        hoverlabel=dict(bgcolor=COLORS["panel"], bordercolor=COLORS["border"],
                        font=dict(family=FONT_MONO, size=11, color=COLORS["text"])),
    )
    return fig


# ── Timeline sparkline ────────────────────────────────────────────────────────

def build_timeline(daily: pd.DataFrame, settl: pd.DataFrame,
                   selected_date: str | None) -> go.Figure:
    """Compact sparkline: daily closes (area) + settlement diff bars. Clickable."""
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.65, 0.35], vertical_spacing=0.0,
    )

    if daily.empty:
        fig.update_layout(
            paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
            margin=dict(l=0, r=0, t=0, b=0), height=80,
        )
        return fig

    fig.add_trace(go.Scatter(
        x=daily.index, y=daily["Close"],
        fill="tozeroy", fillcolor="rgba(100,116,139,0.06)",
        line=dict(color=COLORS["muted"], width=1),
        mode="lines", showlegend=False, name="Close",
        hovertemplate="%{x|%b %d, %Y}<br>$%{y:.2f}<extra></extra>",
    ), row=1, col=1)

    if not settl.empty and "close_chg" in settl.columns:
        bar_colors = [COLORS["up"] if v >= 0 else COLORS["down"]
                      for v in settl["close_chg"].fillna(0)]
        fig.add_trace(go.Bar(
            x=settl["date"], y=settl["close_chg"],
            marker_color=bar_colors, marker_opacity=0.5,
            showlegend=False, name="Δ",
            hovertemplate="%{x|%b %d}<br>$%{y:+.2f}<extra>Settle Δ</extra>",
        ), row=2, col=1)

    if selected_date:
        for row_n in (1, 2):
            fig.add_vline(
                x=selected_date, line_dash="dot",
                line_color=COLORS["accent"], line_width=1.5,
                row=row_n, col=1,
            )

    fig.update_layout(
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
        margin=dict(l=50, r=50, t=0, b=0), height=90,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   linecolor=COLORS["border"]),
        xaxis2=dict(showgrid=False, zeroline=False, showticklabels=True,
                    tickfont=dict(family=FONT_MONO, size=9, color=COLORS["muted"]),
                    linecolor=COLORS["border"]),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis2=dict(showgrid=False, zeroline=True,
                    zerolinecolor=COLORS["border"], showticklabels=False),
        hovermode="x unified",
        hoverlabel=dict(bgcolor=COLORS["panel"], bordercolor=COLORS["border"],
                        font=dict(family=FONT_MONO, size=10, color=COLORS["text"])),
        dragmode=False,
    )
    return fig


# ── Stat card helper ──────────────────────────────────────────────────────────

def stat(label: str, value: str, color: str = None) -> html.Div:
    return html.Div([
        html.Span(label, style={
            "fontSize": "10px", "letterSpacing": "0.12em",
            "textTransform": "uppercase", "color": COLORS["muted"],
            "display": "block", "marginBottom": "3px", "fontFamily": FONT_MONO,
        }),
        html.Span(value, style={
            "fontSize": "17px", "fontWeight": "600",
            "color": color or COLORS["text"],
            "fontFamily": FONT_MONO, "letterSpacing": "-0.01em",
        }),
    ], style={
        "background": COLORS["panel"],
        "border": f"1px solid {COLORS['border']}",
        "borderRadius": "6px", "padding": "12px 16px", "minWidth": "110px",
    })


# ── App ───────────────────────────────────────────────────────────────────────

app = dash.Dash(__name__, title="WTI Live")
app.index_string = f"""
<!DOCTYPE html>
<html>
<head>
    {{%metas%}}
    <title>{{%title%}}</title>
    {{%favicon%}}
    {{%css%}}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ background: {COLORS['bg']}; color: {COLORS['text']}; font-family: {FONT_SANS}; }}
        ::-webkit-scrollbar {{ width: 4px; }}
        ::-webkit-scrollbar-track {{ background: {COLORS['bg']}; }}
        ::-webkit-scrollbar-thumb {{ background: {COLORS['border']}; border-radius: 2px; }}
        .custom-tabs {{ border-bottom: 1px solid {COLORS['border']} !important; padding: 0 20px; }}
        .custom-tab {{ font-family: {FONT_MONO}; font-size: 11px; letter-spacing: 0.08em;
            color: {COLORS['muted']}; background: transparent; border: none;
            padding: 10px 16px 8px; cursor: pointer; border-bottom: 2px solid transparent; }}
        .custom-tab:hover {{ color: {COLORS['text']}; }}
        .custom-tab--selected {{ color: {COLORS['text']} !important;
            border-bottom: 2px solid {COLORS['text']} !important; background: transparent !important; }}
        @keyframes pulse {{ 0%,100% {{ opacity: 1; }} 50% {{ opacity: 0.3; }} }}
        .live-dot {{ display: inline-block; width: 7px; height: 7px; border-radius: 50%;
            background: {COLORS['up']}; margin-right: 6px; animation: pulse 1.5s ease-in-out infinite; }}
        .stale-dot {{ display: inline-block; width: 7px; height: 7px; border-radius: 50%;
            background: {COLORS['muted']}; margin-right: 6px; }}
    </style>
</head>
<body>
    {{%app_entry%}}
    <footer>{{%config%}}{{%scripts%}}{{%renderer%}}</footer>
</body>
</html>
"""

app.layout = html.Div([

    html.Div([
        html.Div([
            html.Span("WTI", style={
                "fontSize": "22px", "fontWeight": "600",
                "color": COLORS["text"], "fontFamily": FONT_MONO,
                "letterSpacing": "-0.02em",
            }),
            html.Span("CRUDE OIL · CL=F · NYMEX", style={
                "fontSize": "11px", "color": COLORS["muted"],
                "letterSpacing": "0.1em", "fontFamily": FONT_MONO,
                "marginLeft": "12px",
            }),
        ], style={"display": "flex", "alignItems": "baseline"}),

        html.Div(id="live-status", style={
            "display": "flex", "alignItems": "center", "gap": "10px",
            "fontSize": "11px", "fontFamily": FONT_MONO,
        }),
    ], style={
        "display": "flex", "justifyContent": "space-between",
        "alignItems": "center", "padding": "16px 20px 12px",
        "borderBottom": f"1px solid {COLORS['border']}",
    }),

    html.Div(id="stat-bar", style={
        "display": "flex", "gap": "8px", "flexWrap": "wrap",
        "padding": "12px 20px",
        "borderBottom": f"1px solid {COLORS['border']}",
    }),

    dcc.Tabs(id="tabs", value="map", className="custom-tabs", children=[

        dcc.Tab(label="FLOWS", value="map", className="custom-tab",
                selected_className="custom-tab--selected", children=[
            html.Div([
                html.Div([
                    html.Span("INFRASTRUCTURE", style={
                        "fontSize": "10px", "letterSpacing": "0.12em",
                        "color": COLORS["muted"], "fontFamily": FONT_MONO,
                    }),
                    html.Span(" · Pipeline & Terminal Network", style={
                        "fontSize": "10px", "color": COLORS["muted"],
                        "fontFamily": FONT_MONO, "opacity": "0.5",
                    }),
                ], style={"display": "inline-flex", "gap": "0"}),
                html.Div(id="flow-stats", style={
                    "display": "inline-flex", "gap": "14px",
                    "marginLeft": "20px",
                    "fontSize": "11px", "fontFamily": FONT_MONO,
                }),
                html.Div(id="playback-label", style={
                    "marginLeft": "auto", "display": "flex",
                    "alignItems": "center", "gap": "8px",
                }),
            ], style={"padding": "6px 20px 0", "display": "flex",
                       "alignItems": "center"}),
            dcc.Graph(
                id="geo-map",
                config={"displayModeBar": False, "displaylogo": False},
                animate=True,
                animation_options={"frame": {"redraw": False},
                                   "transition": {"duration": 200,
                                                   "easing": "cubic-in-out"}},
                style={"height": "calc(100vh - 280px)"},
            ),
            dcc.Graph(
                id="timeline",
                config={"displayModeBar": False, "displaylogo": False},
                style={"height": "90px", "padding": "0 0px",
                       "cursor": "pointer"},
            ),
        ]),

        dcc.Tab(label="CHART", value="chart", className="custom-tab",
                selected_className="custom-tab--selected", children=[
            dcc.Graph(
                id="chart",
                config={
                    "displayModeBar": True,
                    "modeBarButtonsToRemove": ["autoScale2d", "lasso2d", "select2d"],
                    "displaylogo": False,
                    "toImageButtonOptions": {"format": "png", "scale": 2},
                },
                style={"height": "calc(100vh - 160px)"},
            ),
        ]),

    ]),

    dcc.Store(id="flow-store"),
    dcc.Store(id="selected-date", data=None),
    dcc.Interval(id="stat-tick",     interval=500,    n_intervals=0),
    dcc.Interval(id="flow-tick",     interval=250,    n_intervals=0),
    dcc.Interval(id="chart-tick",    interval=15_000, n_intervals=0),
    dcc.Interval(id="timeline-tick", interval=30_000, n_intervals=0),

], style={"minHeight": "100vh", "background": COLORS["bg"]})

server = app.server


@server.after_request
def allow_embed(response):
    """Allow GitHub Pages and local preview to iframe this app."""
    response.headers["Content-Security-Policy"] = (
        "frame-ancestors 'self' "
        "https://wock9000.github.io "
        "http://127.0.0.1:* "
        "http://localhost:*;"
    )
    response.headers.pop("X-Frame-Options", None)
    return response


# ── Callbacks ─────────────────────────────────────────────────────────────────

@app.callback(
    Output("selected-date", "data"),
    Input("timeline", "clickData"),
    prevent_initial_call=True,
)
def select_date(click):
    """Click on timeline sparkline → set selected date (or clear to live)."""
    if not click or not click.get("points"):
        return None
    x = click["points"][0].get("x")
    if not x:
        return None
    clicked = str(x)[:10]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return None if clicked >= today else clicked


@app.callback(
    Output("stat-bar",       "children"),
    Output("live-status",    "children"),
    Output("flow-store",     "data"),
    Output("flow-stats",     "children"),
    Output("playback-label", "children"),
    Input("stat-tick",       "n_intervals"),
    State("selected-date",   "data"),
)
def refresh_stats(_, sel_date):
    db = _get_mdb()
    is_playback = sel_date is not None

    if is_playback:
        s = db.day_summary(sel_date)
    else:
        s = db.session()

    supply = db.supply_chain_summary()
    flows = simulate_flows(s, supply)
    fs    = flow_summary(flows)
    now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    # Live status indicator
    elapsed = time.time() - db.last_tick_time if db.last_tick_time else 999
    is_live = elapsed < STALE_TIMEOUT and not is_playback
    source_labels = {
        "databento": "Databento MBP-1",
        "yf-poll": "yfinance", "yfinance": "yfinance", "none": "—",
    }
    dot_cls = "live-dot" if is_live else "stale-dot"
    label   = "LIVE" if is_live else ("PLAYBACK" if is_playback else "DELAYED")
    lbl_clr = COLORS["up"] if is_live else (COLORS["accent"] if is_playback else COLORS["muted"])
    source  = source_labels.get(db.source, db.source) if not is_playback else "daily_bars"
    spread_str = f" · spread ${s.get('spread', 0):.3f}" if s.get("spread") else ""
    ticks_str  = f" · {s.get('ticks', 0):,} ticks" if s.get("ticks") else ""
    extra = f"{spread_str}{ticks_str}" if not is_playback else ""
    status = [
        html.Span(className=dot_cls),
        html.Span(label, style={"fontWeight": "600", "color": lbl_clr,
                                 "letterSpacing": "0.06em"}),
        html.Span(f"{now_utc} · {source}{extra}",
                   style={"color": COLORS["muted"]}),
    ]

    # Playback date label (shown in flow header bar)
    if is_playback:
        playback_label = [
            html.Span(sel_date, style={
                "color": COLORS["accent"], "fontWeight": "600",
                "fontFamily": FONT_MONO, "fontSize": "11px",
                "letterSpacing": "0.04em",
            }),
            html.Span("← click latest to return to LIVE", style={
                "color": COLORS["muted"], "fontFamily": FONT_MONO,
                "fontSize": "9px", "opacity": "0.6",
            }),
        ]
    else:
        playback_label = []

    if not s:
        return ([html.Span("No data", style={"color": COLORS["muted"]})],
                status, {"summary": {}, "flows": []}, [], playback_label)

    chg_color = COLORS["up"] if s["chg"] >= 0 else COLORS["down"]
    sign      = "+" if s["chg"] >= 0 else ""

    stats = [
        stat("Last",   f"${s['last']:.2f}",                              chg_color),
        stat("Change", f"{sign}{s['chg']:.2f} ({sign}{s['pct']:.2f}%)", chg_color),
        stat("Open",   f"${s['first']:.2f}"),
        stat("High",   f"${s['hi']:.2f}",   COLORS["up"]),
        stat("Low",    f"${s['lo']:.2f}",   COLORS["down"]),
        stat("VWAP",   f"${s['vwap']:.2f}", COLORS["accent"]),
        stat("Volume", f"{s['vol']:,}"),
    ]

    net_sign  = "+" if fs["net"] >= 0 else ""
    net_color = COLORS["up"] if fs["net"] >= 0 else COLORS["down"]
    arrow     = "▲" if fs["net"] >= 0 else "▼"
    flow_stats = [
        html.Span(["IN ", html.B(f"{fs['inflow']:,}")],
                   style={"color": COLORS["inflow"]}),
        html.Span(["OUT ", html.B(f"{fs['outflow']:,}")],
                   style={"color": COLORS["outflow"]}),
        html.Span(["NET ", html.B(f"{net_sign}{fs['net']:,}")],
                   style={"color": net_color}),
        html.Span("kb/d", style={"color": COLORS["muted"], "opacity": "0.5"}),
        html.Span(f"{arrow} {fs['signal']}",
                   style={"color": net_color, "fontWeight": "600",
                           "letterSpacing": "0.05em"}),
    ]

    return stats, status, {"summary": s, "flows": flows}, flow_stats, playback_label


@app.callback(
    Output("geo-map", "figure"),
    Input("flow-tick", "n_intervals"),
    State("flow-store", "data"),
)
def animate_flows(n, store):
    s     = store.get("summary", {}) if store else {}
    flows = store.get("flows", [])   if store else []
    return build_geo_figure(s, flows, n)


@app.callback(
    Output("timeline", "figure"),
    Input("timeline-tick", "n_intervals"),
    State("selected-date", "data"),
)
def refresh_timeline(_, sel_date):
    db = _get_mdb()
    daily = db.daily_ohlcv(252)
    settl = db.settlements(252)
    return build_timeline(daily, settl, sel_date)


@app.callback(
    Output("chart", "figure"),
    Input("chart-tick", "n_intervals"),
)
def refresh_chart(_):
    df = _get_mdb().bars_1m()
    return build_figure(df)


# ── Entry point ───────────────────────────────────────────────────────────────

def _boot():
    """Initialize MarketDB and start feed threads (only in reloader child)."""
    market = _get_mdb()
    market.bootstrap_yf()
    market.backfill_daily()
    threading.Thread(target=_run_databento,       args=(market,), daemon=True).start()
    threading.Thread(target=_run_eia_poll,         args=(market,), daemon=True).start()
    threading.Thread(target=_run_eia_supply_poll,  args=(market,), daemon=True).start()

    db_status = "DATABENTO_API_KEY set" if DB_API_KEY else "no key — yfinance fallback"
    print("\n  WTI Live Dashboard")
    print("  ─────────────────────────────────────")
    print("  http://localhost:8050")
    print(f"  Tier 1:  Databento {DB_DATASET} / {DB_SYMBOL} / {DB_SCHEMA}")
    print(f"           {db_status}")
    print(f"  Tier 2:  yfinance {TICKER} 1m bars (fallback)")
    print(f"  Tier 3:  EIA weekly Cushing inventory")
    print(f"  Tier 4:  EIA supply chain (PADD moves, refinery util, trade)")
    print(f"  Store:   DuckDB → {DB_PATH}")
    print(f"  Nodes:   {len(WTI_NODES)} infrastructure points")
    print(f"  Routes:  {len(FLOW_ROUTES)} pipeline segments")
    print("  Ctrl+C to stop\n")


# Gunicorn (Render): import name is "app" — boot feeds here.
if __name__ != "__main__":
    _boot()

if __name__ == "__main__":
    # Werkzeug debug reloader: parent watches files, child serves.
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        _boot()
    app.run(debug=True, host="0.0.0.0", port=8050)
