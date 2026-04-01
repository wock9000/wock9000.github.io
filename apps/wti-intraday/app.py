"""
NYMEX WTI front-month (CL=F) — Dash app for Render free tier.
Embeddable on GitHub Pages via CSP frame-ancestors.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import yfinance as yf
from dash import Dash, Input, Output, callback, dcc, html
from plotly.subplots import make_subplots

COLORS = {
    "bg": "#0a0c0f",
    "panel": "#111418",
    "text": "#e2e8f0",
    "muted": "#4a5568",
    "up": "#26a69a",
    "down": "#ef5350",
    "accent": "#f59e0b",
}

app = Dash(__name__)
server = app.server
app.title = "WTI intraday (CL=F)"


@server.after_request
def allow_embed(response):
    response.headers["Content-Security-Policy"] = (
        "frame-ancestors 'self' "
        "https://wock9000.github.io "
        "http://127.0.0.1:* "
        "http://localhost:*;"
    )
    response.headers.pop("X-Frame-Options", None)
    return response


def fetch_ohlcv():
    t = yf.Ticker("CL=F")
    df = t.history(period="5d", interval="30m", prepost=False)
    if df.empty or len(df) < 4:
        df = t.history(period="1mo", interval="1d", prepost=False)
    return df


def figure_from_df(df: pd.DataFrame) -> go.Figure:
    if df is None or df.empty:
        fig = go.Figure()
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor=COLORS["bg"],
            plot_bgcolor=COLORS["bg"],
            annotations=[
                dict(
                    text="No data from yfinance (market closed or rate limit). Retry shortly.",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(color=COLORS["text"], size=14),
                )
            ],
            height=600,
        )
        return fig

    df = df.copy()
    tp = (df["High"] + df["Low"] + df["Close"]) / 3.0
    df["VWAP"] = (tp * df["Volume"]).cumsum() / df["Volume"].cumsum()

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.72, 0.28],
        vertical_spacing=0.05,
    )

    fig.add_trace(
        go.Candlestick(
            x=df.index,
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name="CL=F",
            increasing_line_color=COLORS["up"],
            decreasing_line_color=COLORS["down"],
            line=dict(width=1),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df["VWAP"],
            mode="lines",
            name="VWAP",
            line=dict(color=COLORS["accent"], width=1.5, dash="dot"),
        ),
        row=1,
        col=1,
    )

    vol_colors = [
        COLORS["up"] if c >= o else COLORS["down"]
        for c, o in zip(df["Close"], df["Open"], strict=False)
    ]
    fig.add_trace(
        go.Bar(
            x=df.index,
            y=df["Volume"],
            name="Volume",
            marker_color=vol_colors,
            opacity=0.45,
            showlegend=False,
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["panel"],
        font=dict(family="JetBrains Mono, ui-monospace, monospace", color=COLORS["text"], size=11),
        margin=dict(l=52, r=36, t=40, b=40),
        xaxis_rangeslider_visible=False,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=640,
    )
    fig.update_xaxes(gridcolor="#1e2329", row=1, col=1)
    fig.update_xaxes(gridcolor="#1e2329", row=2, col=1)
    fig.update_yaxes(title_text="USD/bbl", gridcolor="#1e2329", row=1, col=1)
    fig.update_yaxes(title_text="Volume", gridcolor="#1e2329", row=2, col=1)
    return fig


def stat_children(df: pd.DataFrame) -> list:
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    if df is None or df.empty:
        return [html.Span(f"Updated {now} — no rows", style={"color": COLORS["muted"]})]

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last
    chg = float(last["Close"] - prev["Close"])
    pct = 100.0 * chg / float(prev["Close"]) if prev["Close"] else 0.0
    direction = COLORS["up"] if chg >= 0 else COLORS["down"]
    tp = (df["High"] + df["Low"] + df["Close"]) / 3.0
    vwap = float((tp * df["Volume"]).cumsum().iloc[-1] / df["Volume"].cumsum().iloc[-1])

    def item(label: str, value: str, color: str | None = None):
        c = color or COLORS["text"]
        return html.Span(
            [html.Strong(f"{label}: "), html.Span(value, style={"color": c})],
            style={"marginRight": "1.25rem"},
        )

    parts = [
        item("Last", f"{last['Close']:.2f}", direction),
        item("Chg", f"{chg:+.2f} ({pct:+.2f}%)", direction),
        item("Open", f"{last['Open']:.2f}"),
        item("High", f"{df['High'].max():.2f}", COLORS["up"]),
        item("Low", f"{df['Low'].min():.2f}", COLORS["down"]),
        item("VWAP", f"{vwap:.2f}", COLORS["accent"]),
        item("Vol", f"{int(last['Volume']):,}"),
        html.Span(
            f" · {now}",
            style={"color": COLORS["muted"], "marginLeft": "0.5rem"},
        ),
    ]
    return parts


_df = fetch_ohlcv()

app.layout = html.Div(
    style={
        "backgroundColor": COLORS["bg"],
        "minHeight": "100vh",
        "padding": "12px 16px",
        "color": COLORS["text"],
        "fontFamily": "'DM Sans', system-ui, sans-serif",
    },
    children=[
        html.Div(
            id="stat-bar",
            style={
                "display": "flex",
                "flexWrap": "wrap",
                "alignItems": "baseline",
                "marginBottom": "10px",
                "fontSize": "13px",
                "fontFamily": "JetBrains Mono, ui-monospace, monospace",
            },
            children=stat_children(_df),
        ),
        dcc.Graph(id="chart", figure=figure_from_df(_df), config={"displayModeBar": True}),
        dcc.Interval(id="timer", interval=60_000, n_intervals=0),
    ],
)


@callback(
    Output("chart", "figure"),
    Output("stat-bar", "children"),
    Input("timer", "n_intervals"),
)
def refresh(_n: int):
    df = fetch_ohlcv()
    return figure_from_df(df), stat_children(df)


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8050)
