"""
Portfolio visualization module.

All functions accept pre-computed DataFrames / dicts and return Plotly figures.
No calculation logic lives here — purely presentation.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from typing import Optional

_BG = "#0a0e1a"
_PLOT_BG = "#0f1525"
_GRID = "#1e2a42"
_TEXT = "#7a8fb5"
_GREEN = "#00c8a0"
_RED = "#ff4d6d"
_YELLOW = "#f4c542"
_WHITE = "#c8d4e8"

_BASE = dict(
    template="plotly_dark",
    paper_bgcolor=_BG,
    plot_bgcolor=_PLOT_BG,
    font=dict(color=_WHITE, size=11),
    margin=dict(l=10, r=10, t=44, b=10),
)


def _n(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def holdings_pie(enriched_df: pd.DataFrame) -> Optional[go.Figure]:
    """Donut chart of current holdings by market value."""
    if enriched_df is None or enriched_df.empty:
        return None
    if "Symbol" not in enriched_df.columns or "Market Value" not in enriched_df.columns:
        return None

    df = enriched_df[["Symbol", "Market Value"]].copy()
    df["mv"] = _n(df["Market Value"])
    df = df[df["mv"] > 0].dropna(subset=["mv"]).sort_values("mv", ascending=False)
    if df.empty:
        return None

    fig = go.Figure(go.Pie(
        labels=df["Symbol"],
        values=df["mv"],
        hole=0.48,
        marker=dict(line=dict(color=_BG, width=2)),
        textinfo="label+percent",
        textfont=dict(size=11),
        insidetextorientation="radial",
        hovertemplate="<b>%{label}</b><br>$%{value:,.0f}<br>%{percent}<extra></extra>",
    ))
    fig.update_layout(
        **_BASE,
        title=dict(text="Holdings Composition by Market Value", font=dict(color=_WHITE, size=13)),
        showlegend=False,
        height=340,
    )
    return fig


def pnl_bar(enriched_df: pd.DataFrame, pnl_col: str = "PnL") -> Optional[go.Figure]:
    """Horizontal bar chart of unrealized P&L by symbol, green/red colored."""
    if enriched_df is None or enriched_df.empty:
        return None
    if "Symbol" not in enriched_df.columns or pnl_col not in enriched_df.columns:
        return None

    df = enriched_df[["Symbol", pnl_col]].copy()
    df["val"] = _n(df[pnl_col])
    df = df.dropna(subset=["val"]).sort_values("val")
    if df.empty:
        return None

    colors = [_GREEN if v >= 0 else _RED for v in df["val"]]
    fig = go.Figure(go.Bar(
        x=df["val"],
        y=df["Symbol"],
        orientation="h",
        marker_color=colors,
        text=[f"${v:+,.0f}" for v in df["val"]],
        textposition="outside",
        textfont=dict(size=10, color=_WHITE),
        hovertemplate="<b>%{y}</b><br>$%{x:+,.2f}<extra></extra>",
    ))
    fig.add_vline(x=0, line_color=_TEXT, line_width=1, opacity=0.5)
    fig.update_layout(
        **_BASE,
        title=dict(text="Unrealized P&L by Symbol", font=dict(color=_WHITE, size=13)),
        xaxis=dict(gridcolor=_GRID, color=_TEXT, title="USD"),
        yaxis=dict(gridcolor=_GRID, color=_TEXT),
        height=max(300, len(df) * 30 + 70),
    )
    return fig


def cost_vs_mv_bar(enriched_df: pd.DataFrame) -> Optional[go.Figure]:
    """Grouped bar comparing cost basis vs current market value per symbol."""
    if enriched_df is None or enriched_df.empty:
        return None
    if "Symbol" not in enriched_df.columns or "Market Value" not in enriched_df.columns:
        return None

    df = enriched_df.copy()
    df["mv"] = _n(df["Market Value"])
    df = df[df["mv"] > 0].dropna(subset=["mv"])

    # Derive cost basis: prefer stored column, else Qty × Diluted Avg Cost
    if "Cost Basis ($)" in df.columns:
        df["cb"] = _n(df["Cost Basis ($)"])
    elif "Qty" in df.columns and "Diluted Avg Cost" in df.columns:
        df["cb"] = _n(df["Qty"]) * _n(df["Diluted Avg Cost"])
    else:
        return None

    df = df.dropna(subset=["cb"]).sort_values("mv", ascending=False)
    if df.empty:
        return None

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Cost Basis",
        x=df["Symbol"],
        y=df["cb"],
        marker_color=_YELLOW,
        opacity=0.85,
        hovertemplate="<b>%{x}</b><br>Cost: $%{y:,.0f}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Market Value",
        x=df["Symbol"],
        y=df["mv"],
        marker_color=_GREEN,
        opacity=0.85,
        hovertemplate="<b>%{x}</b><br>Market Value: $%{y:,.0f}<extra></extra>",
    ))
    fig.update_layout(
        **_BASE,
        title=dict(text="Cost Basis vs Market Value", font=dict(color=_WHITE, size=13)),
        barmode="group",
        xaxis=dict(gridcolor=_GRID, color=_TEXT),
        yaxis=dict(gridcolor=_GRID, color=_TEXT, title="USD"),
        legend=dict(orientation="h", y=1.06, font=dict(size=10, color=_TEXT)),
        height=300,
    )
    return fig


def monthly_trade_activity(trades_df: pd.DataFrame) -> Optional[go.Figure]:
    """Bar chart of monthly trade count from raw trades DataFrame."""
    if trades_df is None or trades_df.empty:
        return None
    if "date" not in trades_df.columns:
        return None

    df = trades_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return None

    df["month"] = df["date"].dt.to_period("M")
    monthly = (
        df.groupby("month")
        .size()
        .reset_index(name="count")
        .sort_values("month")
    )
    monthly["label"] = monthly["month"].astype(str)

    fig = go.Figure(go.Bar(
        x=monthly["label"],
        y=monthly["count"],
        marker_color=_GREEN,
        marker_opacity=0.8,
        text=monthly["count"],
        textposition="outside",
        textfont=dict(size=9, color=_WHITE),
        hovertemplate="<b>%{x}</b><br>%{y} trades<extra></extra>",
    ))
    fig.update_layout(
        **_BASE,
        title=dict(text="Monthly Trade Activity", font=dict(color=_WHITE, size=13)),
        xaxis=dict(gridcolor=_GRID, color=_TEXT, tickangle=-45),
        yaxis=dict(gridcolor=_GRID, color=_TEXT, title="# of Trades"),
        height=290,
    )
    return fig


def cumulative_realized_pnl(portfolio: dict) -> Optional[go.Figure]:
    """Bar chart of realized P&L for all symbols in the portfolio dict."""
    if not portfolio:
        return None

    rows = [
        {"Symbol": sym, "Realized P&L": float(info.get("realized_pnl", 0) or 0)}
        for sym, info in portfolio.items()
        if abs(float(info.get("realized_pnl", 0) or 0)) > 0.01
    ]
    if not rows:
        return None

    df = pd.DataFrame(rows).sort_values("Realized P&L")
    colors = [_GREEN if v >= 0 else _RED for v in df["Realized P&L"]]

    fig = go.Figure(go.Bar(
        x=df["Symbol"],
        y=df["Realized P&L"],
        marker_color=colors,
        text=[f"${v:+,.0f}" for v in df["Realized P&L"]],
        textposition="outside",
        textfont=dict(size=9, color=_WHITE),
        hovertemplate="<b>%{x}</b><br>$%{y:+,.2f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_color=_TEXT, line_width=1, opacity=0.5)
    fig.update_layout(
        **_BASE,
        title=dict(text="Realized P&L by Symbol", font=dict(color=_WHITE, size=13)),
        xaxis=dict(gridcolor=_GRID, color=_TEXT, tickangle=-45),
        yaxis=dict(gridcolor=_GRID, color=_TEXT, title="USD"),
        height=310,
    )
    return fig
