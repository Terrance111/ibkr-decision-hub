import math
import numpy as np
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
from pathlib import Path

src_path = Path(__file__).parent / "src"
sys.path.append(str(src_path))

from data.ibkr_fetch import fetch_ibkr_trades
from data.ibkr_account import (
    COL_DILUTED_AVG_COST_TRADES,
    COL_UNREALIZED_PNL_EST,
    COL_UNREALIZED_PNL_PCT,
    enrich_positions_with_trade_cost,
    fetch_ibkr_positions_and_cash,
)
from core.trade_processor import process_trades
from core.market_data import get_prices_for_portfolio
from core.stock_analysis import get_stock_analysis
from core.portfolio_charts import (
    holdings_pie,
    pnl_bar,
    cost_vs_mv_bar,
    monthly_trade_activity,
    cumulative_realized_pnl,
)
from monitors.liquidity_monitor import (
    get_fear_greed_data,
    get_liquidity_indicators,
    get_vix_history,
    get_nfci_history,
    get_yield_spread_history,
)
from monitors.daily_brief import get_daily_brief

# ====================== Page Config ======================
st.set_page_config(page_title="IBKR Decision Hub", page_icon="◈", layout="wide")

# ====================== Global CSS ======================
st.markdown("""
<style>
/* ── Base ── */
html, body, [data-testid="stAppViewContainer"] {
    background-color: #0a0e1a;
}
[data-testid="stSidebar"] {
    background-color: #0f1525;
    border-right: 1px solid #1e2a42;
}
[data-testid="stSidebar"] * { color: #c8d4e8 !important; }

/* ── Metric cards ── */
[data-testid="stMetric"] {
    background: #141a2e;
    border: 1px solid #1e2a42;
    border-radius: 8px;
    padding: 14px 18px 10px 18px;
}
[data-testid="stMetricLabel"] { color: #7a8fb5 !important; font-size: 0.78rem !important; letter-spacing: 0.04em; }
[data-testid="stMetricValue"] { color: #e8ecf0 !important; font-size: 1.5rem !important; font-weight: 700; }
[data-testid="stMetricDelta"] svg { display: none; }

/* ── Tabs ── */
[data-testid="stTabs"] button {
    font-size: 0.85rem !important;
    font-weight: 600 !important;
    color: #7a8fb5 !important;
    border-bottom: 2px solid transparent !important;
    padding: 8px 18px !important;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: #00c8a0 !important;
    border-bottom: 2px solid #00c8a0 !important;
}

/* ── Buttons ── */
[data-testid="stButton"] button[kind="primary"] {
    background: linear-gradient(135deg, #00c8a0, #00a882) !important;
    color: #0a0e1a !important;
    font-weight: 700 !important;
    border: none !important;
    border-radius: 6px !important;
}
[data-testid="stButton"] button[kind="primary"]:hover {
    background: linear-gradient(135deg, #00dbb0, #00c8a0) !important;
}

/* ── Divider ── */
hr { border-color: #1e2a42 !important; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] { border: 1px solid #1e2a42 !important; border-radius: 8px; overflow: hidden; }

/* ── Section headers ── */
.section-header {
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    color: #7a8fb5;
    text-transform: uppercase;
    margin-bottom: 12px;
    padding-bottom: 6px;
    border-bottom: 1px solid #1e2a42;
}

/* ── Stat chips ── */
.stat-chip {
    display: inline-block;
    background: #141a2e;
    border: 1px solid #1e2a42;
    border-radius: 20px;
    padding: 3px 12px;
    font-size: 0.78rem;
    color: #c8d4e8;
    margin: 3px 3px 3px 0;
}
.chip-green { border-color: #00c8a0; color: #00c8a0 !important; }
.chip-red   { border-color: #ff4d6d; color: #ff4d6d !important; }
.chip-gold  { border-color: #f4c542; color: #f4c542 !important; }

/* ── Rating badge ── */
.rating-badge {
    display: inline-block;
    padding: 4px 14px;
    border-radius: 4px;
    font-size: 0.82rem;
    font-weight: 700;
    letter-spacing: 0.05em;
}
.rating-strong-buy { background: #003d2e; color: #00c8a0; border: 1px solid #00c8a0; }
.rating-buy        { background: #002a20; color: #00e8b0; border: 1px solid #00e8b0; }
.rating-hold       { background: #2a2500; color: #f4c542; border: 1px solid #f4c542; }
.rating-sell       { background: #3d0015; color: #ff4d6d; border: 1px solid #ff4d6d; }
.rating-na         { background: #1a1f30; color: #7a8fb5; border: 1px solid #2a3350; }

/* ── App title ── */
h1 { color: #e8ecf0 !important; font-size: 1.4rem !important; font-weight: 700 !important; letter-spacing: -0.01em; }
h2, h3 { color: #c8d4e8 !important; }
</style>
""", unsafe_allow_html=True)

# ====================== Helpers ======================
def _holding_pnl_and_pct(price: float, info: dict) -> tuple[float, float]:
    sh = float(info.get("current_shares", 0) or 0)
    ac = float(info.get("avg_cost", 0) or 0)
    if abs(sh) > 1e-9:
        pnl = (price - ac) * sh
        pct = (price - ac) / ac * 100 if ac > 1e-9 else float("nan")
    else:
        pnl = float(info.get("realized_pnl", 0) or 0)
        pct = float("nan")
    nan = float("nan")
    return round(pnl, 2), (round(pct, 2) if not math.isnan(pct) else nan)


def _trade_history_display(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df is None or trades_df.empty:
        return pd.DataFrame(columns=["Date", "Symbol", "Amount"])
    t = trades_df.copy()
    t["date"] = pd.to_datetime(t["date"], errors="coerce")
    pq = pd.to_numeric(t["quantity"], errors="coerce") * pd.to_numeric(t["price"], errors="coerce")
    if "proceeds" in t.columns:
        pr = pd.to_numeric(t["proceeds"], errors="coerce")
        amt = pr.where(pr.notna(), np.nan)
    else:
        amt = pd.Series(np.nan, index=t.index)
    if "action" not in t.columns:
        t["action"] = ""

    def _row_is_buy(a) -> bool:
        s = str(a).strip().upper()
        return s in ("B", "BOT", "BUY") or s.startswith("BUY")

    is_buy = t["action"].map(_row_is_buy)
    fallback = np.where(is_buy, -np.abs(pq.fillna(0)), np.abs(pq.fillna(0)))
    amt = amt.where(amt.notna(), fallback)
    out = pd.DataFrame({
        "Date": t["date"].dt.strftime("%Y-%m-%d"),
        "Symbol": t["symbol"].astype(str),
        "Amount": np.round(pd.to_numeric(amt, errors="coerce"), 2),
    })
    return out.sort_values("Date", ascending=False, na_position="last").reset_index(drop=True)


def _color_pnl(val):
    if isinstance(val, (int, float)) and not isinstance(val, bool) and pd.notna(val):
        if val > 0:
            return "background-color: #003d2e; color: #00c8a0"
        if val < 0:
            return "background-color: #3d0015; color: #ff4d6d"
    return ""


def _fmt(x, fmt: str, suffix: str = "") -> str:
    if isinstance(x, (int, float)) and not isinstance(x, bool) and pd.notna(x) and not math.isnan(x):
        return format(x, fmt) + suffix
    return "—"


def _val_color(v) -> str:
    """Return inline color style for a valuation/fundamental value."""
    try:
        f = float(v)
        if f > 0:
            return "color:#00c8a0"
        return "color:#ff4d6d"
    except Exception:
        return "color:#7a8fb5"


# ====================== Page Header ======================
col_title, col_date = st.columns([6, 1])
with col_title:
    st.markdown("# ◈ IBKR Decision Hub")
with col_date:
    st.markdown(f"<div style='text-align:right;color:#7a8fb5;font-size:0.78rem;padding-top:12px'>{datetime.now().strftime('%a %b %d, %Y')}</div>", unsafe_allow_html=True)

# ====================== Sidebar ======================
with st.sidebar:
    st.markdown("<div style='font-size:0.7rem;letter-spacing:0.1em;color:#7a8fb5;text-transform:uppercase;font-weight:700;padding-bottom:8px'>Control Panel</div>", unsafe_allow_html=True)

    from datetime import date as _date
    _sidebar_start = st.session_state.get("_sidebar_start_date", _date(2024, 1, 1))
    _picked_start = st.date_input(
        "Fetch history from",
        value=_sidebar_start,
        min_value=_date(2015, 1, 1),
        max_value=datetime.now().date(),
        help="Full re-fetch from this date when Refresh is clicked.",
        key="sidebar_date_picker",
    )

    if st.button("↺  Refresh All Data", type="primary", use_container_width=True):
        _start_str = _picked_start.strftime("%Y%m%d")
        st.session_state["_ibkr_flex_full_refresh"] = True
        st.session_state["_trade_start_date_override"] = _start_str
        st.session_state["_sidebar_start_date"] = _picked_start
        _keep = {"_ibkr_flex_full_refresh", "_trade_start_date_override", "_sidebar_start_date"}
        for key in list(st.session_state.keys()):
            if key not in _keep:
                del st.session_state[key]
        st.rerun()

    st.markdown("<hr style='margin:12px 0'>", unsafe_allow_html=True)
    st.markdown("<div style='font-size:0.7rem;letter-spacing:0.1em;color:#7a8fb5;text-transform:uppercase;font-weight:700;padding-bottom:8px'>Display</div>", unsafe_allow_html=True)
    view_mode = st.radio(
        "Portfolio View",
        ["Current Holdings", "All Historical Holdings"],
        horizontal=False,
        index=0,
    )

# ====================== Load Data ======================
_flex_full_refresh = st.session_state.pop("_ibkr_flex_full_refresh", False)
_trade_start_override = st.session_state.pop("_trade_start_date_override", None)

if "trades" not in st.session_state:
    try:
        st.session_state.trades = fetch_ibkr_trades(
            force_full=_flex_full_refresh,
            start_date_override=_trade_start_override,
        )
    except Exception:
        st.session_state.trades = pd.DataFrame()

if "account_data" not in st.session_state:
    try:
        st.session_state.account_data = fetch_ibkr_positions_and_cash(flex_refresh=_flex_full_refresh)
    except Exception:
        st.session_state.account_data = {
            "cash_balance": 0.0,
            "positions_flex_df": None,
            "positions_market_value_total": 0.0,
            "positions_flex_warning": None,
            "positions": {},
        }

trades = st.session_state.trades
ibkr_available = trades is not None and not trades.empty
portfolio = process_trades(trades) if ibkr_available else {}
account = st.session_state.account_data
cash_balance = float(account.get("cash_balance", 0.0))

# ====================== Stale Data Warning ======================
_trade_max_date = pd.to_datetime(
    trades["date"] if (trades is not None and not trades.empty and "date" in trades.columns) else [],
    errors="coerce",
).max()
_today_ts = pd.Timestamp.today().normalize()
if pd.notna(_trade_max_date):
    _stale_days = (_today_ts - pd.Timestamp(_trade_max_date)).days
    if _stale_days > 30:
        st.error(
            f"**Trade history is {_stale_days} days out of date** "
            f"(latest: **{pd.Timestamp(_trade_max_date).date()}**).  \n"
            "Fix: Client Portal → Flex Queries → edit your Trades query → set Period to **Last 3 Years** → Save → Refresh here."
        )

# ====================== Tabs ======================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Portfolio",
    "Liquidity",
    "Market Brief",
    "Trade History",
    "Stock Analysis",
])

# ─────────────────────────────────────────────────────
# TAB 1 — Portfolio Analysis
# ─────────────────────────────────────────────────────
with tab1:
    if not ibkr_available:
        st.markdown(
            "<div style='background:#141a2e;border:1px solid #1e2a42;border-radius:10px;"
            "padding:32px;margin:24px 0;text-align:center'>"
            "<div style='font-size:2rem;margin-bottom:10px'>⚡</div>"
            "<div style='color:#e8ecf0;font-weight:700;font-size:1.05rem;margin-bottom:8px'>"
            "IBKR Data Not Available</div>"
            "<div style='color:#7a8fb5;font-size:0.85rem;line-height:1.6'>"
            "Set <code>IBKR_FLEX_TOKEN</code>, <code>IBKR_FLEX_QUERY_ID</code>, and "
            "<code>IBKR_FLEX_POSITIONS_QUERY_ID</code> in your <code>.env</code> file, "
            "then click <strong style='color:#00c8a0'>↺ Refresh</strong> in the sidebar."
            "</div>"
            "<div style='color:#4a5a78;font-size:0.78rem;margin-top:10px'>"
            "Liquidity, Market Brief, and Stock Analysis tabs are fully available without IBKR credentials."
            "</div></div>",
            unsafe_allow_html=True,
        )
    elif view_mode == "Current Holdings":
        wmsg = account.get("positions_flex_warning")
        if wmsg:
            st.warning(wmsg)
        pflex = account.get("positions_flex_df")
        if pflex is not None and not pflex.empty:
            disp = enrich_positions_with_trade_cost(pflex, portfolio)
            if "Market Value" in disp.columns:
                disp = disp.assign(
                    _mv=pd.to_numeric(disp["Market Value"], errors="coerce")
                ).sort_values("_mv", ascending=False, na_position="last").drop(columns=["_mv"]).reset_index(drop=True)

            _price_cols = {COL_DILUTED_AVG_COST_TRADES, "Mark Price", "Avg Open Price", "Cost Basis Price"}
            fmt_map: dict = {}
            for col in disp.columns:
                if col in _price_cols:
                    fmt_map[col] = "{:.4f}"
                elif col == COL_UNREALIZED_PNL_EST:
                    fmt_map[col] = lambda x: _fmt(x, ",.2f")
                elif col == COL_UNREALIZED_PNL_PCT:
                    fmt_map[col] = lambda x: _fmt(x, ".2f", "%")
                elif col in ("Market Value", "Cost Basis ($)"):
                    fmt_map[col] = lambda x: _fmt(x, ",.2f")
                elif col == "% of NAV":
                    fmt_map[col] = lambda x: _fmt(x, ".4f", "%")
                elif col == "Qty":
                    fmt_map[col] = lambda x: _fmt(x, ",.4f")

            pnl_style_cols = [c for c in (COL_UNREALIZED_PNL_EST, COL_UNREALIZED_PNL_PCT) if c in disp.columns]
            sty = disp.style.format(fmt_map, na_rep="—")
            if pnl_style_cols:
                sty = sty.applymap(_color_pnl, subset=pnl_style_cols)
            st.dataframe(sty, use_container_width=True, hide_index=True)
        elif not wmsg:
            st.warning("No open position rows to display.")

        pv = float(account.get("positions_market_value_total") or 0)
        nb = pv + float(cash_balance)
        st.markdown("<br>", unsafe_allow_html=True)
        m1, m2, m3 = st.columns(3)
        m1.metric("Positions (Market Value)", f"${pv:,.0f}")
        m2.metric("Cash Balance", f"${cash_balance:,.2f}")
        m3.metric("Net Portfolio Value", f"${nb:,.0f}")

    else:
        pflex_hist = account.get("positions_flex_df")
        st.markdown("<div class='section-header'>Open Positions</div>", unsafe_allow_html=True)
        if pflex_hist is not None and not pflex_hist.empty:
            open_disp = enrich_positions_with_trade_cost(pflex_hist, portfolio)
            if "Market Value" in open_disp.columns:
                open_disp = (
                    open_disp
                    .assign(_mv=pd.to_numeric(open_disp["Market Value"], errors="coerce"))
                    .sort_values("_mv", ascending=False, na_position="last")
                    .drop(columns=["_mv"])
                    .reset_index(drop=True)
                )
            _price_cols_h = {COL_DILUTED_AVG_COST_TRADES, "Mark Price", "Avg Open Price", "Cost Basis Price"}
            fmt_h: dict = {}
            for col in open_disp.columns:
                if col in _price_cols_h:
                    fmt_h[col] = "{:.4f}"
                elif col == COL_UNREALIZED_PNL_EST:
                    fmt_h[col] = lambda x: _fmt(x, ",.2f")
                elif col == COL_UNREALIZED_PNL_PCT:
                    fmt_h[col] = lambda x: _fmt(x, ".2f", "%")
                elif col in ("Market Value", "Cost Basis ($)"):
                    fmt_h[col] = lambda x: _fmt(x, ",.2f")
                elif col == "% of NAV":
                    fmt_h[col] = lambda x: _fmt(x, ".4f", "%")
                elif col == "Qty":
                    fmt_h[col] = lambda x: _fmt(x, ",.4f")
            pnl_style_h = [c for c in (COL_UNREALIZED_PNL_EST, COL_UNREALIZED_PNL_PCT) if c in open_disp.columns]
            sty_h = open_disp.style.format(fmt_h, na_rep="—")
            if pnl_style_h:
                sty_h = sty_h.applymap(_color_pnl, subset=pnl_style_h)
            st.dataframe(sty_h, use_container_width=True, hide_index=True)

            pv_h = float(account.get("positions_market_value_total") or 0)
            nb_h = pv_h + float(cash_balance)
            st.markdown("<br>", unsafe_allow_html=True)
            c1, c2, c3 = st.columns(3)
            c1.metric("Positions", f"${pv_h:,.0f}")
            c2.metric("Cash", f"${cash_balance:,.0f}")
            c3.metric("Total", f"${nb_h:,.0f}")
        else:
            wmsg_h = account.get("positions_flex_warning")
            if wmsg_h:
                st.warning(wmsg_h)
            else:
                st.info("No open positions data available. Refresh from IBKR.")

        flex_syms = set()
        if pflex_hist is not None and not pflex_hist.empty:
            _s = next((c for c in pflex_hist.columns if c.strip().lower() == "symbol"), None)
            if _s:
                flex_syms = set(pflex_hist[_s].dropna().astype(str).str.strip())

        closed_rows = []
        for sym, info in portfolio.items():
            shares = float(info.get("current_shares", 0) or 0)
            if abs(shares) < 1e-9:
                realized = float(info.get("realized_pnl", 0) or 0)
                close_date = info.get("last_sell_date") or ""
                closed_rows.append({
                    "Symbol": sym,
                    "Realized P&L ($)": round(realized, 2),
                    "Close Date": close_date,
                })

        if closed_rows:
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown("<div class='section-header'>Closed Positions</div>", unsafe_allow_html=True)
            closed_df = (
                pd.DataFrame(closed_rows)
                .sort_values("Realized P&L ($)", ascending=False)
                .reset_index(drop=True)
            )
            closed_sty = (
                closed_df.style
                .format({"Realized P&L ($)": lambda x: _fmt(x, ",.2f")}, na_rep="—")
                .applymap(_color_pnl, subset=["Realized P&L ($)"])
            )
            st.dataframe(closed_sty, use_container_width=True, hide_index=True)

    # ── Chart Analysis expander (visible when IBKR data is loaded) ──
    if ibkr_available:
        st.markdown("<br>", unsafe_allow_html=True)
        with st.expander("Chart Analysis", expanded=False):
            if view_mode == "Current Holdings":
                _pflex_chart = account.get("positions_flex_df")
                if _pflex_chart is not None and not _pflex_chart.empty:
                    _enriched_chart = enrich_positions_with_trade_cost(_pflex_chart, portfolio)
                    _c1, _c2 = st.columns([1, 1])
                    with _c1:
                        _fig = holdings_pie(_enriched_chart)
                        if _fig:
                            st.plotly_chart(_fig, use_container_width=True)
                    with _c2:
                        _fig = pnl_bar(_enriched_chart)
                        if _fig:
                            st.plotly_chart(_fig, use_container_width=True)
                    _fig = cost_vs_mv_bar(_enriched_chart)
                    if _fig:
                        st.plotly_chart(_fig, use_container_width=True)
            else:
                _c1, _c2 = st.columns([1, 1])
                with _c1:
                    _fig = monthly_trade_activity(trades)
                    if _fig:
                        st.plotly_chart(_fig, use_container_width=True)
                with _c2:
                    _fig = cumulative_realized_pnl(portfolio)
                    if _fig:
                        st.plotly_chart(_fig, use_container_width=True)

# ─────────────────────────────────────────────────────
# TAB 2 — Liquidity Monitor
# ─────────────────────────────────────────────────────
with tab2:
    st.markdown("<div class='section-header'>US Equity Liquidity Monitor</div>", unsafe_allow_html=True)
    indicators = get_liquidity_indicators()
    fg_df = get_fear_greed_data()

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Fed Net Liquidity", f"${indicators['Fed_Net_Liquidity_B']}B", delta=indicators['Net_Liquidity_Trend'])
    with col2:
        st.metric("Chicago Fed NFCI", f"{indicators['NFCI']}", delta=indicators['NFCI_Interp'])
    with col3:
        st.metric("10Y–2Y Spread", f"{indicators['10Y_2Y_Spread']}%")
    with col4:
        st.metric("VIX", f"{indicators['VIX']}")
    with col5:
        if not fg_df.empty:
            st.metric("Fear & Greed", f"{fg_df['value'].iloc[-1]:.0f} / 100")

    st.markdown("<br>", unsafe_allow_html=True)
    if not fg_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=fg_df["timestamp"].tail(30),
            y=fg_df["value"].tail(30),
            mode="lines+markers",
            line=dict(color="#00c8a0", width=2),
            marker=dict(size=4, color="#00c8a0"),
            fill="tozeroy",
            fillcolor="rgba(0,200,160,0.08)",
            name="Fear & Greed",
        ))
        fig.add_hline(y=25, line_dash="dot", line_color="#ff4d6d", annotation_text="Extreme Fear", annotation_font_color="#ff4d6d")
        fig.add_hline(y=75, line_dash="dot", line_color="#f4c542", annotation_text="Extreme Greed", annotation_font_color="#f4c542")
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="#0a0e1a",
            plot_bgcolor="#0f1525",
            title=dict(text="CNN Fear & Greed — Last 30 Days", font=dict(color="#c8d4e8", size=13)),
            xaxis=dict(gridcolor="#1e2a42", color="#7a8fb5"),
            yaxis=dict(gridcolor="#1e2a42", color="#7a8fb5", range=[0, 100]),
            margin=dict(l=10, r=10, t=40, b=10),
            height=260,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div class='section-header'>Credit & Stress</div>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    c1.metric("High Yield OAS", f"{indicators['HY_OAS']}%")
    c2.metric("Overall Assessment", indicators["Overall_Assessment"])

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div class='section-header'>Margin & Leverage (NYSE Market Debt)</div>", unsafe_allow_html=True)
    _mg = indicators
    _mg_period = _mg.get("Margin_Period", "—")
    _mg_trend_color = "#ff4d6d" if _mg.get("Margin_Trend") == "Expanding" else ("#00c8a0" if _mg.get("Margin_Trend") == "Contracting" else "#f4c542")
    mc1, mc2, mc3 = st.columns(3)
    mc1.metric(
        f"Margin Debt  ({_mg_period})",
        f"${_mg.get('Margin_Debt_B', '—')}B",
    )
    mc2.metric(
        "MoM Change",
        f"{_mg.get('Margin_Change_Pct', '—'):+.1f}%" if isinstance(_mg.get("Margin_Change_Pct"), (int, float)) else "—",
    )
    with mc3:
        _trend = _mg.get("Margin_Trend", "—")
        st.markdown(
            f"<div style='background:#141a2e;border:1px solid #1e2a42;border-radius:8px;"
            f"padding:14px 18px 10px 18px'>"
            f"<div style='color:#7a8fb5;font-size:0.78rem;letter-spacing:0.04em'>Trend</div>"
            f"<div style='color:{_mg_trend_color};font-size:1.4rem;font-weight:700;margin-top:4px'>{_trend}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
    st.markdown(
        "<div style='color:#4a5a78;font-size:0.72rem;margin-top:6px'>"
        "Source: FINRA monthly margin statistics. Expanding margin debt signals rising leverage; "
        "sharp contractions often precede deleveraging events."
        "</div>",
        unsafe_allow_html=True,
    )

    # ── Indicator Trends ──
    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("Indicator Trends", expanded=True):
        _vix_df = get_vix_history("3mo")
        _nfci_df = get_nfci_history("2y")
        _spread_df = get_yield_spread_history("1y")
        _mg_history = indicators.get("Margin_History", [])

        _liq_c1, _liq_c2 = st.columns(2)

        # — VIX 90-day —
        with _liq_c1:
            if not _vix_df.empty:
                _fig_vix = go.Figure()
                _fig_vix.add_trace(go.Scatter(
                    x=_vix_df["date"], y=_vix_df["vix"],
                    mode="lines", line=dict(color="#f4c542", width=1.8),
                    fill="tozeroy", fillcolor="rgba(244,197,66,0.06)",
                    name="VIX",
                ))
                _fig_vix.add_hline(y=20, line_dash="dot", line_color="#00c8a0",
                                   annotation_text="20 (calm)", annotation_font_color="#00c8a0",
                                   annotation_position="right")
                _fig_vix.add_hline(y=30, line_dash="dot", line_color="#ff4d6d",
                                   annotation_text="30 (stress)", annotation_font_color="#ff4d6d",
                                   annotation_position="right")
                _fig_vix.update_layout(
                    template="plotly_dark", paper_bgcolor="#0a0e1a", plot_bgcolor="#0f1525",
                    title=dict(text="VIX — Last 90 Days", font=dict(color="#c8d4e8", size=12)),
                    xaxis=dict(gridcolor="#1e2a42", color="#7a8fb5"),
                    yaxis=dict(gridcolor="#1e2a42", color="#7a8fb5"),
                    margin=dict(l=10, r=40, t=36, b=10), height=220, showlegend=False,
                )
                st.plotly_chart(_fig_vix, use_container_width=True)
            else:
                st.caption("VIX history unavailable.")

        # — NFCI 2-year —
        with _liq_c2:
            if not _nfci_df.empty:
                _fig_nfci = go.Figure()
                _fig_nfci.add_trace(go.Scatter(
                    x=_nfci_df["date"], y=_nfci_df["nfci"],
                    mode="lines", line=dict(color="#00c8a0", width=1.8),
                    name="NFCI",
                ))
                _fig_nfci.add_hline(y=0, line_dash="dot", line_color="#7a8fb5",
                                    annotation_text="0 (neutral)", annotation_font_color="#7a8fb5",
                                    annotation_position="right")
                _fig_nfci.add_hline(y=-0.3, line_dash="dot", line_color="#00c8a0",
                                    annotation_text="-0.3 (loose)", annotation_font_color="#00c8a0",
                                    annotation_position="right")
                _fig_nfci.update_layout(
                    template="plotly_dark", paper_bgcolor="#0a0e1a", plot_bgcolor="#0f1525",
                    title=dict(text="NFCI — Last 2 Years", font=dict(color="#c8d4e8", size=12)),
                    xaxis=dict(gridcolor="#1e2a42", color="#7a8fb5"),
                    yaxis=dict(gridcolor="#1e2a42", color="#7a8fb5"),
                    margin=dict(l=10, r=50, t=36, b=10), height=220, showlegend=False,
                )
                st.plotly_chart(_fig_nfci, use_container_width=True)
            else:
                st.caption("NFCI history unavailable.")

        _liq_c3, _liq_c4 = st.columns(2)

        # — Yield Spread 12-month —
        with _liq_c3:
            if not _spread_df.empty:
                _spread_color = [
                    "#ff4d6d" if v < 0 else "#00c8a0" for v in _spread_df["spread"]
                ]
                _fig_spread = go.Figure()
                _fig_spread.add_trace(go.Scatter(
                    x=_spread_df["date"], y=_spread_df["spread"],
                    mode="lines", line=dict(color="#7a8fb5", width=1.8),
                    fill="tozeroy", fillcolor="rgba(122,143,181,0.07)",
                    name="Spread",
                ))
                _fig_spread.add_hline(y=0, line_dash="dot", line_color="#ff4d6d",
                                      annotation_text="0 (inversion)", annotation_font_color="#ff4d6d",
                                      annotation_position="right")
                _fig_spread.update_layout(
                    template="plotly_dark", paper_bgcolor="#0a0e1a", plot_bgcolor="#0f1525",
                    title=dict(text="10Y − Short Yield Spread — Last 12 Mo", font=dict(color="#c8d4e8", size=12)),
                    xaxis=dict(gridcolor="#1e2a42", color="#7a8fb5"),
                    yaxis=dict(gridcolor="#1e2a42", color="#7a8fb5", title="%"),
                    margin=dict(l=10, r=60, t=36, b=10), height=220, showlegend=False,
                )
                st.plotly_chart(_fig_spread, use_container_width=True)
            else:
                st.caption("Yield spread history unavailable.")

        # — Margin Debt monthly bar —
        with _liq_c4:
            if _mg_history:
                _mg_hist_df = pd.DataFrame(_mg_history)
                _mg_hist_df = _mg_hist_df.dropna(subset=["debt_B"])
                # Color bars by MoM direction
                _mg_colors = ["#c8d4e8"]
                for i in range(1, len(_mg_hist_df)):
                    prev = _mg_hist_df["debt_B"].iloc[i - 1]
                    curr = _mg_hist_df["debt_B"].iloc[i]
                    _mg_colors.append("#ff4d6d" if curr > prev else "#00c8a0")
                _fig_mg = go.Figure(go.Bar(
                    x=_mg_hist_df["period"],
                    y=_mg_hist_df["debt_B"],
                    marker_color=_mg_colors,
                    marker_opacity=0.85,
                    hovertemplate="<b>%{x}</b><br>$%{y}B<extra></extra>",
                ))
                _fig_mg.update_layout(
                    template="plotly_dark", paper_bgcolor="#0a0e1a", plot_bgcolor="#0f1525",
                    title=dict(text="NYSE Margin Debt — Monthly ($B)", font=dict(color="#c8d4e8", size=12)),
                    xaxis=dict(gridcolor="#1e2a42", color="#7a8fb5", tickangle=-45),
                    yaxis=dict(gridcolor="#1e2a42", color="#7a8fb5", title="$B"),
                    margin=dict(l=10, r=10, t=36, b=10), height=220, showlegend=False,
                )
                st.plotly_chart(_fig_mg, use_container_width=True)
            else:
                st.caption("Margin debt history unavailable.")

# ─────────────────────────────────────────────────────
# TAB 3 — Daily Market Brief
# ─────────────────────────────────────────────────────
with tab3:
    st.markdown(f"<div class='section-header'>{datetime.now().strftime('%B %d, %Y')} — Market Brief</div>", unsafe_allow_html=True)
    brief = get_daily_brief()
    st.markdown("<div class='section-header' style='margin-top:8px'>Top Headlines</div>", unsafe_allow_html=True)
    for item in brief["news"]:
        st.markdown(
            f"<div style='padding:8px 0;border-bottom:1px solid #1e2a42'>"
            f"<a href='{item['link']}' target='_blank' style='color:#00c8a0;font-weight:600;text-decoration:none'>{item['title']}</a>"
            f"<span style='color:#7a8fb5;font-size:0.75rem;margin-left:8px'>{item['published']}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div class='section-header'>Earnings Calendar</div>", unsafe_allow_html=True)
    st.dataframe(brief["earnings"], use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────────────
# TAB 4 — Trade History
# ─────────────────────────────────────────────────────
with tab4:
    st.markdown("<div class='section-header'>Trade History</div>", unsafe_allow_html=True)
    if not ibkr_available:
        st.markdown(
            "<div style='background:#141a2e;border:1px solid #1e2a42;border-radius:10px;"
            "padding:24px;text-align:center;color:#7a8fb5'>"
            "Trade history requires IBKR credentials. Configure your <code>.env</code> and click "
            "<strong style='color:#00c8a0'>↺ Refresh</strong>.</div>",
            unsafe_allow_html=True,
        )
    else:
        th_display = _trade_history_display(trades)
        sym_opts = ["All"] + sorted(th_display["Symbol"].dropna().unique().tolist())
        symbol_filter = st.selectbox("Filter by Symbol", sym_opts, label_visibility="collapsed")
        if symbol_filter == "All":
            st.dataframe(th_display, use_container_width=True, hide_index=True)
        else:
            st.dataframe(
                th_display[th_display["Symbol"] == symbol_filter].reset_index(drop=True),
                use_container_width=True,
                hide_index=True,
            )


# ─────────────────────────────────────────────────────
# TAB 5 — Stock Analysis
# ─────────────────────────────────────────────────────
with tab5:
    # Symbol input — free text so any ticker can be analyzed, not just held ones
    portfolio_syms = sorted(portfolio.keys()) if portfolio else []
    sym_col, btn_col = st.columns([3, 1])
    with sym_col:
        _default_sym = st.session_state.get("_sa_sym", portfolio_syms[0] if portfolio_syms else "AAPL")
        selected_symbol = st.text_input(
            "Symbol",
            value=_default_sym,
            placeholder="Enter any ticker, e.g. NVDA, TSLA, SPY …",
            label_visibility="collapsed",
        ).strip().upper()
        if portfolio_syms:
            _quick = st.selectbox(
                "Quick-pick from your portfolio",
                options=["— pick —"] + portfolio_syms,
                index=0,
            )
            if _quick != "— pick —":
                selected_symbol = _quick
    with btn_col:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        run_analysis = st.button("Analyze", type="primary", use_container_width=True)

    if selected_symbol and (run_analysis or ("_sa_cache" in st.session_state and st.session_state.get("_sa_sym") == selected_symbol)):
        if run_analysis or "_sa_cache" not in st.session_state:
            with st.spinner(f"Fetching data for {selected_symbol}..."):
                analysis = get_stock_analysis(selected_symbol)
                st.session_state["_sa_cache"] = analysis
                st.session_state["_sa_sym"] = selected_symbol
        else:
            analysis = st.session_state["_sa_cache"]

        if analysis.get("error"):
            st.error(analysis["error"])
        else:
            info_row = analysis
            val = analysis["valuation"]
            fund = analysis["fundamentals"]
            tech = analysis["technicals"]
            analyst = analysis["analyst"]
            inst = analysis["institutional"]
            earn = analysis["earnings"]

            # ── Header row ──
            mc = info_row.get("market_cap")
            mc_str = f"${mc/1e9:.1f}B" if mc and mc >= 1e9 else (f"${mc/1e6:.0f}M" if mc else "—")
            st.markdown(
                f"<div style='margin-bottom:16px'>"
                f"<span style='font-size:1.3rem;font-weight:700;color:#e8ecf0'>{selected_symbol}</span>"
                f"<span style='color:#7a8fb5;margin-left:10px;font-size:0.9rem'>{info_row.get('name','')}</span>"
                f"<span class='stat-chip' style='margin-left:12px'>{info_row.get('sector','—')}</span>"
                f"<span class='stat-chip'>{info_row.get('industry','—')}</span>"
                f"<span class='stat-chip'>Mkt Cap {mc_str}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

            # ── Row 1: Valuation + Fundamentals metrics ──
            st.markdown("<div class='section-header'>Valuation Multiples & Key Fundamentals</div>", unsafe_allow_html=True)
            cols = st.columns(6)
            metrics_row1 = [
                ("Fwd P/E",   val.get("Forward P/E")),
                ("EV/EBITDA", val.get("EV/EBITDA")),
                ("P/S",       val.get("P/S")),
                ("P/FCF",     val.get("P/FCF")),
                ("PEG",       val.get("PEG")),
                ("P/B",       val.get("P/B")),
            ]
            for col, (label, v) in zip(cols, metrics_row1):
                disp = _fmt(v, ".1f") if v is not None else "—"
                col.metric(label, disp)

            cols2 = st.columns(6)
            metrics_row2 = [
                ("Gross Margin",    fund.get("Gross Margin"),        "%"),
                ("Op Margin",       fund.get("Operating Margin"),    "%"),
                ("Net Margin",      fund.get("Net Margin"),          "%"),
                ("ROE",             fund.get("ROE"),                 "%"),
                ("Rev Growth YoY",  fund.get("Revenue Growth YoY"), "%"),
                ("Debt/Equity",     fund.get("Debt/Equity"),        "x"),
            ]
            for col, (label, v, sfx) in zip(cols2, metrics_row2):
                disp = (f"{v:+.1f}{sfx}" if sfx == "%" else f"{v:.2f}{sfx}") if v is not None else "—"
                col.metric(label, disp)

            st.markdown("<br>", unsafe_allow_html=True)

            # ── Row 2: 3-column layout ──
            col_analyst, col_tech, col_earn = st.columns([1, 1, 1])

            # ── Analyst Consensus ──
            with col_analyst:
                st.markdown("<div class='section-header'>Analyst Consensus</div>", unsafe_allow_html=True)
                rating = analyst.get("overall_rating", "—")
                rating_class = {
                    "Strong Buy": "rating-strong-buy",
                    "Buy":        "rating-buy",
                    "Hold":       "rating-hold",
                    "Sell":       "rating-sell",
                }.get(rating, "rating-na")
                st.markdown(f"<span class='rating-badge {rating_class}'>{rating}</span>", unsafe_allow_html=True)
                st.markdown("<br>", unsafe_allow_html=True)

                mean_t = analyst.get("mean_target")
                high_t = analyst.get("high_target")
                low_t  = analyst.get("low_target")
                upside = analyst.get("upside_pct")
                curr   = analyst.get("current_price") or tech.get("price")

                if curr:
                    st.metric("Current Price", f"${curr:,.2f}")
                if mean_t:
                    upside_str = f"{upside:+.1f}% upside" if upside is not None else ""
                    st.metric("Mean Target", f"${float(mean_t):,.2f}", delta=upside_str)
                if high_t and low_t:
                    st.markdown(
                        f"<div style='color:#7a8fb5;font-size:0.78rem;margin-top:4px'>"
                        f"Range: <span style='color:#00c8a0'>${float(high_t):,.2f}</span>"
                        f" / <span style='color:#ff4d6d'>${float(low_t):,.2f}</span></div>",
                        unsafe_allow_html=True,
                    )

                rc = analyst.get("rating_counts", {})
                if rc:
                    st.markdown("<br>", unsafe_allow_html=True)
                    total_rc = sum(rc.values())
                    for label, count in sorted(rc.items(), key=lambda x: -x[1]):
                        pct_rc = count / total_rc * 100 if total_rc > 0 else 0
                        bar_color = "#00c8a0" if "BUY" in label.upper() else ("#ff4d6d" if "SELL" in label.upper() else "#f4c542")
                        st.markdown(
                            f"<div style='margin-bottom:6px'>"
                            f"<div style='display:flex;justify-content:space-between;font-size:0.75rem;color:#c8d4e8;margin-bottom:2px'>"
                            f"<span>{label}</span><span>{count}</span></div>"
                            f"<div style='background:#1e2a42;border-radius:3px;height:5px'>"
                            f"<div style='background:{bar_color};width:{pct_rc:.0f}%;height:5px;border-radius:3px'></div>"
                            f"</div></div>",
                            unsafe_allow_html=True,
                        )

            # ── Technicals ──
            with col_tech:
                st.markdown("<div class='section-header'>Technical Indicators</div>", unsafe_allow_html=True)

                rsi = tech.get("rsi")
                if rsi is not None:
                    rsi_color = "#ff4d6d" if rsi > 70 else ("#00c8a0" if rsi < 30 else "#f4c542")
                    rsi_label = "Overbought" if rsi > 70 else ("Oversold" if rsi < 30 else "Neutral")
                    st.markdown(
                        f"<div style='margin-bottom:10px'>"
                        f"<span style='color:#7a8fb5;font-size:0.75rem'>RSI (14)</span><br>"
                        f"<span style='font-size:1.6rem;font-weight:700;color:{rsi_color}'>{rsi}</span>"
                        f"<span style='color:{rsi_color};font-size:0.75rem;margin-left:6px'>{rsi_label}</span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

                macd = tech.get("macd_signal", "—")
                macd_color = "#00c8a0" if macd == "Bullish" else ("#ff4d6d" if macd == "Bearish" else "#7a8fb5")
                bb = tech.get("bb_position")
                rel_vol = tech.get("rel_volume")
                w52_pct = tech.get("week52_pct")

                chip_items = [
                    ("MACD",       macd,                                                  macd_color),
                    ("BB Position", f"{bb:.0f}%" if bb is not None else "—",             "#c8d4e8"),
                    ("Rel Volume",  f"{rel_vol:.2f}x" if rel_vol is not None else "—",   "#00c8a0" if rel_vol and rel_vol > 1.5 else "#c8d4e8"),
                    ("52W Range",   f"{w52_pct:.0f}%" if w52_pct is not None else "—",   "#c8d4e8"),
                ]
                for lbl, val_s, color in chip_items:
                    st.markdown(
                        f"<div style='display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #1e2a42'>"
                        f"<span style='color:#7a8fb5;font-size:0.78rem'>{lbl}</span>"
                        f"<span style='color:{color};font-size:0.78rem;font-weight:600'>{val_s}</span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

                price_now = tech.get("price")
                if price_now:
                    st.markdown("<div style='margin-top:10px;color:#7a8fb5;font-size:0.72rem;letter-spacing:0.05em'>PRICE vs MOVING AVERAGES</div>", unsafe_allow_html=True)
                    for ma_lbl, ma_key, diff_key in [("MA 20", "ma20", "price_vs_ma20"), ("MA 50", "ma50", "price_vs_ma50"), ("MA 200", "ma200", "price_vs_ma200")]:
                        ma_val = tech.get(ma_key)
                        diff = tech.get(diff_key)
                        if ma_val is not None and diff is not None:
                            c = "#00c8a0" if diff >= 0 else "#ff4d6d"
                            sign = "+" if diff >= 0 else ""
                            st.markdown(
                                f"<div style='display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #1e2a42'>"
                                f"<span style='color:#7a8fb5;font-size:0.75rem'>{ma_lbl}  <span style='color:#4a5a78'>${ma_val:,.2f}</span></span>"
                                f"<span style='color:{c};font-size:0.75rem;font-weight:600'>{sign}{diff:.1f}%</span>"
                                f"</div>",
                                unsafe_allow_html=True,
                            )

            # ── Earnings Momentum ──
            with col_earn:
                st.markdown("<div class='section-header'>Earnings Momentum</div>", unsafe_allow_html=True)

                next_e = earn.get("next_earnings", "—")
                eps_trend = earn.get("eps_trend", "—")
                trend_color = "#00c8a0" if eps_trend == "Improving" else ("#ff4d6d" if eps_trend == "Declining" else "#f4c542")

                st.markdown(
                    f"<div style='margin-bottom:12px'>"
                    f"<div style='display:flex;justify-content:space-between;margin-bottom:6px'>"
                    f"<span style='color:#7a8fb5;font-size:0.75rem'>Next Earnings</span>"
                    f"<span style='color:#c8d4e8;font-size:0.75rem;font-weight:600'>{next_e}</span>"
                    f"</div>"
                    f"<div style='display:flex;justify-content:space-between'>"
                    f"<span style='color:#7a8fb5;font-size:0.75rem'>EPS Trend</span>"
                    f"<span style='color:{trend_color};font-size:0.75rem;font-weight:600'>{eps_trend}</span>"
                    f"</div></div>",
                    unsafe_allow_html=True,
                )

                eps_hist = earn.get("eps_history", pd.DataFrame())
                if not eps_hist.empty:
                    st.markdown("<div style='color:#7a8fb5;font-size:0.72rem;letter-spacing:0.05em;margin-bottom:6px'>EPS HISTORY (LAST 4Q)</div>", unsafe_allow_html=True)
                    rename_map = {
                        "quarter": "Quarter", "epsActual": "Actual",
                        "epsEstimate": "Estimate", "surprisePercent": "Surprise %",
                    }
                    eps_show = eps_hist.rename(columns={k: v for k, v in rename_map.items() if k in eps_hist.columns})

                    def _color_surprise(val):
                        try:
                            f = float(val)
                            if f > 0:
                                return "background-color:#003d2e;color:#00c8a0"
                            if f < 0:
                                return "background-color:#3d0015;color:#ff4d6d"
                        except Exception:
                            pass
                        return ""

                    fmt_eps = {}
                    for c in eps_show.columns:
                        if c in ("Actual", "Estimate"):
                            fmt_eps[c] = lambda x: _fmt(x, ".2f")
                        elif c == "Surprise %":
                            fmt_eps[c] = lambda x: _fmt(x, ".1f", "%")
                    sty_eps = eps_show.style.format(fmt_eps, na_rep="—")
                    if "Surprise %" in eps_show.columns:
                        sty_eps = sty_eps.applymap(_color_surprise, subset=["Surprise %"])
                    st.dataframe(sty_eps, use_container_width=True, hide_index=True, height=180)

            # ── Row 3: Institutional ──
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown("<div class='section-header'>Institutional Ownership</div>", unsafe_allow_html=True)
            inst_col, insider_col = st.columns([3, 1])

            with inst_col:
                top_h = inst.get("top_holders", pd.DataFrame())
                if not top_h.empty:
                    pct_col = next((c for c in top_h.columns if "%" in c or "out" in c.lower()), None)
                    fmt_inst = {}
                    if pct_col:
                        fmt_inst[pct_col] = lambda x: _fmt(x, ".2f", "%")
                    if "Value" in top_h.columns:
                        fmt_inst["Value"] = lambda x: _fmt(x, ",.0f")
                    st.dataframe(top_h.style.format(fmt_inst, na_rep="—"), use_container_width=True, hide_index=True)
                else:
                    st.caption("No institutional data available.")

            with insider_col:
                insider_dir = inst.get("insider_direction", "—")
                dir_color = "#00c8a0" if insider_dir == "Net Buying" else ("#ff4d6d" if insider_dir == "Net Selling" else "#f4c542")
                dir_icon = "↑" if insider_dir == "Net Buying" else ("↓" if insider_dir == "Net Selling" else "↔")
                st.markdown(
                    f"<div style='background:#141a2e;border:1px solid #1e2a42;border-radius:8px;padding:16px;text-align:center'>"
                    f"<div style='color:#7a8fb5;font-size:0.72rem;letter-spacing:0.08em;margin-bottom:8px'>INSIDER ACTIVITY</div>"
                    f"<div style='font-size:2rem;color:{dir_color}'>{dir_icon}</div>"
                    f"<div style='color:{dir_color};font-weight:700;font-size:0.85rem;margin-top:4px'>{insider_dir}</div>"
                    f"<div style='color:#4a5a78;font-size:0.7rem;margin-top:4px'>Last 90 days</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            # ── Short Interest ──
            _si = analysis.get("short_interest", {})
            _si_pct = _si.get("short_pct_float")
            _si_ratio = _si.get("short_ratio")
            _si_shares = _si.get("shares_short")
            _si_date = _si.get("date", "—")
            if any(v is not None for v in [_si_pct, _si_ratio, _si_shares]):
                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown("<div class='section-header'>Short Interest</div>", unsafe_allow_html=True)
                si_c1, si_c2, si_c3, si_c4 = st.columns(4)
                _si_pct_color = "#ff4d6d" if (_si_pct or 0) > 15 else ("#f4c542" if (_si_pct or 0) > 8 else "#00c8a0")
                with si_c1:
                    st.markdown(
                        f"<div style='background:#141a2e;border:1px solid #1e2a42;border-radius:8px;padding:14px 18px 10px 18px'>"
                        f"<div style='color:#7a8fb5;font-size:0.78rem;letter-spacing:0.04em'>Short % of Float</div>"
                        f"<div style='color:{_si_pct_color};font-size:1.5rem;font-weight:700;margin-top:4px'>"
                        f"{'—' if _si_pct is None else f'{_si_pct:.2f}%'}</div></div>",
                        unsafe_allow_html=True,
                    )
                si_c2.metric("Days to Cover", f"{'—' if _si_ratio is None else f'{_si_ratio:.1f}d'}")
                si_c3.metric(
                    "Shares Short",
                    f"{'—' if _si_shares is None else (f'{_si_shares/1e6:.1f}M' if _si_shares >= 1_000_000 else f'{_si_shares:,}')}"
                )
                si_c4.metric("As of", _si_date)
                st.markdown(
                    "<div style='color:#4a5a78;font-size:0.72rem;margin-top:4px'>"
                    "High short % (&gt;15%) may indicate bearish sentiment or squeeze potential. "
                    "Days to Cover = Shares Short / Avg Daily Volume."
                    "</div>",
                    unsafe_allow_html=True,
                )

            # ── Sentiment Indicators (RSI history) ──
            _rsi_hist = tech.get("rsi_history", pd.DataFrame())
            if not _rsi_hist.empty:
                st.markdown("<br>", unsafe_allow_html=True)
                with st.expander("Sentiment Indicators — RSI History", expanded=False):
                    _fig_rsi = go.Figure()
                    _rsi_vals = _rsi_hist["rsi"].values
                    _rsi_dates = _rsi_hist["date"]

                    # Dynamic line color: overbought red, oversold green, neutral yellow
                    _rsi_line_color = (
                        "#ff4d6d" if float(_rsi_vals[-1]) > 70
                        else "#00c8a0" if float(_rsi_vals[-1]) < 30
                        else "#f4c542"
                    )
                    _fig_rsi.add_trace(go.Scatter(
                        x=_rsi_dates, y=_rsi_vals,
                        mode="lines",
                        line=dict(color=_rsi_line_color, width=2),
                        name="RSI(14)",
                        hovertemplate="<b>%{x|%Y-%m-%d}</b><br>RSI: %{y:.1f}<extra></extra>",
                    ))
                    # Reference bands
                    _fig_rsi.add_hrect(y0=70, y1=100, fillcolor="rgba(255,77,109,0.07)",
                                       line_width=0, annotation_text="Overbought",
                                       annotation_position="top left",
                                       annotation_font=dict(color="#ff4d6d", size=10))
                    _fig_rsi.add_hrect(y0=0, y1=30, fillcolor="rgba(0,200,160,0.07)",
                                       line_width=0, annotation_text="Oversold",
                                       annotation_position="bottom left",
                                       annotation_font=dict(color="#00c8a0", size=10))
                    _fig_rsi.add_hline(y=70, line_dash="dot", line_color="#ff4d6d",
                                       line_width=1, opacity=0.6)
                    _fig_rsi.add_hline(y=50, line_dash="dot", line_color="#7a8fb5",
                                       line_width=1, opacity=0.4)
                    _fig_rsi.add_hline(y=30, line_dash="dot", line_color="#00c8a0",
                                       line_width=1, opacity=0.6)
                    _fig_rsi.update_layout(
                        template="plotly_dark",
                        paper_bgcolor="#0a0e1a",
                        plot_bgcolor="#0f1525",
                        title=dict(
                            text=f"RSI(14) — {selected_symbol} — Last 6 Months",
                            font=dict(color="#c8d4e8", size=13),
                        ),
                        xaxis=dict(gridcolor="#1e2a42", color="#7a8fb5"),
                        yaxis=dict(gridcolor="#1e2a42", color="#7a8fb5",
                                   range=[0, 100], title="RSI"),
                        margin=dict(l=10, r=10, t=40, b=10),
                        height=260,
                        showlegend=False,
                    )
                    st.plotly_chart(_fig_rsi, use_container_width=True)
                    _cur_rsi = tech.get("rsi")
                    if _cur_rsi is not None:
                        _rsi_zone = "Overbought" if _cur_rsi > 70 else ("Oversold" if _cur_rsi < 30 else "Neutral")
                        _zone_color = "#ff4d6d" if _cur_rsi > 70 else ("#00c8a0" if _cur_rsi < 30 else "#f4c542")
                        st.markdown(
                            f"<div style='color:#4a5a78;font-size:0.72rem;margin-top:4px'>"
                            f"Current RSI: <span style='color:{_zone_color};font-weight:600'>{_cur_rsi} — {_rsi_zone}</span>. "
                            f"RSI &gt; 70 = overbought territory; RSI &lt; 30 = oversold territory."
                            f"</div>",
                            unsafe_allow_html=True,
                        )

            # ── Row 4: Price chart ──
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown("<div class='section-header'>Price & Volume (6 Months)</div>", unsafe_allow_html=True)
            try:
                import yfinance as yf
                hist_chart = yf.Ticker(selected_symbol).history(period="6mo")
                if hist_chart is not None and not hist_chart.empty:
                    fig_price = go.Figure()
                    fig_price.add_trace(go.Candlestick(
                        x=hist_chart.index,
                        open=hist_chart["Open"],
                        high=hist_chart["High"],
                        low=hist_chart["Low"],
                        close=hist_chart["Close"],
                        increasing_line_color="#00c8a0",
                        decreasing_line_color="#ff4d6d",
                        name="Price",
                    ))
                    for n, color, name in [(20, "#f4c542", "MA20"), (50, "#7a8fb5", "MA50")]:
                        if len(hist_chart) >= n:
                            fig_price.add_trace(go.Scatter(
                                x=hist_chart.index,
                                y=hist_chart["Close"].rolling(n).mean(),
                                line=dict(color=color, width=1.2, dash="dot"),
                                name=name,
                                opacity=0.8,
                            ))
                    fig_price.update_layout(
                        template="plotly_dark",
                        paper_bgcolor="#0a0e1a",
                        plot_bgcolor="#0f1525",
                        xaxis=dict(gridcolor="#1e2a42", color="#7a8fb5", rangeslider_visible=False),
                        yaxis=dict(gridcolor="#1e2a42", color="#7a8fb5"),
                        legend=dict(orientation="h", y=1.05, font=dict(size=10, color="#7a8fb5")),
                        margin=dict(l=10, r=10, t=10, b=10),
                        height=340,
                    )
                    st.plotly_chart(fig_price, use_container_width=True)
            except Exception:
                st.caption("Price chart unavailable.")

    else:
        st.markdown(
            "<div style='text-align:center;padding:60px 0;color:#7a8fb5'>"
            "<div style='font-size:2rem;margin-bottom:8px'>◈</div>"
            "<div>Enter a ticker above and click <strong style='color:#00c8a0'>Analyze</strong></div>"
            "</div>",
            unsafe_allow_html=True,
        )
