import math
import numpy as np
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import sys
from pathlib import Path

# Add src to Python path
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
from monitors.liquidity_monitor import get_fear_greed_data, get_liquidity_indicators
from monitors.daily_brief import get_daily_brief
from core.dcf_valuation import calculate_all_dcf


def _holding_pnl_and_pct(price: float, info: dict) -> tuple[float, float]:
    """
    Return (pnl_usd, pnl_pct) for a symbol.

    Open  – unrealized: (current_price − diluted_avg_cost) × shares
    Closed – realized_pnl already computed by process_trades (total_sell_proceeds − total_buy_cost)
    """
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
    """Only date, symbol, and signed trade cash (prefer IBKR proceeds)."""
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
    out = pd.DataFrame(
        {
            "Date": t["date"].dt.strftime("%Y-%m-%d"),
            "Symbol": t["symbol"].astype(str),
            "Amount": np.round(pd.to_numeric(amt, errors="coerce"), 2),
        }
    )
    return out.sort_values("Date", ascending=False, na_position="last").reset_index(drop=True)


def get_single_stock_indicators(symbol: str):
    """Calculate technical and funding flow indicators for a single stock"""
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="60d")
        if hist is None or hist.empty or len(hist) < 15:
            raise ValueError("insufficient history")

        delta = hist["Close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi_series = 100 - (100 / (1 + rs))
        rsi_raw = rsi_series.iloc[-1]
        rsi = float(rsi_raw) if pd.notna(rsi_raw) and math.isfinite(float(rsi_raw)) else 50.0

        avg_vol = hist["Volume"].rolling(20).mean().iloc[-1]
        today_vol = hist["Volume"].iloc[-1]
        av = float(avg_vol) if pd.notna(avg_vol) else 0.0
        tv = float(today_vol) if pd.notna(today_vol) else 0.0
        rel_vol = tv / av if av > 0 else 0.0

        pc_ratio = 0.85
        try:
            opts = getattr(ticker, "options", None)
            if opts:
                opt = ticker.option_chain(opts[0])
                put_oi = opt.puts["openInterest"].sum()
                call_oi = opt.calls["openInterest"].sum()
                pc_ratio = put_oi / call_oi if call_oi > 0 else 1.0
        except Exception:
            pass

        return {
            "RSI": round(rsi, 1),
            "Relative_Volume": round(rel_vol, 2),
            "Put_Call_Ratio": round(pc_ratio, 2),
            "Signal": "Bullish"
            if rsi < 40 and rel_vol > 1.2
            else "Bearish"
            if rsi > 70
            else "Neutral",
        }
    except Exception:
        return {
            "RSI": 50.0,
            "Relative_Volume": 1.0,
            "Put_Call_Ratio": 0.85,
            "Signal": "Neutral",
        }


# ====================== Page Configuration ======================
st.set_page_config(page_title="IBKR Decision Hub", page_icon="🧭", layout="wide")
st.title("🧭 IBKR Local Investment Decision Hub")
st.divider()

# ====================== Sidebar ======================
with st.sidebar:
    st.header("⚙️ Control Panel")

    st.subheader("📅 Trade History Range")
    from datetime import date as _date
    _sidebar_start = st.session_state.get("_sidebar_start_date", _date(2024, 1, 1))
    _picked_start = st.date_input(
        "Fetch history from",
        value=_sidebar_start,
        min_value=_date(2015, 1, 1),
        max_value=datetime.now().date(),
        help=(
            "When you click Refresh, trade history is fully re-fetched starting from "
            "this date. Does not affect incremental auto-sync on normal app loads."
        ),
        key="sidebar_date_picker",
    )

    if st.button("🔄 Refresh All Data from IBKR", type="primary", use_container_width=True):
        _start_str = _picked_start.strftime("%Y%m%d")
        # Persist the chosen date and the override before clearing other state
        st.session_state["_ibkr_flex_full_refresh"] = True
        st.session_state["_trade_start_date_override"] = _start_str
        st.session_state["_sidebar_start_date"] = _picked_start
        _keep = {"_ibkr_flex_full_refresh", "_trade_start_date_override", "_sidebar_start_date"}
        for key in list(st.session_state.keys()):
            if key not in _keep:
                del st.session_state[key]
        st.rerun()

    st.divider()
    st.subheader("📊 Display Settings")
    view_mode = st.radio("Portfolio View",
                         ["Current Holdings", "All Historical Holdings (by Performance)"],
                         horizontal=True, index=0)

# ====================== Load Data ======================
_flex_full_refresh = st.session_state.pop("_ibkr_flex_full_refresh", False)
_trade_start_override = st.session_state.pop("_trade_start_date_override", None)
if "trades" not in st.session_state:
    st.session_state.trades = fetch_ibkr_trades(
        force_full=_flex_full_refresh,
        start_date_override=_trade_start_override,
    )
if "account_data" not in st.session_state:
    st.session_state.account_data = fetch_ibkr_positions_and_cash(flex_refresh=_flex_full_refresh)

trades = st.session_state.trades
portfolio = process_trades(trades)
account = st.session_state.account_data
cash_balance = account["cash_balance"]

# ====================== Stale Data Warning ======================
# IBKR Flex fd/td parameters can only NARROW a query's date range, not extend it.
# If the template's "To Date" is in the past, the API will silently cap all
# responses at that date no matter what td we send.  Surface this clearly.
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
            f"(latest trade in cache: **{pd.Timestamp(_trade_max_date).date()}**).  \n\n"
            "**Root cause**: your IBKR Flex Trade query has a fixed 'To Date' in the past. "
            "IBKR's API parameters (`fd`/`td`) can only *narrow* a query's date range — "
            "they cannot extend beyond the template's configured end date.  \n\n"
            "**How to fix (takes ~2 minutes):**  \n"
            "1. Log in to IBKR Client Portal → **Reports** → **Flex Queries**  \n"
            "2. Click **Edit** on your **Trades** Flex query  \n"
            "3. Under **Reporting Period**, change the period to **'Last 3 Years'** "
            "(or set a custom date range with 'To Date' = **20991231**)  \n"
            "4. Save the query  \n"
            "5. Come back here and click **Refresh All Data from IBKR** in the sidebar"
        )

# ====================== Tabs ======================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Portfolio Analysis", 
    "🌊 Liquidity Monitor", 
    "📅 Daily Market Brief", 
    "📜 Trade History",
    "📈 DCF + Stock Analysis"
])

def _color_pnl(val):
    """Green / red cell background for P&L columns."""
    if isinstance(val, (int, float)) and not isinstance(val, bool) and pd.notna(val):
        if val > 0:
            return "background-color: #90EE90; color: black"
        if val < 0:
            return "background-color: #FF9999; color: black"
    return ""


def _fmt(x, fmt: str, suffix: str = "") -> str:
    if isinstance(x, (int, float)) and not isinstance(x, bool) and pd.notna(x) and not math.isnan(x):
        return format(x, fmt) + suffix
    return "—"


with tab1:
    st.subheader("Portfolio Analysis")

    if view_mode == "Current Holdings":
        # --- Current Holdings: one row per symbol from Flex Open Positions ---
        wmsg = account.get("positions_flex_warning")
        if wmsg:
            st.warning(wmsg)
        pflex = account.get("positions_flex_df")
        if pflex is not None and not pflex.empty:
            disp = enrich_positions_with_trade_cost(pflex, portfolio)
            # Sort by Market Value descending (largest positions first)
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
        m1, m2, m3 = st.columns(3)
        m1.metric("Positions (Flex)", f"${pv:,.0f}")
        m2.metric("Cash", f"${cash_balance:,.2f}")
        m3.metric("Net", f"${nb:,.0f}")

    else:
        # --- Historical Holdings ---
        # Open positions: same enriched Flex table as Current Holdings (ground truth from IBKR).
        # Closed positions: symbols in trade history where current_shares ≈ 0.
        pflex_hist = account.get("positions_flex_df")

        # ---- Open positions section ----
        st.markdown("### Open Positions")
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
            st.markdown(f"**Net** — Positions: ${pv_h:,.0f} | Cash: ${cash_balance:,.0f} | Total: ${nb_h:,.0f}")
        else:
            wmsg_h = account.get("positions_flex_warning")
            if wmsg_h:
                st.warning(wmsg_h)
            else:
                st.info("No open positions data available. Refresh from IBKR.")

        # ---- Closed positions section ----
        # Only show symbols where the trade ledger reaches exactly zero shares.
        # Symbols with a non-zero ledger balance that are absent from the live snapshot
        # have incomplete trade history (initial buys or recent sells outside the fetch
        # window) and are silently excluded — see README § "Known Data Limitations".
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
                    "Realized PnL ($)": round(realized, 2),
                    "Close Date": close_date,
                })

        if closed_rows:
            st.markdown("### Closed Positions")
            closed_df = (
                pd.DataFrame(closed_rows)
                .sort_values("Realized PnL ($)", ascending=False)
                .reset_index(drop=True)
            )
            closed_sty = (
                closed_df.style
                .format({"Realized PnL ($)": lambda x: _fmt(x, ",.2f")}, na_rep="—")
                .applymap(_color_pnl, subset=["Realized PnL ($)"])
            )
            st.dataframe(closed_sty, use_container_width=True, hide_index=True)

with tab2:
    st.subheader("🌊 US Equity Liquidity Monitor")
    indicators = get_liquidity_indicators()
    fg_df = get_fear_greed_data()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Fed Net Liquidity", f"${indicators['Fed_Net_Liquidity_B']}B", delta=indicators['Net_Liquidity_Trend'])
    with col2:
        st.metric("Chicago Fed NFCI", f"{indicators['NFCI']}", delta=indicators['NFCI_Interp'])
    with col3:
        st.metric("10Y-2Y Yield Spread", f"{indicators['10Y_2Y_Spread']}%")
    with col4:
        st.metric("VIX", f"{indicators['VIX']}")
    if not fg_df.empty:
        current_fg = fg_df["value"].iloc[-1]
        st.metric("CNN Fear & Greed Index", f"{current_fg:.0f}/100")
        fig = px.line(fg_df.tail(30), x="timestamp", y="value", title="Fear & Greed Trend (Last 30 Days)")
        st.plotly_chart(fig, use_container_width=True)
    st.subheader("Credit & Stress Indicators")
    c1, c2 = st.columns(2)
    with c1:
        st.metric("High Yield OAS", f"{indicators['HY_OAS']}%")
    with c2:
        st.metric("Overall Assessment", indicators["Overall_Assessment"])

with tab3:
    st.subheader(f"📅 {datetime.now().strftime('%Y-%m-%d')} Daily Market Brief")
    brief = get_daily_brief()
    st.write("### Top Headlines")
    for item in brief["news"]:
        st.markdown(f"**[{item['title']}]({item['link']})** — {item['published']}")
    st.write("### Earnings Calendar")
    st.dataframe(brief["earnings"])

with tab4:
    st.subheader("📜 Full Trade History")
    th_display = _trade_history_display(trades)
    sym_opts = ["All"] + sorted(th_display["Symbol"].dropna().unique().tolist())
    symbol_filter = st.selectbox("Filter by Symbol", sym_opts)
    if symbol_filter == "All":
        st.dataframe(th_display, use_container_width=True, hide_index=True)
    else:
        st.dataframe(
            th_display[th_display["Symbol"] == symbol_filter].reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )

with tab5:
    st.subheader("📈 DCF Valuation + Stock Analysis")
    if portfolio:
        selected_symbol = st.selectbox("Select Symbol", options=list(portfolio.keys()))
        col_assume, col_result = st.columns([1, 2])
        with col_assume:
            st.write("**DCF Valuation Assumptions**")
            high_growth = st.slider("High Growth Rate (%)", 0, 30, 12) / 100.0
            high_years = st.slider("High Growth Period (years)", 3, 10, 5)
            terminal_g = st.slider("Terminal Growth Rate (%)", 0, 6, 3) / 100.0
            wacc = st.slider("WACC (%)", 6, 15, 10) / 100.0
            assumptions = {
                'high_growth': high_growth, 'high_years': high_years,
                'terminal_growth': terminal_g, 'wacc': wacc,
            }
            st.caption(
                "FCF = Operating Cash Flow − CapEx (avg of up to 3 years). "
                "Net debt subtracted to convert Enterprise Value → Equity Value."
            )
        with col_result:
            if st.button("🚀 Run Multi-Model DCF", type="primary", use_container_width=True):
                with st.spinner(f"Calculating for {selected_symbol}..."):
                    dcf_results = calculate_all_dcf(selected_symbol, assumptions)
                    if "error" in dcf_results:
                        st.error(dcf_results["error"])
                    else:
                        fv = dcf_results["Average Fair Value"]
                        cp = dcf_results["Current Price"]
                        upside = dcf_results.get("Upside (%)", 0.0)
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Two Stage DCF", f"${dcf_results['Two Stage DCF']:,.2f}")
                        m2.metric("Conservative", f"${dcf_results['Conservative Two Stage']:,.2f}")
                        m3.metric("Perpetual (Gordon)", f"${dcf_results['Perpetual (Gordon)']:,.2f}")
                        m4.metric(
                            "Avg Fair Value",
                            f"${fv:,.2f}",
                            delta=f"{upside:+.1f}% vs current ${cp}",
                            delta_color="normal",
                        )
                        if selected_symbol in portfolio:
                            diluted = float(portfolio[selected_symbol].get("avg_cost", 0) or 0)
                            if diluted > 0:
                                mos = fv - diluted
                                mos_pct = mos / diluted * 100
                                st.info(
                                    f"**Your Diluted Cost**: ${diluted:.2f}  |  "
                                    f"**DCF Margin of Safety**: ${mos:+.2f} ({mos_pct:+.1f}%)"
                                )
                        # Summary table (exclude verbose keys for cleanliness)
                        display_keys = [
                            "Two Stage DCF", "Conservative Two Stage", "Perpetual (Gordon)",
                            "Average Fair Value", "Current Price", "Upside (%)",
                            "Latest FCF", "Net Debt ($)", "Shares Outstanding",
                        ]
                        tbl = {k: dcf_results[k] for k in display_keys if k in dcf_results}
                        st.dataframe(pd.DataFrame([tbl]), use_container_width=True, hide_index=True)
        st.divider()
        st.subheader(f"🔍 {selected_symbol} Technical & Funding Flow Analysis")
        indicators = get_single_stock_indicators(selected_symbol)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("RSI (14)", f"{indicators['RSI']}")
        c2.metric("Relative Volume", f"{indicators['Relative_Volume']}x")
        c3.metric("Put/Call Ratio (OI)", f"{indicators['Put_Call_Ratio']}")
        c4.metric("Money Flow Signal", indicators["Signal"])
    else:
        st.info("No portfolio symbols yet — load trades from IBKR so symbols appear here.")

