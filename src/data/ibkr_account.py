"""
IBKR Flex: current positions + cash from an Activity Flex query that includes **Open Positions**.

Field reference (columns depend on what you select in the query):
https://www.ibkrguides.com/reportingreference/reportguide/open%20positionsfq.htm

Trades-only Flex CSV (no usable Open Positions shape) is rejected for the positions table; then either
add Open Positions to the same template or set IBKR_FLEX_POSITIONS_QUERY_ID to a positions-focused query.

Local cache: the raw Open Positions parser output is saved to ``cache/open_positions_snapshot.csv``.
Diluted average cost in the UI is merged from ``process_trades`` using ``cache/trade_history.csv``, not from the positions file.
"""

from __future__ import annotations

import os
import re
import tempfile
from typing import Any, Dict, Optional, Tuple

import pandas as pd

import numpy as np

import config
from data.flex_report import fetch_flex_report_dataframe, flex_send_request_url

# Local snapshot of the last Flex Open Positions table (same shape as returned by the Web Service parser).
POSITIONS_SNAPSHOT_FILE = os.path.join(config.CACHE_DIR, "open_positions_snapshot.csv")

# Appended by enrich_positions_with_trade_cost (diluted avg from trade ledger; unrealized estimate from mark).
COL_DILUTED_AVG_COST_TRADES = "Diluted Avg Cost"
COL_UNREALIZED_PNL_EST = "PnL"
COL_UNREALIZED_PNL_PCT = "PnL %"

# Ordered preferred display columns for Current Holdings (Flex name → display name).
# Only columns that exist in the CSV will appear; unknown Flex columns are dropped.
_POSITION_DISPLAY_COLS: list[tuple[str, str]] = [
    ("Symbol", "Symbol"),
    ("Description", "Description"),
    ("Quantity", "Qty"),
    ("Mark Price", "Mark Price"),
    ("MarkPrice", "Mark Price"),
    ("Close Price", "Mark Price"),
    ("ClosePrice", "Mark Price"),
    ("Position Value", "Market Value"),
    ("PositionValue", "Market Value"),
    ("Open Price", "Avg Open Price"),
    ("OpenPrice", "Avg Open Price"),
    ("Cost Basis Price", "Cost Basis Price"),
    ("CostBasisPrice", "Cost Basis Price"),
    ("Cost Basis Money", "Cost Basis ($)"),
    ("CostBasisMoney", "Cost Basis ($)"),
    ("FIFO Unrealized PNL", "IBKR Unrealized PnL"),
    ("FIFOUnrealizedPNL", "IBKR Unrealized PnL"),
    ("Percent of NAV", "% of NAV"),
    ("PercentOfNAV", "% of NAV"),
    ("Currency", "Currency"),
]


def _hdr_norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lstrip("\ufeff").lower())


def _find_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    norms = {_hdr_norm(c): c for c in df.columns}
    for cand in candidates:
        key = _hdr_norm(cand)
        if key in norms:
            return norms[key]
    return None


def _is_likely_trades_executions_flex(df: pd.DataFrame) -> bool:
    """Trades / executions CSV typically has trade date + buy/sell (or trade id)."""
    cols = {_hdr_norm(c) for c in df.columns}
    has_dt = bool({"tradedate", "datetime", "trade date"} & cols)
    has_side = bool({"buysell", "tradeid", "trade id"} & cols)
    return has_dt and has_side


def _is_likely_open_positions_flex(df: pd.DataFrame) -> bool:
    cols = {_hdr_norm(c) for c in df.columns}
    pos_markers = {
        "markprice",
        "positionvalue",
        "fifo unrealized pnl",
        "fifounrealizedpnl",
        "costbasismoney",
        "costbasisprice",
        "percentofnav",
        "percent of nav",
    }
    if cols & pos_markers:
        return True
    # Minimal Open Positions query: Symbol + Quantity/Position, without execution columns
    has_sym = "symbol" in cols
    has_qty = bool({"quantity", "position", "qty"} & cols)
    return has_sym and has_qty and not _is_likely_trades_executions_flex(df)


def _positions_only_table(df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Return rows that are open positions (exclude CASH), and optional warning if
    the CSV looks like Trades instead of Open Positions.
    """
    if df is None or df.empty:
        return pd.DataFrame(), None

    if _is_likely_trades_executions_flex(df) and not _is_likely_open_positions_flex(df):
        return (
            pd.DataFrame(),
            "Flex columns look like **Trades / executions**, not **Open Positions**. In your Activity Flex template, "
            "include **Open Positions** (and the columns you need), or create a positions-only query; if you use a "
            "second query, set **IBKR_FLEX_POSITIONS_QUERY_ID** in `.env`.",
        )

    sym_col = _find_col(df, "Symbol")
    if not sym_col:
        return pd.DataFrame(), "No **Symbol** column in the Flex table; cannot split positions."

    # When Flex exports per-lot detail, the snapshot contains both SUMMARY rows (aggregate per
    # symbol) and LOT rows (one per tax lot).  Summing both produces doubled quantities.
    # Keep only SUMMARY rows; fall back to all rows if no SUMMARY exists (e.g., simpler queries).
    lod_col = _find_col(df, "LevelOfDetail")
    if lod_col is not None:
        lod = df[lod_col].astype(str).str.strip().str.upper()
        summary_rows = lod == "SUMMARY"
        if summary_rows.any():
            df = df.loc[summary_rows].copy()

    s = df[sym_col].astype(str).str.strip().str.upper()
    mask = s.ne("") & ~s.eq("CASH")
    pos = df.loc[mask].copy()
    pos = pos.dropna(axis=1, how="all")
    return pos.reset_index(drop=True), None


def _sum_cash_from_flex(df: pd.DataFrame) -> float:
    """
    Extract the base-currency (USD) cash balance from a Flex Open Positions / Balances DataFrame.

    Strategy (tried in order):
    1. Rows where Symbol == "CASH" → sum Quantity/Balance column  (classic Open Positions format)
    2. Rows where AssetClass == "CASH" with a Quantity/Balance column  (Flex Cash Balances section)
    3. Dedicated ending-cash column anywhere in the frame  (Net Asset Value / Balances summary)
    """
    # --- Strategy 1: Symbol == "CASH" rows ---
    sym_col = _find_col(df, "Symbol")
    if sym_col:
        cash_rows = df[df[sym_col].astype(str).str.strip().str.upper().eq("CASH")]
        if not cash_rows.empty:
            for name in ("Quantity", "Position", "Balance", "EndingCash", "Ending Cash",
                         "CashBalance", "Ending Settled Cash"):
                c = _find_col(cash_rows, name)
                if c:
                    return float(pd.to_numeric(cash_rows[c], errors="coerce").fillna(0).sum())

    # --- Strategy 2: AssetClass == "CASH" rows (Flex Cash Balances section) ---
    ac_col = _find_col(df, "AssetClass")
    if ac_col:
        ac_rows = df[df[ac_col].astype(str).str.strip().str.upper().eq("CASH")]
        if not ac_rows.empty:
            # Try currency filter: keep only base currency (USD) rows if Currency column exists
            cur_col = _find_col(ac_rows, "Currency", "CurrencyPrimary")
            if cur_col:
                usd_rows = ac_rows[ac_rows[cur_col].astype(str).str.strip().str.upper().eq("USD")]
                if not usd_rows.empty:
                    ac_rows = usd_rows
            for name in ("Cash", "Quantity", "EndingCash", "Ending Cash", "CashBalance",
                         "Balance", "FxCashBalance", "NetCash"):
                c = _find_col(ac_rows, name)
                if c:
                    val = float(pd.to_numeric(ac_rows[c], errors="coerce").fillna(0).sum())
                    if abs(val) > 0:
                        return val

    # --- Strategy 3: dedicated ending-cash column in the whole frame ---
    for name in ("EndingCash", "Ending Cash", "TotalCash", "Total Cash",
                 "CashBalance", "NetLiquidation"):
        c = _find_col(df, name)
        if c:
            val = float(pd.to_numeric(df[c], errors="coerce").fillna(0).sum())
            if abs(val) > 0:
                return val

    return 0.0


def _total_position_market_value(pos_df: pd.DataFrame) -> float:
    if pos_df.empty:
        return 0.0
    for name in ("PositionValue", "Position Value", "MarketValue", "MarkValue", "Value"):
        c = _find_col(pos_df, name)
        if c:
            return float(pd.to_numeric(pos_df[c], errors="coerce").fillna(0).sum())
    qc = _find_col(pos_df, "Quantity", "Position", "Qty")
    mpc = _find_col(
        pos_df,
        "MarkPrice",
        "Mark Price",
        "ClosePrice",
        "Close Price",
    )
    if qc and mpc:
        return float(
            (
                pd.to_numeric(pos_df[qc], errors="coerce")
                * pd.to_numeric(pos_df[mpc], errors="coerce")
            )
            .fillna(0)
            .sum()
        )
    return 0.0


def _positions_dict_from_flex(pos_df: pd.DataFrame) -> Dict[str, float]:
    out: Dict[str, float] = {}
    sym_col = _find_col(pos_df, "Symbol")
    qc = _find_col(pos_df, "Quantity", "Position", "Qty")
    if not sym_col or not qc:
        return out
    for _, row in pos_df.iterrows():
        sym = str(row[sym_col]).strip()
        if not sym:
            continue
        qv = float(pd.to_numeric(row[qc], errors="coerce") or 0.0)
        if abs(qv) > 1e-9:
            out[sym] = qv
    return out


def _portfolio_entry_for_symbol(sym: str, portfolio: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    su = str(sym).strip().upper()
    for k, v in (portfolio or {}).items():
        if str(k).strip().upper() == su:
            return v if isinstance(v, dict) else None
    return None


def _load_positions_raw_from_cache() -> pd.DataFrame:
    if not os.path.isfile(POSITIONS_SNAPSHOT_FILE):
        return pd.DataFrame()
    try:
        df = pd.read_csv(POSITIONS_SNAPSHOT_FILE)
        if df is None or df.empty or len(df.columns) == 0:
            return pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame()


def _write_positions_snapshot(raw_df: pd.DataFrame) -> None:
    if raw_df is None or raw_df.empty:
        return
    os.makedirs(config.CACHE_DIR, exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(
        suffix=".csv", prefix="open_positions_", dir=config.CACHE_DIR
    )
    os.close(tmp_fd)
    try:
        raw_df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, POSITIONS_SNAPSHOT_FILE)
    except Exception:
        if os.path.isfile(tmp_path):
            os.remove(tmp_path)
        raise


def _build_display_positions(pos_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter and rename raw Flex Open Positions columns for UI display, then collapse
    lot-level rows into one row per symbol (Flex exports one row per lot when
    'Level of Detail = Lot' is selected).

    Numeric additive columns (Qty, Market Value, Cost Basis $, IBKR Unrealized PnL,
    % of NAV) are summed across lots; price/description columns use the first lot value.

    The cost/PnL columns appended by enrich_positions_with_trade_cost always appear last.
    """
    if pos_df is None or pos_df.empty:
        return pos_df if pos_df is not None else pd.DataFrame()

    # Build case-insensitive lookup: normalised flex name -> (original col, display name)
    norm_to_original: Dict[str, str] = {_hdr_norm(c): c for c in pos_df.columns}
    seen_display: set[str] = set()
    rename_map: Dict[str, str] = {}
    ordered_cols: list[str] = []

    for flex_name, display_name in _POSITION_DISPLAY_COLS:
        norm = _hdr_norm(flex_name)
        orig = norm_to_original.get(norm)
        if orig is None or display_name in seen_display:
            continue
        if orig in rename_map:
            continue
        rename_map[orig] = display_name
        seen_display.add(display_name)
        ordered_cols.append(orig)

    if not ordered_cols:
        return pos_df.copy()

    out = pos_df[ordered_cols].rename(columns=rename_map).copy()

    # Collapse lot rows → one row per Symbol
    if "Symbol" not in out.columns:
        return out

    # Columns whose values should be summed across lots (additive quantities)
    _SUM_COLS = {"Qty", "Market Value", "Cost Basis ($)", "IBKR Unrealized PnL", "% of NAV"}
    agg: Dict[str, str] = {}
    for col in out.columns:
        if col == "Symbol":
            continue
        # Convert candidate sum columns to numeric so groupby sum works correctly
        if col in _SUM_COLS:
            out[col] = pd.to_numeric(out[col], errors="coerce")
            agg[col] = "sum"
        else:
            agg[col] = "first"

    out = out.groupby("Symbol", as_index=False).agg(agg)
    return out


def enrich_positions_with_trade_cost(
    pos_df: pd.DataFrame,
    portfolio: Dict[str, Any],
) -> pd.DataFrame:
    """
    Filter Flex columns for display, then append diluted avg cost and unrealized PnL estimate
    from the local trade ledger (``process_trades``).

    Appended columns (always last):
      COL_DILUTED_AVG_COST_TRADES – diluted avg cost from trade_history.csv
      COL_UNREALIZED_PNL_EST      – (mark − diluted_avg) × qty  (estimate)
    """
    if pos_df is None or pos_df.empty:
        return pos_df if pos_df is not None else pd.DataFrame()

    out = _build_display_positions(pos_df)

    sym_col = _find_col(out, "Symbol")
    if not sym_col:
        out[COL_DILUTED_AVG_COST_TRADES] = np.nan
        out[COL_UNREALIZED_PNL_EST] = np.nan
        return out

    qty_col = _find_col(out, "Qty", "Quantity", "Position")
    mark_col = _find_col(out, "Mark Price", "MarkPrice", "ClosePrice", "Close Price")

    # Flex column that carries per-share cost for the position (fallback when no trade history)
    flex_cost_col = _find_col(out, "Cost Basis Price", "CostBasisPrice", "Avg Open Price", "AvgOpenPrice")

    diluted: list[float] = []
    unreal: list[float] = []
    unreal_pct: list[float] = []
    for _, row in out.iterrows():
        sym = str(row[sym_col]).strip()
        ent = _portfolio_entry_for_symbol(sym, portfolio)

        # --- Diluted avg cost from trade history ---
        ac = float("nan")
        _has_trade_history = ent is not None
        if _has_trade_history and ent.get("avg_cost") is not None:
            try:
                ac = round(float(ent["avg_cost"]), 4)
            except (TypeError, ValueError):
                pass

        # Fallback: use Flex Cost Basis Price ONLY when the symbol is completely absent
        # from trade history (e.g. positions opened after the last cache fetch).
        # When trade history exists (even if avg_cost == 0 due to profitable wave trading),
        # we respect the calculated value and do not silently replace it.
        if not _has_trade_history and flex_cost_col:
            flex_ac = pd.to_numeric(row.get(flex_cost_col), errors="coerce")
            if pd.notna(flex_ac) and flex_ac > 0:
                ac = round(float(flex_ac), 4)

        diluted.append(ac)

        ue = float("nan")
        ue_pct = float("nan")
        if qty_col and mark_col and pd.notna(ac) and ac >= 0:
            qv = pd.to_numeric(row.get(qty_col), errors="coerce")
            mv = pd.to_numeric(row.get(mark_col), errors="coerce")
            if pd.notna(qv) and pd.notna(mv):
                ue = round((float(mv) - ac) * float(qv), 2)
                cost_total = ac * float(qv)
                # When avg_cost == 0 (position fully recovered via wave trading profit),
                # the entire market value is unrealized gain; % return is not meaningful.
                if abs(cost_total) > 1e-9:
                    ue_pct = round(ue / cost_total * 100, 2)
        unreal.append(ue)
        unreal_pct.append(ue_pct)

    out[COL_DILUTED_AVG_COST_TRADES] = diluted
    out[COL_UNREALIZED_PNL_EST] = unreal
    out[COL_UNREALIZED_PNL_PCT] = unreal_pct
    return out


def _fetch_positions_raw_from_flex() -> pd.DataFrame:
    token = config.IBKR_FLEX_TOKEN
    qid = config.IBKR_FLEX_POSITIONS_QUERY_ID or config.IBKR_FLEX_QUERY_ID
    if not token or not qid:
        raise ValueError(
            "Missing IBKR_FLEX_TOKEN or Flex query id "
            "(set IBKR_FLEX_POSITIONS_QUERY_ID for Open Positions, or IBKR_FLEX_QUERY_ID as fallback)."
        )
    params = {"t": token, "q": qid, "v": "3"}
    return fetch_flex_report_dataframe(
        params,
        token,
        send_request_url=flex_send_request_url(),
        timeout=30,
        parse_for_open_positions=True,
    )


def _account_payload_from_positions_raw(raw_df: pd.DataFrame) -> Dict[str, Any]:
    positions_flex, warn = _positions_only_table(raw_df)
    cash = _sum_cash_from_flex(raw_df)
    total_mv = _total_position_market_value(positions_flex)
    positions = _positions_dict_from_flex(positions_flex)

    cols_preview = ", ".join(str(c) for c in list(raw_df.columns)[:18])
    if len(raw_df.columns) > 18:
        cols_preview += ", …"
    print(
        f"[Flex] positions snapshot: raw {len(raw_df)} row(s), {len(raw_df.columns)} col(s); "
        f"non-CASH position rows {len(positions_flex)}; cash_balance={cash}; "
        f"warning={'yes' if warn else 'no'}"
    )
    if warn:
        print(f"[Flex] positions snapshot warning: {warn}")
    elif len(positions_flex) == 0 and len(raw_df) > 0:
        print(
            f"[Flex] positions snapshot: first columns: {cols_preview}. "
            "If you expect holdings, confirm Open Positions is in this Flex query and Symbol is present."
        )

    return {
        "positions": positions,
        "cash_balance": round(cash, 2),
        "positions_flex_df": positions_flex,
        "positions_market_value_total": total_mv,
        "positions_flex_query_id": config.IBKR_FLEX_POSITIONS_QUERY_ID or config.IBKR_FLEX_QUERY_ID,
        "positions_flex_warning": warn,
    }


def fetch_ibkr_positions_and_cash(flex_refresh: bool = False) -> Dict[str, Any]:
    """
    Open Positions + cash: load from ``open_positions_snapshot.csv`` when available, or pull Flex.

    Writes the last Flex **raw** Open Positions table (parser output) to
    ``POSITIONS_SNAPSHOT_FILE``. Diluted cost in the UI still comes from
    ``process_trades(trades)`` using ``cache/trade_history.csv`` — not from this file.

    Args:
        flex_refresh: If True (e.g. after **Refresh All Data**), always call Flex and update the CSV.
    """
    raw_df = pd.DataFrame()
    if not flex_refresh:
        raw_df = _load_positions_raw_from_cache()
        if not raw_df.empty:
            print(
                f"📁 Loaded positions from {POSITIONS_SNAPSHOT_FILE} ({len(raw_df)} rows). "
                "Diluted cost still uses trade_history.csv via process_trades. "
                "Use Refresh All Data to update from Flex."
            )
            return _account_payload_from_positions_raw(raw_df)

    try:
        raw_df = _fetch_positions_raw_from_flex()
    except Exception as exc:
        fallback = _load_positions_raw_from_cache()
        if not fallback.empty:
            print(f"⚠️ Flex positions fetch failed ({exc}); using cached {POSITIONS_SNAPSHOT_FILE}.")
            raw_df = fallback
        else:
            raise

    if flex_refresh and raw_df.empty:
        fallback = _load_positions_raw_from_cache()
        if not fallback.empty:
            print(
                "⚠️ Flex returned empty positions snapshot; keeping previous "
                f"{POSITIONS_SNAPSHOT_FILE}."
            )
            raw_df = fallback

    if not raw_df.empty:
        _write_positions_snapshot(raw_df)

    return _account_payload_from_positions_raw(raw_df)
