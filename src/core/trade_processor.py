import re

import pandas as pd
import numpy as np


def _normalize_action(raw) -> str:
    """Map Flex variants to BUY / SELL for cost logic (display column unchanged)."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = str(raw).strip().upper()
    if s in ("B", "BOT", "BUY") or s.startswith("BUY"):
        return "BUY"
    if s in ("S", "SLD", "SELL") or s.startswith("SELL"):
        return "SELL"
    return s


def _filter_level_of_detail(df: pd.DataFrame, lod_col: str) -> pd.DataFrame:
    """
    Per-symbol LevelOfDetail deduplication.

    IBKR Flex can emit three row types for each trade:
      ORDER      – aggregate order (qty = sum of all fills) — duplicates execution rows
      EXECUTION  – individual fill(s) — what we want
      CLOSED_LOT – internal lot-matching record — NOT an actual trade, always excluded

    Strategy (per symbol, not globally):
      • Always drop CLOSED_LOT rows for every symbol.
      • For symbols that have EXECUTION rows → keep only EXECUTION rows.
      • For symbols that only have ORDER rows → keep ORDER rows (fallback for Flex
        queries that export order-level detail only — otherwise those symbols disappear).
      • Rows with any other / missing level → kept as-is.

    This prevents both double-counting (ORDER + EXECUTION) AND missing symbols (symbols
    that only have ORDER rows being dropped by a global "keep-EXECUTION-only" filter).
    """
    levels = df[lod_col].astype(str).str.strip().str.upper()

    # Step 1: always remove CLOSED_LOT (lot-matching records, not real trades)
    df = df[levels != "CLOSED_LOT"].copy()
    levels = df[lod_col].astype(str).str.strip().str.upper()

    # Step 2: per-symbol preference
    exec_mask = levels == "EXECUTION"
    order_mask = levels == "ORDER"
    other_mask = ~levels.isin(["EXECUTION", "ORDER"])

    # Symbols that have at least one EXECUTION row get EXECUTION-only treatment
    syms_with_exec: set = set(df.loc[exec_mask, "symbol"].unique()) if exec_mask.any() else set()

    # ORDER rows are kept only for symbols with NO EXECUTION rows
    order_fallback = order_mask & ~df["symbol"].isin(syms_with_exec)

    return df[exec_mask | order_fallback | other_mask].copy()


def process_trades(trades_df: pd.DataFrame) -> dict:
    """
    Net-cash-flow diluted average cost basis.

    For each symbol the result includes:
      current_shares  – net shares remaining after all trades
      avg_cost        – diluted avg cost per share (0 when fully closed)
                        = (total_buy_cost − total_sell_proceeds) / current_shares
      realized_pnl    – total realised P&L for fully-closed positions
                        = total_sell_proceeds − total_buy_cost
                        (0 for open positions — unrealized is computed at display time)
      close_price     – price of the LAST sell trade (for the closed-position marker)
      last_sell_date  – date of the last sell trade
      trades          – list of per-trade dicts with 'shares_after' field

    IBKR Flex 'proceeds' column sign convention:
      BUY  → proceeds < 0  (cash outflow, you paid money)
      SELL → proceeds > 0  (cash inflow, you received money)

    We use abs(proceeds) for both sides so the sign convention doesn't matter:
      buy_cost_i   = abs(proceeds_i) for BUY rows
      sell_proc_i  = abs(proceeds_i) for SELL rows

    If the proceeds column is absent we fall back to qty × price.
    """
    result = {}
    if trades_df is None or trades_df.empty:
        return result

    df = trades_df.copy()

    # ── Filter 1: keep equity (STK) rows only ──────────────────────────────────
    # CASH rows are FX round-trips (USD.HKD etc.) and BILL rows are T-Bills.
    # Neither is a stock position; both produce nonsense avg_cost=0 when treated
    # as equities.  Fall back to all rows when the AssetClass column is absent.
    ac_col = next(
        (c for c in df.columns if re.sub(r"[^a-z0-9]+", "", c.strip().lower()) == "assetclass"),
        None,
    )
    if ac_col is not None:
        stk_mask = df[ac_col].astype(str).str.strip().str.upper() == "STK"
        if stk_mask.any():
            df = df[stk_mask].copy()

    # ── Filter 2: LevelOfDetail deduplication (per-symbol) ────────────────────
    lod_col = next(
        (c for c in df.columns if re.sub(r"[^a-z0-9]+", "", c.strip().lower()) == "levelofdetail"),
        None,
    )
    if lod_col is not None and not df.empty:
        df = _filter_level_of_detail(df, lod_col)

    if df.empty:
        return result

    for symbol, group in df.groupby("symbol"):
        group = group.sort_values("date").copy()
        group["_act"] = group["action"].map(_normalize_action)
        group["_qty"] = pd.to_numeric(group["quantity"], errors="coerce").fillna(0).abs()
        group["_price"] = pd.to_numeric(group["price"], errors="coerce").fillna(0)

        # Use actual proceeds when available (more accurate than qty × price).
        # abs() on both sides makes the sign convention irrelevant: works whether
        # IBKR exports proceeds as negative-for-BUY/positive-for-SELL or vice-versa,
        # because we route by _act (BUY/SELL) not by the sign of proceeds.
        if "proceeds" in group.columns:
            _proc_abs = pd.to_numeric(group["proceeds"], errors="coerce").fillna(0).abs()
            group["_buy_cost"] = np.where(group["_act"] == "BUY", _proc_abs, 0.0)
            group["_sell_proc"] = np.where(group["_act"] == "SELL", _proc_abs, 0.0)
        else:
            group["_buy_cost"] = np.where(
                group["_act"] == "BUY", group["_qty"] * group["_price"], 0.0
            )
            group["_sell_proc"] = np.where(
                group["_act"] == "SELL", group["_qty"] * group["_price"], 0.0
            )

        # Signed qty: BUY adds shares, SELL removes shares.
        group["_signed_qty"] = np.where(group["_act"] == "SELL", -group["_qty"], group["_qty"])
        group["shares_after"] = group["_signed_qty"].cumsum()

        total_buy_cost = float(group["_buy_cost"].sum())
        total_sell_proc = float(group["_sell_proc"].sum())
        current_shares = float(group["shares_after"].iloc[-1])

        if abs(current_shares) > 1e-9:
            # Open position: avg_cost = net cash still invested / remaining shares
            # This naturally reflects "wave trading" benefits: selling high and
            # rebuying low reduces net_cost, lowering avg_cost proportionally.
            net_cost = total_buy_cost - total_sell_proc
            avg_cost = net_cost / current_shares if current_shares > 0 else 0.0
            avg_cost = max(0.0, avg_cost)
            realized_pnl = 0.0
        else:
            # Fully closed: realized P&L = total cash received − total cash paid
            avg_cost = 0.0
            realized_pnl = total_sell_proc - total_buy_cost

        sell_rows = group[group["_act"] == "SELL"]
        close_price = float(sell_rows["_price"].iloc[-1]) if not sell_rows.empty else 0.0
        last_sell_date = (
            pd.to_datetime(sell_rows["date"].iloc[-1]).date() if not sell_rows.empty else None
        )

        trade_cols = ["date", "action", "quantity", "price", "shares_after"]
        if "proceeds" in group.columns:
            trade_cols.append("proceeds")

        clean = group.drop(
            columns=[
                c for c in ["_act", "_qty", "_price", "_buy_cost", "_sell_proc", "_signed_qty"]
                if c in group.columns
            ],
            errors="ignore",
        )
        result[symbol] = {
            "current_shares": current_shares,
            "avg_cost": round(avg_cost, 4),
            "realized_pnl": round(realized_pnl, 2),
            "close_price": close_price,
            "last_sell_date": str(last_sell_date) if last_sell_date else None,
            "trades": clean[[c for c in trade_cols if c in clean.columns]].to_dict("records"),
        }
    return result
