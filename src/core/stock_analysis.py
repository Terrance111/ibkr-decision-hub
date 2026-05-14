"""
Professional stock analysis module.

Provides valuation multiples, analyst consensus, institutional ownership,
earnings momentum, and enhanced technical indicators — all sourced from
yfinance with graceful fallbacks when data is unavailable.
"""

from __future__ import annotations

import math
import warnings
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

_NA = "—"


def _safe(val: Any, fmt: str = "", suffix: str = "") -> str:
    """Format a value safely; return _NA on None / NaN / non-finite."""
    if val is None:
        return _NA
    try:
        v = float(val)
        if not math.isfinite(v):
            return _NA
        return (format(v, fmt) + suffix) if fmt else str(round(v, 2))
    except (TypeError, ValueError):
        return str(val) if val else _NA


# ─────────────────────────────────────────────────────────────────────────────
# Valuation multiples
# ─────────────────────────────────────────────────────────────────────────────

def get_valuation_multiples(info: dict) -> dict:
    def _ratio(key: str) -> float | None:
        v = info.get(key)
        return float(v) if v is not None else None

    fwd_pe = _ratio("forwardPE")
    trail_pe = _ratio("trailingPE")
    ev_ebitda = _ratio("enterpriseToEbitda")
    ps = _ratio("priceToSalesTrailing12Months")
    pb = _ratio("priceToBook")
    peg = _ratio("pegRatio")

    # P/FCF = market cap / free cash flow
    pfcf = None
    mc = info.get("marketCap")
    fcf = info.get("freeCashflow")
    if mc and fcf and float(fcf) > 0:
        pfcf = float(mc) / float(fcf)

    return {
        "Forward P/E": fwd_pe,
        "Trailing P/E": trail_pe,
        "EV/EBITDA": ev_ebitda,
        "P/S": ps,
        "P/B": pb,
        "P/FCF": round(pfcf, 1) if pfcf else None,
        "PEG": peg,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Key fundamentals
# ─────────────────────────────────────────────────────────────────────────────

def get_fundamentals(info: dict) -> dict:
    def _pct(key: str) -> float | None:
        v = info.get(key)
        return round(float(v) * 100, 2) if v is not None else None

    def _raw(key: str) -> float | None:
        v = info.get(key)
        return round(float(v), 4) if v is not None else None

    return {
        "Revenue Growth YoY": _pct("revenueGrowth"),
        "Earnings Growth YoY": _pct("earningsGrowth"),
        "Gross Margin": _pct("grossMargins"),
        "Operating Margin": _pct("operatingMargins"),
        "Net Margin": _pct("profitMargins"),
        "ROE": _pct("returnOnEquity"),
        "ROA": _pct("returnOnAssets"),
        "Debt/Equity": _raw("debtToEquity"),
        "Current Ratio": _raw("currentRatio"),
        "Beta": _raw("beta"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Analyst consensus
# ─────────────────────────────────────────────────────────────────────────────

def get_analyst_consensus(ticker: yf.Ticker, info: dict) -> dict:
    result: dict = {
        "mean_target": None,
        "high_target": None,
        "low_target": None,
        "upside_pct": None,
        "current_price": None,
        "rating_counts": {},
        "overall_rating": _NA,
    }

    current = float(info.get("currentPrice") or info.get("regularMarketPrice") or 0)
    result["current_price"] = current if current > 0 else None
    result["mean_target"] = info.get("targetMeanPrice")
    result["high_target"] = info.get("targetHighPrice")
    result["low_target"] = info.get("targetLowPrice")

    if result["mean_target"] and current > 0:
        result["upside_pct"] = round((float(result["mean_target"]) / current - 1) * 100, 1)

    # Analyst rating distribution
    try:
        rec = ticker.recommendations_summary
        if rec is not None and not rec.empty:
            counts: dict[str, int] = {}
            for _, row in rec.iterrows():
                period = str(row.get("period", "")).strip()
                if period != "0m":
                    continue
                for col in rec.columns:
                    if col == "period":
                        continue
                    label = col.strip().title()
                    counts[label] = int(row[col]) if pd.notna(row[col]) else 0
            result["rating_counts"] = counts
    except Exception:
        pass

    if not result["rating_counts"]:
        try:
            recs = ticker.recommendations
            if recs is not None and not recs.empty:
                recent = recs.tail(20)
                if "To Grade" in recent.columns:
                    vc = recent["To Grade"].value_counts()
                    result["rating_counts"] = vc.to_dict()
        except Exception:
            pass

    # Derive overall rating label
    rc = result["rating_counts"]
    if rc:
        buy_like = sum(v for k, v in rc.items() if any(w in k.upper() for w in ("BUY", "STRONG BUY", "OUTPERFORM", "OVERWEIGHT")))
        sell_like = sum(v for k, v in rc.items() if any(w in k.upper() for w in ("SELL", "UNDERPERFORM", "UNDERWEIGHT")))
        hold_like = sum(v for k, v in rc.items() if any(w in k.upper() for w in ("HOLD", "NEUTRAL", "MARKET")))
        total = buy_like + sell_like + hold_like
        if total > 0:
            if buy_like / total >= 0.6:
                result["overall_rating"] = "Strong Buy"
            elif buy_like / total >= 0.4:
                result["overall_rating"] = "Buy"
            elif sell_like / total >= 0.4:
                result["overall_rating"] = "Sell"
            else:
                result["overall_rating"] = "Hold"

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Institutional & insider
# ─────────────────────────────────────────────────────────────────────────────

def get_institutional_data(ticker: yf.Ticker) -> dict:
    result: dict = {"top_holders": pd.DataFrame(), "insider_direction": _NA}

    try:
        ih = ticker.institutional_holders
        if ih is not None and not ih.empty:
            cols = [c for c in ["Holder", "Shares", "% Out", "Value", "Date Reported"] if c in ih.columns]
            result["top_holders"] = ih[cols].head(8).reset_index(drop=True)
    except Exception:
        pass

    try:
        it = ticker.insider_transactions
        if it is not None and not it.empty:
            # Look at recent 90 days of insider transactions
            if "Start Date" in it.columns:
                it["_dt"] = pd.to_datetime(it["Start Date"], errors="coerce")
            elif "Date" in it.columns:
                it["_dt"] = pd.to_datetime(it["Date"], errors="coerce")
            else:
                it["_dt"] = pd.NaT

            cutoff = pd.Timestamp.now() - pd.Timedelta(days=90)
            recent = it[it["_dt"] >= cutoff] if it["_dt"].notna().any() else it.head(20)

            if "Shares" in recent.columns and "Transaction" in recent.columns:
                buys = recent[recent["Transaction"].astype(str).str.upper().str.contains("BUY|PURCHASE", na=False)]["Shares"].apply(pd.to_numeric, errors="coerce").sum()
                sells = recent[recent["Transaction"].astype(str).str.upper().str.contains("SELL|SALE", na=False)]["Shares"].apply(pd.to_numeric, errors="coerce").sum()
                if buys > sells * 1.5:
                    result["insider_direction"] = "Net Buying"
                elif sells > buys * 1.5:
                    result["insider_direction"] = "Net Selling"
                else:
                    result["insider_direction"] = "Mixed"
    except Exception:
        pass

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Earnings momentum
# ─────────────────────────────────────────────────────────────────────────────

def get_earnings_momentum(ticker: yf.Ticker, info: dict) -> dict:
    result: dict = {
        "eps_history": pd.DataFrame(),
        "next_earnings": _NA,
        "eps_trend": _NA,
    }

    # Next earnings date
    try:
        ned = info.get("earningsTimestamp") or info.get("earningsDate")
        if ned:
            ts = pd.Timestamp(ned, unit="s") if isinstance(ned, (int, float)) else pd.Timestamp(ned)
            result["next_earnings"] = ts.strftime("%Y-%m-%d")
    except Exception:
        pass

    try:
        dates_df = ticker.earnings_dates
        if dates_df is not None and not dates_df.empty:
            future = dates_df[dates_df.index > pd.Timestamp.now()]
            if not future.empty:
                result["next_earnings"] = future.index[0].strftime("%Y-%m-%d")
    except Exception:
        pass

    # EPS history (last 4 quarters)
    try:
        eh = ticker.earnings_history
        if eh is not None and not eh.empty:
            cols = [c for c in ["quarter", "epsActual", "epsEstimate", "surprisePercent"] if c in eh.columns]
            if not cols:
                cols = list(eh.columns[:4])
            hist = eh[cols].tail(4).reset_index(drop=True)
            result["eps_history"] = hist
            # Trend: compare last 2 quarters of actual EPS
            if "epsActual" in hist.columns and len(hist) >= 2:
                last = pd.to_numeric(hist["epsActual"], errors="coerce").dropna()
                if len(last) >= 2 and last.iloc[-1] > last.iloc[-2]:
                    result["eps_trend"] = "Improving"
                elif len(last) >= 2 and last.iloc[-1] < last.iloc[-2]:
                    result["eps_trend"] = "Declining"
                else:
                    result["eps_trend"] = "Stable"
    except Exception:
        pass

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Enhanced technicals
# ─────────────────────────────────────────────────────────────────────────────

def get_technicals(ticker: yf.Ticker) -> dict:
    result: dict = {
        "rsi": None,
        "macd_signal": _NA,
        "ma20": None, "ma50": None, "ma200": None,
        "price": None,
        "price_vs_ma20": None, "price_vs_ma50": None, "price_vs_ma200": None,
        "bb_position": None,
        "week52_high": None, "week52_low": None, "week52_pct": None,
        "rel_volume": None,
        "avg_volume": None,
        "volume": None,
    }
    try:
        hist = ticker.history(period="1y")
        if hist is None or hist.empty or len(hist) < 20:
            return result

        close = hist["Close"]
        volume = hist["Volume"]
        price = float(close.iloc[-1])
        result["price"] = round(price, 2)

        # MAs
        for n, key in [(20, "ma20"), (50, "ma50"), (200, "ma200")]:
            if len(close) >= n:
                ma = float(close.rolling(n).mean().iloc[-1])
                result[key] = round(ma, 2)
                pct_key = f"price_vs_{key}"
                result[pct_key] = round((price / ma - 1) * 100, 2)

        # RSI(14)
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi_val = float((100 - 100 / (1 + rs)).iloc[-1])
        result["rsi"] = round(rsi_val, 1) if math.isfinite(rsi_val) else None

        # MACD
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        macd_now = float(macd_line.iloc[-1])
        sig_now = float(signal_line.iloc[-1])
        if math.isfinite(macd_now) and math.isfinite(sig_now):
            result["macd_signal"] = "Bullish" if macd_now > sig_now else "Bearish"

        # Bollinger Bands (20-day, 2σ)
        bb_mid = close.rolling(20).mean()
        bb_std = close.rolling(20).std()
        bb_upper = bb_mid + 2 * bb_std
        bb_lower = bb_mid - 2 * bb_std
        u, l = float(bb_upper.iloc[-1]), float(bb_lower.iloc[-1])
        if u > l:
            result["bb_position"] = round((price - l) / (u - l) * 100, 1)

        # 52-week range
        high52 = float(close.rolling(252, min_periods=50).max().iloc[-1])
        low52 = float(close.rolling(252, min_periods=50).min().iloc[-1])
        result["week52_high"] = round(high52, 2)
        result["week52_low"] = round(low52, 2)
        if high52 > low52:
            result["week52_pct"] = round((price - low52) / (high52 - low52) * 100, 1)

        # Volume
        avg_vol = float(volume.rolling(20).mean().iloc[-1])
        today_vol = float(volume.iloc[-1])
        result["avg_volume"] = int(avg_vol) if math.isfinite(avg_vol) else None
        result["volume"] = int(today_vol) if math.isfinite(today_vol) else None
        result["rel_volume"] = round(today_vol / avg_vol, 2) if avg_vol > 0 else None

    except Exception:
        pass

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def get_stock_analysis(symbol: str) -> dict:
    """
    Return a comprehensive analysis dict for `symbol`.
    All sub-sections degrade gracefully to empty / _NA on API failures.
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}
    except Exception:
        info = {}
        ticker = None  # type: ignore

    result: dict = {
        "symbol": symbol,
        "name": info.get("longName") or info.get("shortName") or symbol,
        "sector": info.get("sector") or _NA,
        "industry": info.get("industry") or _NA,
        "market_cap": info.get("marketCap"),
        "current_price": float(info.get("currentPrice") or info.get("regularMarketPrice") or 0) or None,
        "valuation": get_valuation_multiples(info),
        "fundamentals": get_fundamentals(info),
        "analyst": get_analyst_consensus(ticker, info) if ticker else {},
        "institutional": get_institutional_data(ticker) if ticker else {},
        "earnings": get_earnings_momentum(ticker, info) if ticker else {},
        "technicals": get_technicals(ticker) if ticker else {},
        "error": None,
    }

    if not info and ticker is None:
        result["error"] = f"Could not fetch data for {symbol}"

    return result
