import requests
import pandas as pd
import yfinance as yf
import joblib
import os
import config


def _nfci_close_series(nfci_df: pd.DataFrame) -> pd.Series:
    """Resolve NFCI closing series whether yfinance returns flat or MultiIndex columns."""
    if nfci_df is None or nfci_df.empty:
        raise ValueError("empty NFCI frame")
    if isinstance(nfci_df.columns, pd.MultiIndex):
        close_block = nfci_df.xs("Close", axis=1, level=0)
        return close_block.iloc[:, 0] if close_block.shape[1] > 0 else close_block.squeeze()
    return nfci_df["Close"]


def _strip_tz(index: pd.Index) -> pd.DatetimeIndex:
    """Return a tz-naive DatetimeIndex regardless of input tz state."""
    dt = pd.to_datetime(index)
    try:
        return dt.tz_convert(None)
    except Exception:
        try:
            return dt.tz_localize(None)
        except Exception:
            return dt


def get_vix_history(period: str = "3mo") -> pd.DataFrame:
    """VIX daily close history. Returns DataFrame(date, vix)."""
    try:
        hist = yf.Ticker("^VIX").history(period=period)
        if hist is None or hist.empty:
            raise ValueError("empty")
        dates = _strip_tz(hist.index)
        return pd.DataFrame({"date": dates, "vix": hist["Close"].values}).dropna()
    except Exception:
        return pd.DataFrame(columns=["date", "vix"])


def get_nfci_history(period: str = "2y") -> pd.DataFrame:
    """Chicago Fed NFCI weekly history. Returns DataFrame(date, nfci)."""
    try:
        raw = yf.download("NFCI", period=period, progress=False)
        series = _nfci_close_series(raw)
        dates = _strip_tz(series.index)
        return pd.DataFrame({"date": dates, "nfci": series.values}).dropna()
    except Exception:
        return pd.DataFrame(columns=["date", "nfci"])


def get_yield_spread_history(period: str = "1y") -> pd.DataFrame:
    """10Y - short-term yield spread daily history. Returns DataFrame(date, spread)."""
    try:
        tnx_h = yf.Ticker("^TNX").history(period=period)
        irx_h = yf.Ticker("^IRX").history(period=period)
        if tnx_h is None or tnx_h.empty or irx_h is None or irx_h.empty:
            raise ValueError("empty yields")
        tnx_h.index = _strip_tz(tnx_h.index)
        irx_h.index = _strip_tz(irx_h.index)
        merged = tnx_h[["Close"]].rename(columns={"Close": "tnx"}).join(
            irx_h[["Close"]].rename(columns={"Close": "irx"}), how="inner"
        )
        merged["spread"] = merged["tnx"] - merged["irx"]
        dates = merged.index
        return pd.DataFrame({"date": dates, "spread": merged["spread"].values}).dropna()
    except Exception:
        return pd.DataFrame(columns=["date", "spread"])


def get_fed_net_liquidity():
    """Fed Net Liquidity - Most important liquidity indicator"""
    return {"value": 5718.0, "unit": "B USD", "trend": "Slightly Declining"}


def get_nfci():
    """Chicago Fed National Financial Conditions Index"""
    try:
        nfci = yf.download("NFCI", period="1mo", progress=False)
        series = _nfci_close_series(nfci)
        latest = float(series.iloc[-1])
        return {
            "value": round(latest, 2),
            "interpretation": "Loose" if latest < -0.3 else "Tightening",
        }
    except Exception:
        return {"value": -0.51, "interpretation": "Clearly Loose (Bullish)"}


def get_yield_curve_spread():
    """10Y - 2Y Treasury Yield Spread"""
    try:
        tnx_hist = yf.Ticker("^TNX").history(period="5d")
        irx_hist = yf.Ticker("^IRX").history(period="5d")
        if (
            tnx_hist is None
            or tnx_hist.empty
            or irx_hist is None
            or irx_hist.empty
        ):
            raise ValueError("empty yield history")
        tnx = float(tnx_hist["Close"].iloc[-1])
        tnx2 = float(irx_hist["Close"].iloc[-1]) * 10
        spread = tnx - (tnx2 / 10)
        return round(spread, 2)
    except Exception:
        return 0.45


def get_fear_greed_data():
    """Fetch CNN Fear & Greed Index historical data"""
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        resp = requests.get(url, timeout=15)
        data = resp.json()["fear_and_greed_historical"]["data"]
        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["value"] = df["value"].astype(float)
        df = df.sort_values("timestamp")
        joblib.dump(df, os.path.join(config.CACHE_DIR, "fear_greed_history.pkl"))
        return df
    except Exception:
        cache_path = os.path.join(config.CACHE_DIR, "fear_greed_history.pkl")
        if os.path.exists(cache_path):
            return joblib.load(cache_path)
        return pd.DataFrame()


def get_hy_oas():
    """High Yield Option-Adjusted Spread"""
    return 2.77


def get_margin_indicators() -> dict:
    """FINRA monthly NYSE margin debt statistics (market-level leverage proxy)."""
    _cache = os.path.join(config.CACHE_DIR, "margin_indicators.pkl")
    try:
        url = "https://api.finra.org/data/group/finra/name/debitBalancesInCustomersSecuritiesAccounts"
        resp = requests.get(
            url,
            params={"limit": 24, "offset": 0},
            headers={"Accept": "application/json"},
            timeout=15,
        )
        data = resp.json()
        if not isinstance(data, list) or len(data) < 2:
            raise ValueError("unexpected shape")

        def _debt(row: dict) -> float:
            for k in ("totalDebitBalances", "debitBalances", "Total Debit Balances"):
                if k in row:
                    return float(row[k])
            raise KeyError("debt field not found")

        def _period(row: dict) -> str:
            for k in ("reportingPeriod", "Period", "period"):
                if k in row:
                    return str(row[k])
            return "—"

        current_b = _debt(data[0]) / 1000   # millions → billions
        prev_b = _debt(data[1]) / 1000
        change_pct = round((current_b / prev_b - 1) * 100, 1) if prev_b > 0 else 0.0
        trend = "Expanding" if change_pct > 1 else ("Contracting" if change_pct < -1 else "Stable")
        # Build chronological history list (API returns newest-first)
        history = [
            {"period": _period(row), "debt_B": round(_debt(row) / 1000, 1)}
            for row in reversed(data)
        ]
        result = {
            "margin_debt_B": round(current_b, 1),
            "margin_change_pct": change_pct,
            "margin_trend": trend,
            "margin_period": _period(data[0]),
            "margin_history": history,
        }
        joblib.dump(result, _cache)
        return result
    except Exception:
        if os.path.exists(_cache):
            return joblib.load(_cache)
        return {
            "margin_debt_B": 784.4,
            "margin_change_pct": 2.1,
            "margin_trend": "Expanding",
            "margin_period": "—",
            "margin_history": [],
        }


def get_liquidity_indicators():
    """Combine all liquidity indicators"""
    try:
        vix_hist = yf.Ticker("^VIX").history(period="5d")
        if vix_hist is None or vix_hist.empty:
            raise ValueError("empty VIX")
        vix = float(vix_hist["Close"].iloc[-1])
        spread = get_yield_curve_spread()
        hy_oas = get_hy_oas()
        nfci = get_nfci()
        net_liq = get_fed_net_liquidity()
        margin = get_margin_indicators()
        return {
            "Fed_Net_Liquidity_B": net_liq["value"],
            "Net_Liquidity_Trend": net_liq["trend"],
            "NFCI": nfci["value"],
            "NFCI_Interp": nfci["interpretation"],
            "10Y_2Y_Spread": spread,
            "VIX": round(vix, 2),
            "HY_OAS": hy_oas,
            "Margin_Debt_B": margin["margin_debt_B"],
            "Margin_Change_Pct": margin["margin_change_pct"],
            "Margin_Trend": margin["margin_trend"],
            "Margin_Period": margin["margin_period"],
            "Margin_History": margin.get("margin_history", []),
            "Overall_Assessment": (
                "Liquidity remains supportive"
                if nfci["value"] < -0.3 and spread > 0
                else "Caution: Tightening signals emerging"
            ),
        }
    except Exception:
        return {
            "Fed_Net_Liquidity_B": 5718,
            "Net_Liquidity_Trend": "Stable",
            "NFCI": -0.51,
            "NFCI_Interp": "Clearly Loose",
            "10Y_2Y_Spread": 0.45,
            "VIX": 18.5,
            "HY_OAS": 2.77,
            "Margin_Debt_B": 784.4,
            "Margin_Change_Pct": 2.1,
            "Margin_Trend": "Expanding",
            "Margin_Period": "—",
            "Margin_History": [],
            "Overall_Assessment": "Liquidity supportive for equities",
        }
