"""
DCF valuation models for individual equities.

All models compute Equity Value per share by:
  1. Discounting projected Free Cash Flows to Firm (FCF = OCF − CapEx)
  2. Subtracting net debt  →  Enterprise Value → Equity Value
  3. Dividing by shares outstanding

FCF sourced from yfinance cash-flow statement; up to 3 years are averaged to
smooth out single-year noise (capex spikes, working-capital swings, etc.).
"""

import warnings
from typing import List, Optional

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _row_series(cf: pd.DataFrame, *labels: str, max_years: int = 3) -> List[float]:
    """Return up to `max_years` annual values (most-recent first) for a cash-flow row."""
    if cf is None or cf.empty:
        return []
    for label in labels:
        if label in cf.index:
            return [
                float(v)
                for v in cf.loc[label].iloc[:max_years]
                if pd.notna(v)
            ]
    return []


def _compute_fcf_series(cf: pd.DataFrame) -> List[float]:
    """Derive FCF = OCF − |CapEx| for each available year.  Returns only positive values."""
    ocf_vals = _row_series(
        cf,
        "Operating Cash Flow",
        "Net Cash Provided By Operating Activities",
    )
    cpx_vals = _row_series(
        cf,
        "Capital Expenditure",
        "Capital Expenditures",
    )
    fcf_list = []
    for i in range(max(len(ocf_vals), len(cpx_vals))):
        ocf = ocf_vals[i] if i < len(ocf_vals) else 0.0
        cpx = cpx_vals[i] if i < len(cpx_vals) else 0.0
        fcf = ocf - abs(cpx)           # abs() handles both sign conventions
        if fcf > 0:
            fcf_list.append(fcf)
    return fcf_list


# ──────────────────────────────────────────────────────────────────────────────
# Data fetcher
# ──────────────────────────────────────────────────────────────────────────────

def get_financials(symbol: str) -> dict:
    """
    Fetch fundamentals from yfinance.

    Returns:
      avg_fcf           – average of up to 3 years of positive FCF (more stable than a single year)
      shares_outstanding
      current_price
      net_debt          – totalDebt − totalCash  (positive = indebted; negative = net-cash company)
      fcf_cagr          – implied historical FCF growth rate (NaN if < 2 data points)
    """
    ticker = yf.Ticker(symbol)
    info = ticker.info or {}

    avg_fcf = 0.0
    fcf_cagr = float("nan")
    try:
        cf = ticker.cashflow
        fcf_list = _compute_fcf_series(cf) if (cf is not None and not cf.empty) else []
        if fcf_list:
            avg_fcf = sum(fcf_list) / len(fcf_list)
            if len(fcf_list) >= 2:
                # Annualised CAGR over the available years
                fcf_cagr = (fcf_list[0] / fcf_list[-1]) ** (1 / (len(fcf_list) - 1)) - 1
        else:
            # Fallback: yfinance summary field
            avg_fcf = float(info.get("freeCashflow") or 0)
    except Exception:
        avg_fcf = float(info.get("freeCashflow") or 0)

    shares = float(
        info.get("sharesOutstanding")
        or info.get("impliedSharesOutstanding")
        or 1
    )
    current_price = float(
        info.get("currentPrice") or info.get("regularMarketPrice") or 0
    )

    total_debt = float(info.get("totalDebt") or 0)
    total_cash = float(info.get("totalCash") or 0)
    net_debt = total_debt - total_cash   # positive = leveraged; negative = net-cash

    return {
        "avg_fcf": avg_fcf,
        "shares_outstanding": shares,
        "current_price": current_price,
        "net_debt": net_debt,
        "fcf_cagr": fcf_cagr,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Valuation models
# ──────────────────────────────────────────────────────────────────────────────

def two_stage_dcf(
    fcf0: float,
    high_growth: float,
    high_years: int,
    terminal_growth: float,
    wacc: float,
    shares: float,
    net_debt: float = 0.0,
) -> float:
    """
    Two-stage FCF DCF → equity value per share.

    Stage 1: FCF grows at `high_growth` for `high_years`.
    Stage 2: perpetuity at `terminal_growth`.
    Net-debt adjustment converts enterprise value to equity value.
    """
    if fcf0 <= 0 or wacc <= terminal_growth or shares <= 0:
        return 0.0

    fcf_series = [fcf0 * (1 + high_growth) ** t for t in range(1, high_years + 1)]
    pv_fcf = sum(f / (1 + wacc) ** t for t, f in enumerate(fcf_series, 1))

    terminal_fcf = fcf_series[-1] * (1 + terminal_growth)
    terminal_value = terminal_fcf / (wacc - terminal_growth)
    pv_terminal = terminal_value / (1 + wacc) ** high_years

    equity_value = (pv_fcf + pv_terminal) - net_debt
    return round(max(equity_value / shares, 0.0), 2)


def perpetual_dcf(
    fcf0: float,
    growth: float,
    wacc: float,
    shares: float,
    net_debt: float = 0.0,
) -> float:
    """Gordon Growth Model (single-stage perpetuity)."""
    if fcf0 <= 0 or wacc <= growth or shares <= 0:
        return 0.0
    enterprise_value = fcf0 * (1 + growth) / (wacc - growth)
    equity_value = enterprise_value - net_debt
    return round(max(equity_value / shares, 0.0), 2)


# ──────────────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────────────

def calculate_all_dcf(symbol: str, assumptions: Optional[dict] = None) -> dict:
    """
    Run three DCF scenarios and return a summary dict.

    Keys always present in success result:
      Two Stage DCF, Conservative Two Stage, Perpetual (Gordon),
      Average Fair Value, Current Price, Upside (%), Latest FCF,
      Shares Outstanding, Net Debt ($)
    """
    if assumptions is None:
        assumptions = {
            "high_growth": 0.12,
            "high_years": 5,
            "terminal_growth": 0.03,
            "wacc": 0.10,
        }

    data = get_financials(symbol)
    fcf0 = data["avg_fcf"]
    shares = data["shares_outstanding"]
    current_price = data["current_price"]
    net_debt = data["net_debt"]

    if fcf0 <= 0:
        return {
            "error": (
                f"{symbol} has no positive Free Cash Flow in the available history. "
                "DCF requires positive FCF. For pre-profit / FCF-negative companies, "
                "consider revenue-multiple or EV/EBITDA approaches instead."
            )
        }

    hg = assumptions["high_growth"]
    hy = int(assumptions["high_years"])
    tg = assumptions["terminal_growth"]
    w = assumptions["wacc"]

    base = two_stage_dcf(fcf0, hg, hy, tg, w, shares, net_debt)
    conservative = two_stage_dcf(
        fcf0,
        hg * 0.7,           # lower growth assumption
        hy,
        tg * 0.8,           # lower terminal growth
        w + 0.01,           # +1% risk premium
        shares,
        net_debt,
    )
    gordon = perpetual_dcf(
        fcf0,
        tg + 0.02,          # slightly above terminal growth
        w,
        shares,
        net_debt,
    )

    model_vals = [v for v in (base, conservative, gordon) if v > 0]
    avg_fv = round(sum(model_vals) / len(model_vals), 2) if model_vals else 0.0

    upside_pct = (
        round((avg_fv / current_price - 1) * 100, 1)
        if current_price > 0 and avg_fv > 0
        else 0.0
    )

    return {
        "Two Stage DCF": base,
        "Conservative Two Stage": conservative,
        "Perpetual (Gordon)": gordon,
        "Average Fair Value": avg_fv,
        "Current Price": round(current_price, 2),
        "Upside (%)": upside_pct,
        "Latest FCF": round(fcf0, 0),
        "Shares Outstanding": int(shares),
        "Net Debt ($)": round(net_debt, 0),
    }
