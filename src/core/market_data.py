import yfinance as yf


def get_current_price(symbol: str) -> float:
    """Get latest market price using yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="5d")
        if hist is None or hist.empty or "Close" not in hist.columns:
            return 0.0
        close = hist["Close"].dropna()
        if close.empty:
            return 0.0
        return float(close.iloc[-1])
    except Exception:
        return 0.0


def get_prices_for_portfolio(symbols: list) -> dict:
    """Batch fetch current prices for multiple symbols"""
    prices = {}
    for sym in symbols:
        prices[sym] = get_current_price(sym)
    return prices
