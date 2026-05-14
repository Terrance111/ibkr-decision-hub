import os
from dotenv import load_dotenv

load_dotenv()

# IBKR Flex Configuration
# Token: Client Portal → Reporting → Flex Web Service.
IBKR_FLEX_TOKEN = os.getenv("IBKR_FLEX_TOKEN")
# Trades: Activity Flex query id used with date range (fd/td) for trade_history.csv (see src/data/ibkr_fetch.py).
IBKR_FLEX_QUERY_ID = os.getenv("IBKR_FLEX_QUERY_ID")
# Positions: Activity Flex query id for Open Positions snapshot (no fd/td; see src/data/ibkr_account.py).
# If empty, positions + cash use IBKR_FLEX_QUERY_ID (same id as trades — only valid if that template returns Open Positions).
IBKR_FLEX_POSITIONS_QUERY_ID = (os.getenv("IBKR_FLEX_POSITIONS_QUERY_ID") or "").strip()

# Flex Web Service SendRequest base URL (no query string). Override via IBKR_FLEX_SEND_REQUEST_URL in .env if IBKR changes hosts.
_default_flex_send = (
    "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest"
)
IBKR_FLEX_SEND_REQUEST_URL = (os.getenv("IBKR_FLEX_SEND_REQUEST_URL") or "").strip() or _default_flex_send

# Cache directory
CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

print("✅ Configuration loaded successfully.")
