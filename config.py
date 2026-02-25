"""
config.py - Load and expose all settings from environment variables.
All tunable parameters live here so scanner.py stays logic-only.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── API ──────────────────────────────────────────────────────────────────────
POLYMARKET_API_URL: str = os.getenv(
    "POLYMARKET_API_URL", "https://clob.polymarket.com"
)

# ── Scanning ─────────────────────────────────────────────────────────────────
# How often (in seconds) to re-scan all filtered markets
SCAN_INTERVAL_SECONDS: int = int(os.getenv("SCAN_INTERVAL_SECONDS", "10"))

# ── Arbitrage detection ───────────────────────────────────────────────────────
# Flag a market when YES_ask + NO_ask < ARB_THRESHOLD
# A "fair" binary market sums to exactly 1.00; anything below leaves free edge
ARB_THRESHOLD: float = float(os.getenv("ARB_THRESHOLD", "0.98"))

# ── Liquidity filter ─────────────────────────────────────────────────────────
# Skip markets whose reported liquidity is below this USD value
MIN_LIQUIDITY: float = float(os.getenv("MIN_LIQUIDITY", "500"))

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_FILE: str = os.getenv("LOG_FILE", "opportunities.csv")

# ── Market keyword filters ────────────────────────────────────────────────────
# A market must match at least one asset keyword AND at least one timeframe keyword
ASSET_KEYWORDS: list[str] = ["BTC", "ETH", "Bitcoin", "Ethereum"]
TIMEFRAME_KEYWORDS: list[str] = ["5 min", "5-min", "15 min", "15-min", "5minute", "15minute"]

# ── Retry policy ─────────────────────────────────────────────────────────────
MAX_RETRIES: int = 3
RETRY_DELAY_SECONDS: float = 5.0

# ── Heartbeat ────────────────────────────────────────────────────────────────
HEARTBEAT_INTERVAL_SECONDS: int = 60
