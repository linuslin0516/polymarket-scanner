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
# Actual Polymarket title format (confirmed):
#   "Bitcoin Up or Down - February 24, 12:10AM-12:15AM ET"  (15-min)
#   "Ethereum Up or Down - February 23, 3:15PM-3:30PM ET"   (15-min)
#   "Bitcoin Up or Down - February 24, 12:05AM-12:10AM ET"  (5-min)
# A market matches if its title contains ANY of these strings (case-insensitive).
MARKET_KEYWORDS: list[str] = [
    "Bitcoin Up or Down",
    "Ethereum Up or Down",
    "Solana Up or Down",
    "XRP Up or Down",
]

# NOTE: 15-min and 5-min crypto markets on Polymarket charge taker fees
# (market orders). Maker orders (limit orders) are free and earn rebates.
# Always use LIMIT orders when executing to avoid fees eating the arb edge.
# TODO: place_order() here — use limit orders only

# ── Retry policy ─────────────────────────────────────────────────────────────
MAX_RETRIES: int = 3
RETRY_DELAY_SECONDS: float = 5.0

# ── Heartbeat ────────────────────────────────────────────────────────────────
HEARTBEAT_INTERVAL_SECONDS: int = 60
