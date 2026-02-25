"""
logger.py - Console + CSV logging for arbitrage opportunities and heartbeats.

Responsibilities:
  - Pretty-print arb alerts to stdout
  - Append each opportunity row to a CSV file (creates header on first write)
  - Emit periodic heartbeat lines so the operator knows the scanner is alive

Railway note:
  Railway's filesystem is ephemeral — files are wiped on every redeploy or
  restart.  When the RAILWAY_ENVIRONMENT env var is present we skip writing
  the CSV file and instead emit every opportunity as a structured CSV line
  to stdout (prefixed with "CSV|") so you can capture it via Railway's log
  drain / log export instead.
"""

import csv
import io
import os
from datetime import datetime
from dataclasses import dataclass, fields, astuple

from config import LOG_FILE

# True when running inside a Railway deployment
_ON_RAILWAY: bool = bool(os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_SERVICE_NAME"))


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Opportunity:
    """One detected arbitrage opportunity."""
    timestamp: str
    market_id: str
    market_title: str
    yes_ask: float
    no_ask: float
    total_cost: float
    edge_pct: float


# ── Internal helpers ──────────────────────────────────────────────────────────

_CSV_HEADERS = [f.name for f in fields(Opportunity)]
_csv_initialized = False  # write header only once per process run


def _ensure_csv_header() -> None:
    """Write the CSV header row if the file doesn't exist yet."""
    global _csv_initialized
    if _csv_initialized:
        return
    write_header = not os.path.exists(LOG_FILE) or os.path.getsize(LOG_FILE) == 0
    if write_header:
        with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(_CSV_HEADERS)
    _csv_initialized = True


# ── Public API ────────────────────────────────────────────────────────────────

def log_opportunity(opp: Opportunity) -> None:
    """
    Print a formatted alert to stdout and append the row to the CSV log.

    Example console output:
      [2026-02-24 14:32:11] ARB FOUND | Market: BTC 15-min UP | YES: 0.47 | NO: 0.49 | Total: 0.96 | Edge: 4.00%
    """
    # ── Console ──
    print(
        f"[{opp.timestamp}] ARB FOUND | "
        f"Market: {opp.market_title} | "
        f"YES: {opp.yes_ask:.4f} | "
        f"NO: {opp.no_ask:.4f} | "
        f"Total: {opp.total_cost:.4f} | "
        f"Edge: {opp.edge_pct:.2f}%"
    )

    # ── CSV ──
    if _ON_RAILWAY:
        # Ephemeral filesystem: emit a structured CSV line to stdout instead.
        # Capture with Railway's log drain or just grep "^CSV|" from the logs.
        buf = io.StringIO()
        csv.writer(buf).writerow(astuple(opp))
        print(f"CSV|{buf.getvalue().rstrip()}")
    else:
        _ensure_csv_header()
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(astuple(opp))


def log_heartbeat(markets_monitored: int, opportunities_found: int) -> None:
    """
    Print a status line so the operator can confirm the scanner is alive.

    Example:
      [2026-02-24 14:33:11] Scanning... 8 markets monitored, 3 opportunities found so far
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"[{ts}] Scanning... "
        f"{markets_monitored} markets monitored, "
        f"{opportunities_found} opportunities found so far"
    )


def log_info(message: str) -> None:
    """Generic timestamped info line (startup, shutdown, retries, etc.)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] INFO | {message}")


def log_warning(message: str) -> None:
    """Timestamped warning line (skipped markets, retries, etc.)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] WARN | {message}")


def log_error(message: str) -> None:
    """Timestamped error line (API failures, unexpected exceptions)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] ERROR | {message}")
