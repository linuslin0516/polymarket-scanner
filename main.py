"""
main.py - Entry point for the Polymarket arbitrage scanner.

Responsibilities:
  - Load configuration from .env
  - Create a shared aiohttp session for connection pooling
  - Run the async scanner loop (fetch → filter → scan → sleep)
  - Emit a heartbeat every HEARTBEAT_INTERVAL_SECONDS
  - Handle KeyboardInterrupt cleanly and print a session summary on exit

Usage:
  python main.py
"""

import asyncio
import time

import aiohttp

import config
import scanner
from logger import log_info, log_error, log_heartbeat


# ── Main loop ─────────────────────────────────────────────────────────────────

async def run() -> None:
    """
    Outer async loop.

    Market list is refreshed at startup only.  If you want to catch newly
    created markets, you could reload them every N cycles — add logic here.
    """
    log_info("=" * 60)
    log_info("Polymarket Arbitrage Scanner  |  READ-ONLY mode")
    log_info(f"API URL       : {config.POLYMARKET_API_URL}")
    log_info(f"Scan interval : {config.SCAN_INTERVAL_SECONDS}s")
    log_info(f"Arb threshold : {config.ARB_THRESHOLD}")
    log_info(f"Min liquidity : ${config.MIN_LIQUIDITY:,.0f}")
    log_info(f"Log file      : {config.LOG_FILE}")
    log_info("=" * 60)

    session_start = time.monotonic()
    total_opportunities = 0
    best_edge_pct = 0.0
    last_heartbeat = time.monotonic()

    # Use a single aiohttp session for the entire run (connection pooling)
    connector = aiohttp.TCPConnector(limit=20)  # max 20 concurrent connections
    async with aiohttp.ClientSession(connector=connector) as session:

        # ── One-time market discovery ──────────────────────────────────────
        log_info("Fetching and filtering markets…")
        markets = await scanner.load_and_filter_markets(session)

        if not markets:
            log_error(
                "No matching markets found.  Check your keyword filters "
                "or increase SCAN_INTERVAL_SECONDS and try again."
            )
            return

        log_info(f"Watching {len(markets)} markets.  Starting scan loop…\n")

        # ── Continuous scan loop ───────────────────────────────────────────
        while True:
            cycle_start = time.monotonic()

            # Run one full scan pass
            try:
                found = await scanner.scan_once(session, markets)
            except Exception as exc:
                # Safeguard: the main loop must never crash
                log_error(f"Unexpected error in scan cycle: {exc}")
                found = []

            # Track session-level statistics
            total_opportunities += len(found)
            for opp in found:
                if opp.edge_pct > best_edge_pct:
                    best_edge_pct = opp.edge_pct

            # ── Heartbeat ──────────────────────────────────────────────────
            now = time.monotonic()
            if now - last_heartbeat >= config.HEARTBEAT_INTERVAL_SECONDS:
                log_heartbeat(len(markets), total_opportunities)
                last_heartbeat = now

            # ── Sleep for the remainder of the scan interval ───────────────
            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0.0, config.SCAN_INTERVAL_SECONDS - elapsed)
            await asyncio.sleep(sleep_for)


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    """Wrap the async run() with a clean KeyboardInterrupt handler."""
    start_wall = time.time()

    try:
        asyncio.run(run())

    except KeyboardInterrupt:
        pass  # handled below

    finally:
        elapsed_sec = int(time.time() - start_wall)
        hours, remainder = divmod(elapsed_sec, 3600)
        minutes, seconds = divmod(remainder, 60)
        runtime_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        print()
        log_info("Scanner stopped by user.")
        log_info("─" * 40)
        log_info(f"Total runtime           : {runtime_str}")
        # Note: opportunity counts are local to run(); for a one-liner summary
        # we re-count from the CSV if it exists, otherwise just note the file.
        log_info(f"Opportunities logged to : {config.LOG_FILE}")
        log_info("─" * 40)


if __name__ == "__main__":
    main()
