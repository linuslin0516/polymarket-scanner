"""
main.py - WebSocket-based Polymarket arbitrage scanner.

Architecture:
  discovery_loop()  REST, every 60s
    Find new markets by slug, rebuild the subscription list.
    Signals ws_loop() to reconnect when the list changes.

  ws_loop()         WebSocket, persistent connection
    Subscribe to CLOB orderbook for all active markets.
    On every price update: check if YES_ask + NO_ask < ARB_THRESHOLD.
    Log opportunity with millisecond-precision timestamp and latency.

Why WebSocket instead of REST polling:
  Arb windows last 2–30 seconds. REST polling at 10 s intervals misses most.
  WebSocket delivers each price-level change in milliseconds → near-zero lag.

Orderbook state tracked locally:
  _ask_books[token_id] = {price_str: size_float}
  Updated on "book" (snapshot) and "price_change" (incremental) events.
  Best ask = min price where size > 0.

No order placement takes place here. Every spot where execution would happen
is marked with:  # TODO: place_order() here
"""

import asyncio
import json
import time
from datetime import datetime

import aiohttp
import websockets

import config
import scanner
from scanner import MarketDict
from logger import (
    Opportunity,
    log_info,
    log_warning,
    log_error,
    log_heartbeat,
    log_opportunity,
)

CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/"

# ── Shared state (safe: asyncio is single-threaded) ───────────────────────────

_active_markets: list[MarketDict] = []
_markets_updated = asyncio.Event()

# token_id → (condition_id, "up" | "down")
_token_to_market: dict[str, tuple[str, str]] = {}

# condition_id → (up_token_id, down_token_id, market_title)
_market_pairs: dict[str, tuple[str, str, str]] = {}

# Local orderbook: token_id → {price_str: size_float}
_ask_books: dict[str, dict[str, float]] = {}

# Dedup: suppress repeated alerts for the same market within this window
_last_opp_at: dict[str, float] = {}
_OPP_COOLDOWN = 10.0  # seconds

_total_opportunities = 0

# Log first N raw WS messages at startup so we can verify the format
_DEBUG_WS_REMAINING = 5


# ── Market-table helpers ───────────────────────────────────────────────────────

def _update_market_tables(markets: list[MarketDict]) -> None:
    """Rebuild token-lookup dicts from a freshly discovered market list."""
    global _token_to_market, _market_pairs
    _token_to_market = {}
    _market_pairs = {}
    for m in markets:
        cid = m.get("condition_id") or m.get("id") or ""
        if not cid:
            continue
        title = m.get("question") or "Unknown"
        tokens = m.get("tokens", [])
        if len(tokens) < 2:
            continue
        up_id = next(
            (t["token_id"] for t in tokens if t.get("outcome", "").lower() in ("up", "yes")),
            tokens[0]["token_id"],
        )
        down_id = next(
            (t["token_id"] for t in tokens if t.get("outcome", "").lower() in ("down", "no")),
            tokens[1]["token_id"],
        )
        _market_pairs[cid] = (up_id, down_id, title)
        _token_to_market[up_id] = (cid, "up")
        _token_to_market[down_id] = (cid, "down")


# ── Orderbook helpers ──────────────────────────────────────────────────────────

def _best_ask(token_id: str) -> float | None:
    """Return the lowest active ask price for token_id from our tracked book."""
    book = _ask_books.get(token_id, {})
    prices = [float(p) for p, s in book.items() if s > 0]
    return min(prices) if prices else None


def _check_arb(token_id: str, recv_ms: float) -> None:
    """
    After any book update for token_id, check if its market has an arb gap.
    Logs the opportunity and records latency from WS message receipt to log.
    """
    global _total_opportunities

    if token_id not in _token_to_market:
        return
    cid, _ = _token_to_market[token_id]
    if cid not in _market_pairs:
        return

    up_id, down_id, title = _market_pairs[cid]
    up_ask = _best_ask(up_id)
    down_ask = _best_ask(down_id)
    if up_ask is None or down_ask is None:
        return

    total_cost = up_ask + down_ask
    if total_cost >= config.ARB_THRESHOLD:
        return

    # Suppress repeated alerts for the same market within cooldown window
    now = time.time()
    if now - _last_opp_at.get(cid, 0) < _OPP_COOLDOWN:
        return
    _last_opp_at[cid] = now

    edge_pct = (1.0 - total_cost) * 100
    latency_ms = int(now * 1000 - recv_ms)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # millisecond precision

    opp = Opportunity(
        timestamp=ts,
        market_id=cid,
        market_title=title,
        yes_ask=round(up_ask, 4),
        no_ask=round(down_ask, 4),
        total_cost=round(total_cost, 4),
        edge_pct=round(edge_pct, 4),
    )
    log_opportunity(opp)
    log_info(f"  detect latency: {latency_ms}ms from WS message to log")
    _total_opportunities += 1
    # TODO: place_order() here — submit UP and DOWN legs to capture the edge


# ── WebSocket event handlers ───────────────────────────────────────────────────

def _handle_book(event: dict, recv_ms: float) -> None:
    """
    Full orderbook snapshot (event_type = "book").
    Reset our tracked ask book for this token and recompute.

    Polymarket CLOB WS uses "sells" for asks and "buys" for bids.
    """
    token_id = event.get("asset_id") or ""
    if not token_id:
        return

    sells = event.get("sells") or event.get("asks") or []
    new_book: dict[str, float] = {}
    for entry in sells:
        try:
            new_book[str(entry["price"])] = float(entry["size"])
        except (KeyError, ValueError):
            pass

    _ask_books[token_id] = new_book
    _check_arb(token_id, recv_ms)


def _handle_price_change(event: dict, recv_ms: float) -> None:
    """
    Incremental orderbook update (event_type = "price_change").
    Apply delta changes to our tracked ask book and recompute.

    Each change: {"price": "0.49", "side": "SELL", "size": "50.0"}
    size == 0 means that price level was removed from the book.
    """
    token_id = event.get("asset_id") or ""
    if not token_id:
        return

    book = _ask_books.setdefault(token_id, {})
    for change in event.get("changes") or []:
        try:
            side = (change.get("side") or "").upper()
            price = str(change["price"])
            size = float(change["size"])
            if side in ("SELL", "ASK", ""):
                if size == 0:
                    book.pop(price, None)
                else:
                    book[price] = size
        except (KeyError, ValueError):
            pass

    _check_arb(token_id, recv_ms)


# ── Discovery loop ─────────────────────────────────────────────────────────────

async def discovery_loop(session: aiohttp.ClientSession) -> None:
    """
    Refresh market list every 60 s.
    Signals ws_loop() to reconnect when the condition_id set changes
    (new 5-min/15-min windows roll in, old ones expire).
    """
    global _active_markets
    INTERVAL = 60

    while True:
        await asyncio.sleep(INTERVAL)
        try:
            markets = await scanner.load_and_filter_markets(session)
            if not markets:
                continue
            new_ids = {m.get("condition_id") or "" for m in markets}
            old_ids = {m.get("condition_id") or "" for m in _active_markets}
            if new_ids != old_ids:
                log_info(
                    f"Discovery: market list changed "
                    f"({len(markets)} active) — WS will resubscribe."
                )
                _active_markets = markets
                _update_market_tables(markets)
                _markets_updated.set()
        except Exception as exc:
            log_error(f"Discovery error: {exc}")


# ── WebSocket loop ─────────────────────────────────────────────────────────────

async def ws_loop() -> None:
    """
    Maintain a persistent WebSocket connection to the Polymarket CLOB.
    Reconnects automatically when:
      - The market list is updated by discovery_loop()
      - The server closes the connection
      - Any network error occurs
    """
    global _DEBUG_WS_REMAINING

    while True:
        if not _market_pairs:
            log_info("WS: no markets yet, waiting for discovery…")
            await asyncio.sleep(5)
            continue

        condition_ids = list(_market_pairs.keys())
        _markets_updated.clear()
        _ask_books.clear()  # stale prices on reconnect

        log_info(f"WS: connecting to {CLOB_WS} …")
        try:
            async with websockets.connect(
                CLOB_WS,
                ping_interval=20,
                ping_timeout=10,
                open_timeout=15,
            ) as ws:
                await ws.send(json.dumps({
                    "auth": {},
                    "type": "market",
                    "markets": condition_ids,
                }))
                log_info(f"WS: subscribed to {len(condition_ids)} markets. Listening…")

                async def _listen() -> None:
                    async for raw in ws:
                        recv_ms = time.time() * 1000

                        # Debug: log first few raw messages to verify format
                        if _DEBUG_WS_REMAINING > 0:
                            log_info(f"  WS RAW: {raw[:300]}")
                            _DEBUG_WS_REMAINING -= 1  # type: ignore[assignment]

                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        events = data if isinstance(data, list) else [data]
                        for event in events:
                            etype = (
                                event.get("event_type") or event.get("type") or ""
                            ).lower()
                            if etype == "book":
                                _handle_book(event, recv_ms)
                            elif etype == "price_change":
                                _handle_price_change(event, recv_ms)

                # Race: listen until either the market list changes or connection drops
                listen_task = asyncio.create_task(_listen())
                update_task = asyncio.create_task(_markets_updated.wait())

                done, pending = await asyncio.wait(
                    [listen_task, update_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
                    try:
                        await t
                    except (asyncio.CancelledError, Exception):
                        pass

                if update_task in done:
                    log_info("WS: market list updated — reconnecting with new subscriptions…")
                else:
                    log_warning("WS: connection closed — reconnecting in 3 s…")
                    await asyncio.sleep(3)

        except Exception as exc:
            log_warning(f"WS error [{type(exc).__name__}]: {exc} — retrying in 5 s…")
            await asyncio.sleep(5)


# ── Entry point ────────────────────────────────────────────────────────────────

async def run() -> None:
    global _active_markets

    log_info("=" * 60)
    log_info("Polymarket Arbitrage Scanner  |  READ-ONLY  |  WebSocket")
    log_info(f"WS endpoint : {CLOB_WS}")
    log_info(f"Arb thresh  : {config.ARB_THRESHOLD}")
    log_info(f"Min liq     : ${config.MIN_LIQUIDITY:,.0f}")
    log_info(f"Log file    : {config.LOG_FILE}")
    log_info("=" * 60)

    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:

        # Blocking initial discovery so WS has markets to subscribe to immediately
        log_info("Running initial market discovery…")
        try:
            markets = await scanner.load_and_filter_markets(session)
            if markets:
                _active_markets = markets
                _update_market_tables(markets)
                log_info(f"Initial discovery: {len(markets)} markets ready.")
            else:
                log_warning("No markets on first discovery — WS will wait.")
        except Exception as exc:
            log_error(f"Initial discovery failed: {exc}")

        # Launch background tasks
        disc_task = asyncio.create_task(discovery_loop(session))
        ws_task = asyncio.create_task(ws_loop())

        try:
            while True:
                await asyncio.sleep(config.HEARTBEAT_INTERVAL_SECONDS)
                log_heartbeat(len(_active_markets), _total_opportunities)
        except asyncio.CancelledError:
            pass
        finally:
            disc_task.cancel()
            ws_task.cancel()


def main() -> None:
    import time as _time

    start_wall = _time.time()
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
    finally:
        elapsed = int(_time.time() - start_wall)
        h, r = divmod(elapsed, 3600)
        m, s = divmod(r, 60)
        log_info("Scanner stopped.")
        log_info(f"Runtime         : {h:02d}:{m:02d}:{s:02d}")
        log_info(f"Opportunities   : {_total_opportunities}")
        log_info(f"Log file        : {config.LOG_FILE}")


if __name__ == "__main__":
    main()
