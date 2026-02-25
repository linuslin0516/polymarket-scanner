"""
scanner.py - Market discovery logic (READ-ONLY).

Responsibilities:
  - Generate Gamma API slugs for current/upcoming 15-min and 5-min windows
  - Fetch each slug concurrently to discover active Up/Down markets
  - Normalise Gamma API fields to a consistent shape for consumption by main.py

Price fetching and arb detection live in main.py (WebSocket-based).
No order placement takes place here. Every spot where execution would happen
is marked with:  # TODO: place_order() here
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta  # datetime used in _get_window_timestamps
from typing import Any

import aiohttp

import config
from logger import (
    log_info,
    log_warning,
    log_error,
)


# ── Types ─────────────────────────────────────────────────────────────────────

MarketDict = dict[str, Any]

# Gamma API base URL (market discovery)
GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Cryptos that have Up/Down markets on Polymarket
_CRYPTOS_15M = ["btc", "eth", "sol", "xrp"]
_CRYPTOS_5M  = ["btc", "eth"]          # 5-min only available for BTC and ETH
_WINDOW_LOOKAHEAD = 2                  # current window + 2 upcoming windows


# ── HTTP helper ───────────────────────────────────────────────────────────────

async def _get_json(
    session: aiohttp.ClientSession,
    url: str,
    params: dict | None = None,
) -> Any:
    """
    GET *url* and return parsed JSON.
    Retries up to MAX_RETRIES times on network/HTTP errors.
    Returns None if all retries are exhausted.
    """
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                log_warning(
                    f"HTTP {resp.status} from {url} "
                    f"(attempt {attempt}/{config.MAX_RETRIES})"
                )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log_warning(
                f"Request error [{type(exc).__name__}] for {url} "
                f"(attempt {attempt}/{config.MAX_RETRIES})"
            )

        if attempt < config.MAX_RETRIES:
            await asyncio.sleep(config.RETRY_DELAY_SECONDS)

    log_error(f"All {config.MAX_RETRIES} retries failed for {url} — skipping.")
    return None


# ── Market discovery via slug generation ──────────────────────────────────────

def _get_window_timestamps(interval_minutes: int) -> list[int]:
    """
    Return UTC unix timestamps for the current window and the next
    _WINDOW_LOOKAHEAD windows of the given interval.

    Example for interval_minutes=15 at 14:37 UTC:
      → [1740441600 (14:30), 1740442500 (14:45), 1740443400 (15:00)]
    """
    now = datetime.now(timezone.utc)
    floored_minute = (now.minute // interval_minutes) * interval_minutes
    current = now.replace(minute=floored_minute, second=0, microsecond=0)
    return [
        int((current + timedelta(minutes=interval_minutes * i)).timestamp())
        for i in range(_WINDOW_LOOKAHEAD + 1)
    ]


async def _fetch_market_by_slug(
    session: aiohttp.ClientSession,
    slug: str,
) -> MarketDict | None:
    """
    Fetch a specific market by its Gamma API slug.
    Returns the market dict if found and active, else None.
    """
    data = await _get_json(
        session,
        f"{GAMMA_API_URL}/markets",
        params={"slug": slug},
    )
    if isinstance(data, list) and len(data) > 0:
        return data[0]
    return None


async def discover_updown_markets(
    session: aiohttp.ClientSession,
) -> list[MarketDict]:
    """
    Find all currently active Up/Down markets by constructing slugs for the
    current and next _WINDOW_LOOKAHEAD time windows.

    Slug format:
      {crypto}-updown-15m-{unix_start_timestamp}   e.g. btc-updown-15m-1740441600
      {crypto}-updown-5m-{unix_start_timestamp}    e.g. eth-updown-5m-1740441900

    All slug fetches are run concurrently for low latency.
    """
    # Build (slug, label) pairs for all windows and cryptos
    slugs: list[tuple[str, str]] = []
    for ts in _get_window_timestamps(15):
        for crypto in _CRYPTOS_15M:
            slugs.append((f"{crypto}-updown-15m-{ts}", f"{crypto.upper()} 15m"))
    for ts in _get_window_timestamps(5):
        for crypto in _CRYPTOS_5M:
            slugs.append((f"{crypto}-updown-5m-{ts}", f"{crypto.upper()} 5m"))

    total = len(slugs)
    log_info(f"Trying {total} slugs ({len(_CRYPTOS_15M)} cryptos × {_WINDOW_LOOKAHEAD+1} 15m windows"
             f" + {len(_CRYPTOS_5M)} cryptos × {_WINDOW_LOOKAHEAD+1} 5m windows)…")

    async def _try(slug: str, label: str) -> MarketDict | None:
        log_info(f"  Trying slug: {slug}")
        market = await _fetch_market_by_slug(session, slug)
        if market and market.get("active"):
            log_info(f"  Found {label} market: {slug}")
            return market
        return None

    results = await asyncio.gather(*(_try(slug, label) for slug, label in slugs))
    found = [m for m in results if m is not None]
    log_info(f"Discovered {len(found)} active Up/Down markets.")
    return found


# ── Market normalisation ──────────────────────────────────────────────────────

def _normalise_market(market: MarketDict) -> MarketDict:
    """
    The Gamma API uses camelCase field names (conditionId, endDate, clobTokenIds)
    while the CLOB API uses snake_case (condition_id, end_date_iso, tokens).
    Normalise to a consistent snake_case shape so the rest of the code works
    with either source.

    Token structure after normalisation:
      tokens: [
        {"token_id": "...", "outcome": "up"},
        {"token_id": "...", "outcome": "down"},
      ]
    """
    # condition_id
    if not market.get("condition_id") and market.get("conditionId"):
        market["condition_id"] = market["conditionId"]

    # question (Gamma uses "question" too, but fall back to "title")
    if not market.get("question") and market.get("title"):
        market["question"] = market["title"]

    # end_date_iso
    if not market.get("end_date_iso") and market.get("endDate"):
        market["end_date_iso"] = market["endDate"]

    # tokens — Gamma may provide clobTokenIds + outcomes as parallel arrays.
    # The Gamma API returns these fields as JSON-encoded strings, not Python
    # lists, so parse them with json.loads() when needed.
    if not market.get("tokens") or not any(
        t.get("token_id") for t in market.get("tokens", [])
    ):
        raw_ids = market.get("clobTokenIds") or []
        clob_ids: list[str] = json.loads(raw_ids) if isinstance(raw_ids, str) else list(raw_ids)

        raw_outcomes = market.get("outcomes") or ["Up", "Down"]
        outcomes: list[str] = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else list(raw_outcomes)

        if len(clob_ids) >= 2:
            market["tokens"] = [
                {"token_id": clob_ids[0], "outcome": outcomes[0]},
                {"token_id": clob_ids[1], "outcome": outcomes[1]},
            ]

    return market


def filter_markets(markets: list[MarketDict]) -> list[MarketDict]:
    """Normalise fields and keep only markets with valid token IDs."""
    result: list[MarketDict] = []
    for m in markets:
        m = _normalise_market(m)
        tokens: list[dict] = m.get("tokens", [])
        if len(tokens) < 2 or not all(t.get("token_id") for t in tokens):
            log_warning(
                f"Skipping '{m.get('question','?')[:60]}' — missing token IDs"
            )
            continue
        result.append(m)

    log_info(f"--- Watching {len(result)} markets ---")
    for m in result:
        liq = float(m.get("liquidity") or 0)
        log_info(f"  WATCH | ${liq:,.0f} | {m.get('question','?')}")
    log_info("--- End market list ---")
    return result


# ── Market refresh ────────────────────────────────────────────────────────────

async def load_and_filter_markets(session: aiohttp.ClientSession) -> list[MarketDict]:
    """
    Discover Up/Down markets by slug and normalise/validate them.

    Called at startup AND on every scan cycle because 5-min/15-min markets
    roll over continuously — each new window has new token IDs.
    """
    all_markets = await discover_updown_markets(session)
    return filter_markets(all_markets)
