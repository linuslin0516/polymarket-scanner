"""
scanner.py - Core arbitrage detection logic (READ-ONLY).

Flow per scan cycle:
  1. Query Polymarket Gamma API for live Up/Down crypto markets by keyword
  2. For each market, fetch the best ask for the UP (yes) and DOWN (no) tokens
     from the CLOB order-book endpoint
  3. Compute total_cost = up_ask + down_ask
  4. If total_cost < ARB_THRESHOLD emit an Opportunity via the logger

Why Gamma API for discovery, CLOB API for prices:
  - The CLOB /markets endpoint returns 35,000+ markets without useful filtering.
    The 5-min/15-min crypto markets are buried somewhere in that list.
  - The Gamma API (used by the Polymarket frontend) supports keyword search and
    returns fully-populated market objects including token IDs.
  - The CLOB /book endpoint is still the correct source for live order-book data.

Why reload markets every cycle:
  - 5-min and 15-min markets roll over continuously. Each new time window is a
    completely new market with new condition_id and token IDs. Caching the list
    at startup means we'd be querying stale/expired markets within minutes.

No order placement takes place here. Every spot where execution would happen
is marked with:  # TODO: place_order() here
"""

import asyncio
from datetime import datetime
from typing import Any

import aiohttp

import config
from logger import (
    Opportunity,
    log_opportunity,
    log_info,
    log_warning,
    log_error,
)


# ── Types ─────────────────────────────────────────────────────────────────────

MarketDict = dict[str, Any]

# Gamma API base URL (market discovery)
GAMMA_API_URL = "https://gamma-api.polymarket.com"


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


# ── Market discovery via Gamma API ────────────────────────────────────────────

async def fetch_all_markets(session: aiohttp.ClientSession) -> list[MarketDict]:
    """
    Fetch active markets from the Gamma API and filter by keyword client-side.

    Why no tag_slug:
      tag_slug=crypto returns unrelated markets (Trump deportation, GTA 6 price,
      etc.) — apparently that tag is broader than expected.  Fetching a large
      batch of all active markets and matching by title text is more reliable.

    Why limit=500:
      The Up/Down 5-min and 15-min markets roll over every few minutes, so they
      are always among the most recently active markets.  A batch of 500 active
      markets ordered by default (recency) reliably includes all open windows.
    """
    log_info("Fetching active markets from Gamma API (no tag filter)…")
    data = await _get_json(
        session,
        f"{GAMMA_API_URL}/markets",
        params={
            "active": "true",
            "closed": "false",
            "limit":  "500",
        },
    )

    if data is None:
        return []

    raw: list[MarketDict] = data if isinstance(data, list) else data.get("data", [])
    log_info(f"Gamma API returned {len(raw)} active markets (before keyword filter).")

    # Debug: show first 5 raw titles so we can verify ordering/content
    for m in raw[:5]:
        log_info(f"  RAW | {m.get('question','?')[:80]}")

    # Client-side keyword filter — exact substring match on question title
    matched = [
        m for m in raw
        if any(kw.lower() in (m.get("question") or "").lower()
               for kw in config.MARKET_KEYWORDS)
    ]
    log_info(f"After keyword filter: {len(matched)} Up/Down markets.")
    return matched


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

    # tokens — Gamma may provide clobTokenIds + outcomes as parallel arrays
    if not market.get("tokens") or not any(
        t.get("token_id") for t in market.get("tokens", [])
    ):
        clob_ids: list[str] = market.get("clobTokenIds") or []
        outcomes: list[str] = market.get("outcomes") or ["Up", "Down"]
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


# ── Order-book price fetching ─────────────────────────────────────────────────

async def _fetch_best_ask(
    session: aiohttp.ClientSession,
    token_id: str,
) -> float | None:
    """
    Query the CLOB order book for *token_id* and return the best (lowest) ask.

    Endpoint: GET /book?token_id=<token_id>
    Response shape:
      {
        "market": "0x...",
        "asset_id": "...",
        "bids": [{"price": "0.53", "size": "100"}, ...],
        "asks": [{"price": "0.48", "size": "200"}, ...]   ← sorted ascending
      }

    Returns None if the book is empty or the request fails.
    """
    # Debug: log the token_id being queried (first 20 chars)
    log_info(f"    /book query token_id={token_id[:20]}…")
    data = await _get_json(
        session,
        f"{config.POLYMARKET_API_URL}/book",
        params={"token_id": token_id},
    )
    if data is None:
        return None

    asks: list[dict] = data.get("asks", [])
    if not asks:
        return None

    try:
        return float(asks[0]["price"])
    except (KeyError, ValueError, IndexError):
        return None


async def _fetch_token_prices(
    session: aiohttp.ClientSession,
    market: MarketDict,
) -> tuple[float | None, float | None]:
    """
    Concurrently fetch the best ask for the UP (yes) and DOWN (no) tokens.

    Handles both outcome label conventions:
      - Classic binary:  "Yes" / "No"
      - Up/Down markets: "Up"  / "Down"

    Falls back to positional assignment (index 0 = yes/up, index 1 = no/down)
    if no recognisable label is found.

    Returns (up_ask, down_ask). Either value is None if unavailable.
    """
    tokens: list[dict] = market.get("tokens", [])
    if len(tokens) < 2:
        return None, None

    up_id: str | None = None
    down_id: str | None = None

    for tok in tokens:
        outcome = (tok.get("outcome") or "").lower()
        if outcome in ("yes", "up"):
            up_id = tok.get("token_id")
        elif outcome in ("no", "down"):
            down_id = tok.get("token_id")

    # Positional fallback
    if not up_id or not down_id:
        up_id = tokens[0].get("token_id")
        down_id = tokens[1].get("token_id")

    if not up_id or not down_id:
        return None, None

    up_ask, down_ask = await asyncio.gather(
        _fetch_best_ask(session, up_id),
        _fetch_best_ask(session, down_id),
    )
    return up_ask, down_ask


# ── Arbitrage detection ───────────────────────────────────────────────────────

def _check_arb(
    market: MarketDict,
    up_ask: float,
    down_ask: float,
    timestamp: str,
) -> Opportunity | None:
    """
    In a fair binary market UP_ask + DOWN_ask ≈ 1.00.
    If the sum is below ARB_THRESHOLD the buyer locks in a risk-free profit
    equal to (1.00 - total_cost) per dollar deployed.

    Returns an Opportunity dataclass if the condition is met, else None.
    """
    total_cost = up_ask + down_ask
    if total_cost >= config.ARB_THRESHOLD:
        return None

    edge_pct = (1.0 - total_cost) * 100

    return Opportunity(
        timestamp=timestamp,
        market_id=market.get("condition_id") or market.get("id") or "unknown",
        market_title=market.get("question") or "Unknown Market",
        yes_ask=round(up_ask, 4),
        no_ask=round(down_ask, 4),
        total_cost=round(total_cost, 4),
        edge_pct=round(edge_pct, 4),
    )


# ── Single scan cycle ─────────────────────────────────────────────────────────

async def scan_once(
    session: aiohttp.ClientSession,
    markets: list[MarketDict],
) -> list[Opportunity]:
    """
    Scan every market in *markets* for arbitrage in a single pass.
    Fetches all order books concurrently for low latency.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    opportunities: list[Opportunity] = []

    async def _process(market: MarketDict) -> Opportunity | None:
        title = market.get("question") or "?"
        try:
            up_ask, down_ask = await _fetch_token_prices(session, market)
            if up_ask is None or down_ask is None:
                return None
            return _check_arb(market, up_ask, down_ask, timestamp)
        except Exception as exc:
            log_warning(f"Error processing '{title[:50]}': {exc}")
            return None

    results = await asyncio.gather(*(_process(m) for m in markets))

    for result in results:
        if result is not None:
            log_opportunity(result)
            opportunities.append(result)
            # TODO: place_order() here — submit UP and DOWN legs to capture the edge

    return opportunities


# ── Market refresh ────────────────────────────────────────────────────────────

async def load_and_filter_markets(session: aiohttp.ClientSession) -> list[MarketDict]:
    """
    Fetch and filter markets from the Gamma API.

    Called at startup AND on every scan cycle because 5-min/15-min markets
    roll over continuously — each new window has new token IDs.
    """
    all_markets = await fetch_all_markets(session)
    return filter_markets(all_markets)
