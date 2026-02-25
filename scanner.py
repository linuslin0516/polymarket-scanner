"""
scanner.py - Core arbitrage detection logic (READ-ONLY).

Flow per scan cycle:
  1. Fetch all active markets from the Polymarket CLOB REST API
  2. Filter for short-term BTC/ETH binary markets by keyword matching
  3. For each filtered market, fetch the best YES and NO ask prices from
     the order-book endpoint
  4. Compute total_cost = yes_ask + no_ask
  5. If total_cost < ARB_THRESHOLD emit an Opportunity via the logger

No order placement takes place here.  Every spot where execution would
happen is marked with:  # TODO: place_order() here
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

# Raw dict returned by the CLOB /markets endpoint
MarketDict = dict[str, Any]


# ── Polymarket CLOB REST helpers ──────────────────────────────────────────────

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
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    return await resp.json()
                log_warning(f"HTTP {resp.status} from {url} (attempt {attempt}/{config.MAX_RETRIES})")
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log_warning(f"Request error [{type(exc).__name__}] for {url} (attempt {attempt}/{config.MAX_RETRIES})")

        if attempt < config.MAX_RETRIES:
            await asyncio.sleep(config.RETRY_DELAY_SECONDS)

    log_error(f"All {config.MAX_RETRIES} retries failed for {url} — skipping.")
    return None


async def fetch_all_markets(session: aiohttp.ClientSession) -> list[MarketDict]:
    """
    Retrieve active markets from the Polymarket CLOB API.

    Key design decisions:
    - active=true  filters out expired/resolved markets server-side,
      cutting the result set from 28,000+ down to ~200-500
    - Sequential pagination (no concurrent fetches) keeps ordering clean
    - MAX_PAGES = 10 is a safety cap; active markets fit in far fewer pages
    """
    MAX_PAGES = 10
    markets: list[MarketDict] = []
    cursor: str | None = None  # None = first page, no cursor param needed
    page = 0

    while page < MAX_PAGES:
        # Always filter for active markets — avoids pulling 28k+ historical entries
        params: dict[str, str] = {"active": "true"}
        if cursor:
            params["next_cursor"] = cursor

        log_info(f"Fetching markets page {page + 1}" + (f" (cursor={cursor[:12]}…)" if cursor else " (first page)"))
        data = await _get_json(session, f"{config.POLYMARKET_API_URL}/markets", params=params)
        if data is None:
            break  # network failure already logged

        batch: list[MarketDict] = data.get("data", [])
        markets.extend(batch)
        page += 1

        next_cursor: str = data.get("next_cursor", "") or ""
        log_info(f"  → got {len(batch)} markets (total so far: {len(markets)}), next_cursor={next_cursor[:16]!r}")

        # Empty string or "LTE=" = end of data
        if not next_cursor or next_cursor == "LTE=" or len(batch) == 0:
            break

        cursor = next_cursor

    log_info(f"Fetched {len(markets)} active markets across {page} page(s).")
    return markets


# ── Market filtering ──────────────────────────────────────────────────────────

def _matches_keywords(market: MarketDict) -> bool:
    """
    Return True if this market's question/title contains:
      - at least one ASSET keyword  (BTC, ETH, Bitcoin, Ethereum)
      - at least one TIMEFRAME keyword  (5 min, 15 min, …)

    Case-insensitive.
    """
    title: str = (market.get("question") or market.get("description") or "").lower()

    has_asset = any(kw.lower() in title for kw in config.ASSET_KEYWORDS)
    has_timeframe = any(kw.lower() in title for kw in config.TIMEFRAME_KEYWORDS)

    return has_asset and has_timeframe


def _has_sufficient_liquidity(market: MarketDict) -> bool:
    """
    Return True if the market reports at least MIN_LIQUIDITY USD of liquidity.
    Returns False (and silently skips) when the field is absent or zero.
    """
    try:
        liquidity = float(market.get("liquidity") or 0)
        return liquidity >= config.MIN_LIQUIDITY
    except (TypeError, ValueError):
        return False


def filter_markets(markets: list[MarketDict]) -> list[MarketDict]:
    """Apply keyword and liquidity filters; return the qualifying subset."""
    # Debug: print a sample of raw market titles so we can tune keywords
    log_info(f"--- Sample of first 20 active market titles (for keyword tuning) ---")
    for m in markets[:20]:
        title = m.get("question") or m.get("description") or "(no title)"
        log_info(f"  SAMPLE | {title}")
    log_info(f"--- End sample ---")

    filtered = [
        m for m in markets
        if m.get("active", False)
        and _matches_keywords(m)
        and _has_sufficient_liquidity(m)
    ]
    log_info(f"Filtered to {len(filtered)} relevant markets.")
    return filtered


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
    url = f"{config.POLYMARKET_API_URL}/book"
    data = await _get_json(session, url, params={"token_id": token_id})
    if data is None:
        return None

    asks: list[dict] = data.get("asks", [])
    if not asks:
        return None  # no ask side — illiquid

    try:
        # asks are returned best-first (lowest price first for asks)
        return float(asks[0]["price"])
    except (KeyError, ValueError, IndexError):
        return None


async def _fetch_token_prices(
    session: aiohttp.ClientSession,
    market: MarketDict,
) -> tuple[float | None, float | None]:
    """
    Concurrently fetch the best ask for the YES token and the NO token.

    Polymarket binary markets have exactly two outcome tokens stored in
    market["tokens"] as [{"token_id": "...", "outcome": "Yes"}, {"token_id": "...", "outcome": "No"}]

    Returns (yes_ask, no_ask).  Either value is None if unavailable.
    """
    tokens: list[dict] = market.get("tokens", [])
    if len(tokens) < 2:
        return None, None

    # Identify which token is YES and which is NO
    yes_token_id: str | None = None
    no_token_id: str | None = None
    for tok in tokens:
        outcome = (tok.get("outcome") or "").lower()
        if outcome == "yes":
            yes_token_id = tok.get("token_id")
        elif outcome == "no":
            no_token_id = tok.get("token_id")

    if not yes_token_id or not no_token_id:
        return None, None

    # Fetch both asks concurrently
    yes_ask, no_ask = await asyncio.gather(
        _fetch_best_ask(session, yes_token_id),
        _fetch_best_ask(session, no_token_id),
    )
    return yes_ask, no_ask


# ── Arbitrage detection ───────────────────────────────────────────────────────

def _check_arb(
    market: MarketDict,
    yes_ask: float,
    no_ask: float,
    timestamp: str,
) -> Opportunity | None:
    """
    Check whether this market presents an arbitrage opportunity.

    In a fair binary market YES_ask + NO_ask ≈ 1.00.
    If the sum is below ARB_THRESHOLD the buyer can lock in a risk-free profit
    equal to (1.00 - total_cost) per dollar deployed — assuming both legs fill.

    Returns an Opportunity dataclass if the condition is met, else None.
    """
    total_cost = yes_ask + no_ask
    if total_cost >= config.ARB_THRESHOLD:
        return None

    edge = 1.0 - total_cost
    edge_pct = edge * 100

    return Opportunity(
        timestamp=timestamp,
        market_id=market.get("condition_id") or market.get("market_slug") or "unknown",
        market_title=market.get("question") or market.get("description") or "Unknown Market",
        yes_ask=round(yes_ask, 4),
        no_ask=round(no_ask, 4),
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

    Fetches order-book data concurrently (one coroutine per market) to
    keep the full cycle well within SCAN_INTERVAL_SECONDS even for 50+ markets.

    Returns the list of Opportunity objects found this cycle.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    opportunities: list[Opportunity] = []

    # Build one coroutine per market, then gather them all
    async def _process_market(market: MarketDict) -> Opportunity | None:
        title = market.get("question") or "?"
        try:
            yes_ask, no_ask = await _fetch_token_prices(session, market)

            if yes_ask is None or no_ask is None:
                # Missing price data — skip silently as per spec
                return None

            return _check_arb(market, yes_ask, no_ask, timestamp)

        except Exception as exc:
            # Never crash the main loop on a single market error
            log_warning(f"Unexpected error processing '{title}': {exc}")
            return None

    results = await asyncio.gather(*(_process_market(m) for m in markets))

    for result in results:
        if result is not None:
            log_opportunity(result)
            opportunities.append(result)
            # TODO: place_order() here — submit YES and NO legs to capture the edge

    return opportunities


# ── Market refresh ────────────────────────────────────────────────────────────

async def load_and_filter_markets(session: aiohttp.ClientSession) -> list[MarketDict]:
    """
    Fetch all active markets and apply filters.
    Called once at startup (and could be called periodically to catch new markets).
    """
    all_markets = await fetch_all_markets(session)
    return filter_markets(all_markets)
