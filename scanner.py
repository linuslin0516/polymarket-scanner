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
from datetime import datetime, timezone
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


import base64 as _base64


def _offset_cursor(offset: int) -> str:
    """Encode an integer offset as a Polymarket pagination cursor (base64)."""
    return _base64.b64encode(str(offset).encode()).decode()


async def fetch_all_markets(session: aiohttp.ClientSession) -> list[MarketDict]:
    """
    Retrieve the MOST RECENT markets from the Polymarket CLOB API.

    The API returns markets oldest-first.  The 5-min/15-min crypto markets
    were launched recently and live near the END of the ~28,000-market list.
    Fetching from offset 0 and capping at 10 pages misses them entirely.

    Strategy: start at a high offset (e.g. 26,000) and page forward to the
    end of the list, collecting only the newest markets.  We then apply a
    date filter so only currently-open markets are passed to the scanner.
    """
    # Determined empirically: total market count is ~28,000.
    # Start 3,000 from the end so we always catch the latest markets even
    # as new ones are added daily.  Adjust START_OFFSET upward if needed.
    START_OFFSET = 25_000
    MAX_PAGES = 10   # 10 × 1,000 = up to 10,000 markets from that point

    markets: list[MarketDict] = []
    cursor: str = _offset_cursor(START_OFFSET)
    page = 0

    while page < MAX_PAGES:
        params: dict[str, str] = {"next_cursor": cursor}

        log_info(f"Fetching markets page {page + 1} (offset ~{START_OFFSET + page * 1000})")
        data = await _get_json(session, f"{config.POLYMARKET_API_URL}/markets", params=params)
        if data is None:
            break

        batch: list[MarketDict] = data.get("data", [])
        markets.extend(batch)
        page += 1

        next_cursor: str = data.get("next_cursor", "") or ""
        log_info(f"  → got {len(batch)} markets (total so far: {len(markets)}), next_cursor={next_cursor[:16]!r}")

        if not next_cursor or next_cursor == "LTE=" or len(batch) == 0:
            break

        cursor = next_cursor

    log_info(f"Fetched {len(markets)} recent markets across {page} page(s).")
    return markets


# ── Market filtering ──────────────────────────────────────────────────────────

def _is_future_market(market: MarketDict) -> bool:
    """
    Return True if the market's end date is in the future (i.e. not yet resolved).
    The API's active=true param doesn't reliably filter old markets, so we
    check end_date_iso ourselves.  Markets with no end date are kept.
    """
    end_date_str: str = market.get("end_date_iso") or market.get("end_date") or ""
    if not end_date_str:
        return True  # no date info — keep it
    try:
        # Handle both "2026-02-25T00:00:00Z" and "2026-02-25T00:00:00+00:00"
        end_date_str = end_date_str.replace("Z", "+00:00")
        end_dt = datetime.fromisoformat(end_date_str)
        return end_dt > datetime.now(timezone.utc)
    except ValueError:
        return True  # unparseable — keep it


def _matches_keywords(market: MarketDict) -> bool:
    """
    Match against the confirmed Polymarket title format, e.g.:
      "Bitcoin Up or Down - February 24, 12:10AM-12:15AM ET"
      "Ethereum Up or Down - February 23, 3:15PM-3:30PM ET"
    A market matches if its title contains ANY string in MARKET_KEYWORDS.
    """
    title: str = (market.get("question") or market.get("description") or "").lower()
    return any(kw.lower() in title for kw in config.MARKET_KEYWORDS)


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
    """Apply date, keyword, and liquidity filters; return the qualifying subset."""
    # Step 1: only future/active markets (replaces broken active=true API param)
    future = [m for m in markets if _is_future_market(m)]
    log_info(f"After date filter: {len(future)} markets still open (from {len(markets)} total)")

    # Step 2: asset keyword match (BTC / ETH etc.)
    filtered = [
        m for m in future
        if _matches_keywords(m)
        and _has_sufficient_liquidity(m)
    ]

    # On startup, log all matched markets so operator can verify filtering
    log_info(f"--- Matched {len(filtered)} Up/Down crypto markets ---")
    for m in filtered:
        title = m.get("question") or m.get("description") or "(no title)"
        end   = (m.get("end_date_iso") or m.get("end_date") or "?")[:16]
        liq   = float(m.get("liquidity") or 0)
        log_info(f"  WATCH | [{end}] ${liq:,.0f} | {title}")
    log_info(f"--- End market list ---")

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
