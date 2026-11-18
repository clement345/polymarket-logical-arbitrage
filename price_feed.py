from __future__ import annotations
"""
Price Feed — Real-time prices via multiple Polymarket CLOB WebSocket connections.

Architecture:
  - One asyncio event loop in a background daemon thread
  - N concurrent WebSocket coroutines (one per MAX_TOKENS_PER_WS chunk)
  - Shared thread-safe PriceCache readable from any sync code
  - Auto-reconnect with exponential backoff per connection

Usage:
  feed = PriceFeed()
  feed.start()
  feed.wait_for_data()              # block until prices are flowing
  prices = feed.get_prices()        # {market_id: yes_price} — always fresh
"""

import asyncio
import json
import logging
import os
import threading
import time
from threading import Lock
from typing import Optional

import websockets
from dotenv import load_dotenv

from ingestion import get_connection

load_dotenv()
log = logging.getLogger(__name__)

WS_URL            = os.getenv("CLOB_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
MAX_TOKENS_PER_WS = 200     # tokens per connection — stay under WS frame size limit
MAX_WS_CONNECTIONS = int(os.getenv("MAX_WS_CONNECTIONS", "50"))  # cap concurrent connections
WS_STAGGER_S      = 0.1     # delay between launching connections to avoid 429
RECONNECT_BASE_S  = 1.0     # initial reconnect delay
RECONNECT_MAX_S   = 60.0    # max reconnect delay (exponential backoff)
PRICE_STALENESS_S = 60.0    # prices older than this are dropped from get_all_prices()


# ---------------------------------------------------------------------------
# Thread-safe price cache
# ---------------------------------------------------------------------------

class PriceCache:
    """
    Thread-safe in-memory store for real-time token prices.
    Maps token_id → {mid, bid, ask, ts} and market_id → yes_token_id.
    """

    def __init__(self):
        self._lock                          = Lock()
        self._data: dict[str, dict]         = {}   # token_id → {mid, bid, ask, ts}
        self._market_to_yes: dict[str, str] = {}   # market_id → yes_token_id

    def register_market(self, market_id: str, yes_token_id: str, no_token_id: str):
        with self._lock:
            self._market_to_yes[market_id] = yes_token_id

    def update(self, token_id: str, mid: float, bid: float, ask: float):
        with self._lock:
            self._data[token_id] = {
                "mid": mid,
                "bid": bid,
                "ask": ask,
                "ts":  time.monotonic(),
            }

    def get_entry(self, token_id: str) -> dict:
        """Return copy of price entry (empty dict if not found)."""
        with self._lock:
            return dict(self._data.get(token_id, {}))

    def get_mid(self, token_id: str) -> Optional[float]:
        with self._lock:
            entry = self._data.get(token_id)
            if entry and time.monotonic() - entry["ts"] < PRICE_STALENESS_S:
                return entry["mid"]
        return None

    def get_all_prices(self) -> dict[str, float]:
        """Returns {market_id: yes_mid_price} for all markets with non-stale data."""
        with self._lock:
            now = time.monotonic()
            return {
                market_id: self._data[yes_tid]["mid"]
                for market_id, yes_tid in self._market_to_yes.items()
                if yes_tid in self._data
                and now - self._data[yes_tid]["ts"] < PRICE_STALENESS_S
            }

    def size(self) -> int:
        with self._lock:
            return len(self._data)

    def market_count(self) -> int:
        with self._lock:
            return len(self._market_to_yes)


# ---------------------------------------------------------------------------
# WebSocket message handling
# ---------------------------------------------------------------------------

def _handle_single(msg: dict, cache: PriceCache, conn_log: logging.Logger):
    """Process a single message dict from the WebSocket."""
    event_type = msg.get("event_type")
    asset_id   = msg.get("asset_id")

    if not asset_id:
        return

    if event_type == "book":
        bids     = msg.get("bids") or []
        asks     = msg.get("asks") or []
        best_bid = max((float(b["price"]) for b in bids), default=0.0)
        best_ask = min((float(a["price"]) for a in asks), default=1.0)

        if best_bid > 0.0 and best_ask < 1.0 and best_bid < best_ask:
            mid = round((best_bid + best_ask) / 2.0, 6)
            cache.update(asset_id, mid, best_bid, best_ask)
            conn_log.debug("book  | %s… | bid=%.4f ask=%.4f mid=%.4f",
                           asset_id[:10], best_bid, best_ask, mid)

    elif event_type == "price_change":
        for change in (msg.get("changes") or []):
            try:
                price = float(change.get("price") or 0)
            except (ValueError, TypeError):
                continue
            if 0.0 < price < 1.0:
                entry = cache.get_entry(asset_id)
                cache.update(
                    asset_id,
                    price,
                    entry.get("bid", round(price * 0.99, 6)),
                    entry.get("ask", round(price * 1.01, 6)),
                )
                conn_log.debug("price | %s… | %.4f", asset_id[:10], price)

    elif event_type == "last_trade_price":
        try:
            price = float(msg.get("price") or 0)
        except (ValueError, TypeError):
            return
        if 0.0 < price < 1.0:
            entry = cache.get_entry(asset_id)
            cache.update(
                asset_id,
                price,
                entry.get("bid", round(price * 0.99, 6)),
                entry.get("ask", round(price * 1.01, 6)),
            )


def _handle_message(raw: str, cache: PriceCache, conn_log: logging.Logger):
    """Parse WS frame (list or dict) and dispatch to _handle_single."""
    try:
        msg = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return

    if isinstance(msg, list):
        for item in msg:
            if isinstance(item, dict):
                _handle_single(item, cache, conn_log)
    elif isinstance(msg, dict):
        _handle_single(msg, cache, conn_log)


# ---------------------------------------------------------------------------
# Single WebSocket connection coroutine
# ---------------------------------------------------------------------------

async def _ws_connection(
    token_ids:      list[str],
    cache:          PriceCache,
    conn_id:        int,
    api_key:        str,
    api_secret:     str,
    api_passphrase: str,
):
    """
    Persistent WebSocket connection for one chunk of token IDs.
    Reconnects automatically with exponential backoff.
    Runs forever — cancel the task to stop.
    """
    conn_log = log.getChild(f"conn{conn_id}")
    delay    = RECONNECT_BASE_S
    # Stagger connection start to avoid bursting 429s
    await asyncio.sleep(conn_id * WS_STAGGER_S)

    subscribe_msg = json.dumps({
        "auth": {
            "apiKey":     api_key,
            "secret":     api_secret,
            "passphrase": api_passphrase,
        },
        "type":      "market",
        "assets_ids": token_ids,
    })

    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=30,
                open_timeout=30,
            ) as ws:
                await ws.send(subscribe_msg)
                conn_log.info("Connected | %d tokens subscribed", len(token_ids))
                delay = RECONNECT_BASE_S  # reset on clean connect

                async for raw in ws:
                    _handle_message(raw, cache, conn_log)

        except websockets.ConnectionClosed as e:
            conn_log.warning("Closed: %s | retry in %.1fs", e, delay)
        except OSError as e:
            conn_log.warning("Network: %s | retry in %.1fs", e, delay)
        except Exception as e:
            conn_log.error("Unexpected: %s | retry in %.1fs", e, delay)

        await asyncio.sleep(delay)
        delay = min(delay * 2.0, RECONNECT_MAX_S)


# ---------------------------------------------------------------------------
# Token map loader
# ---------------------------------------------------------------------------

def _extract_token_ids(tokens_raw, raw_json) -> tuple[str, str]:
    """Extract (yes_token_id, no_token_id) from DB row fields."""
    if tokens_raw:
        tokens = tokens_raw if isinstance(tokens_raw, list) else json.loads(tokens_raw)
        if isinstance(tokens, list) and len(tokens) >= 2:
            yes = tokens[0].get("token_id") if isinstance(tokens[0], dict) else tokens[0]
            no  = tokens[1].get("token_id") if isinstance(tokens[1], dict) else tokens[1]
            if yes and no:
                return str(yes), str(no)

    raw = raw_json if isinstance(raw_json, dict) else json.loads(raw_json)
    clob = raw.get("clobTokenIds")
    if clob:
        ids = json.loads(clob) if isinstance(clob, str) else clob
        if len(ids) >= 2:
            return str(ids[0]), str(ids[1])

    raise ValueError("No token IDs found")


def _load_token_map() -> dict[str, tuple[str, str]]:
    """
    Load all markets from DB.
    Returns {market_id: (yes_token_id, no_token_id)}.
    """
    sql = "SELECT market_id, tokens, raw_json FROM markets"
    result: dict[str, tuple[str, str]] = {}

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        for market_id, tokens_raw, raw_json in cur.fetchall():
            try:
                yes_tid, no_tid = _extract_token_ids(tokens_raw, raw_json)
                result[market_id] = (yes_tid, no_tid)
            except Exception:
                pass

    log.info("Token map loaded: %d markets", len(result))
    return result


# ---------------------------------------------------------------------------
# PriceFeed — public interface
# ---------------------------------------------------------------------------

class PriceFeed:
    """
    Manages N concurrent WebSocket connections to the Polymarket CLOB.

    On start():
      1. Loads all markets from DB → extracts token IDs
      2. Splits YES token IDs into chunks of MAX_TOKENS_PER_WS
      3. Launches one asyncio coroutine per chunk in a background thread
      4. All coroutines share a single PriceCache

    Thread model:
      - One background daemon thread runs the asyncio event loop
      - PriceCache uses threading.Lock → safe to read from any thread
    """

    def __init__(self):
        self.cache   = PriceCache()
        self._thread: Optional[threading.Thread] = None
        self._tasks:  list[asyncio.Task]          = []

    def start(self):
        """Load token map from DB and start all WebSocket connections."""
        token_map = _load_token_map()

        for market_id, (yes_tid, no_tid) in token_map.items():
            self.cache.register_market(market_id, yes_tid, no_tid)

        # Collect YES token IDs (we track YES prices for arb detection)
        all_yes = [yes_tid for yes_tid, _ in token_map.values()]

        # Split into chunks — one WebSocket connection per chunk, cap at MAX_WS_CONNECTIONS
        all_chunks = [
            all_yes[i:i + MAX_TOKENS_PER_WS]
            for i in range(0, len(all_yes), MAX_TOKENS_PER_WS)
        ]
        chunks = all_chunks[:MAX_WS_CONNECTIONS]
        if len(all_chunks) > MAX_WS_CONNECTIONS:
            log.warning(
                "Capping WS connections: %d chunks → %d (skipping %d chunks / %d tokens)",
                len(all_chunks), MAX_WS_CONNECTIONS,
                len(all_chunks) - MAX_WS_CONNECTIONS,
                sum(len(c) for c in all_chunks[MAX_WS_CONNECTIONS:]),
            )

        api_key        = os.environ["CLOB_API_KEY"]
        api_secret     = os.environ["CLOB_API_SECRET"]
        api_passphrase = os.environ["CLOB_API_PASSPHRASE"]

        async def _run_all():
            await asyncio.gather(*[
                _ws_connection(chunk, self.cache, i, api_key, api_secret, api_passphrase)
                for i, chunk in enumerate(chunks)
            ])

        def _thread_main():
            asyncio.run(_run_all())

        self._thread = threading.Thread(
            target=_thread_main,
            daemon=True,
            name="price-feed",
        )
        self._thread.start()

        log.info(
            "PriceFeed started | %d markets | %d connections | %d tokens/conn max",
            len(token_map), len(chunks), MAX_TOKENS_PER_WS,
        )

    def wait_for_data(self, timeout: float = 15.0, min_prices: int = 10) -> bool:
        """
        Block the calling thread until at least min_prices entries are in cache.
        Returns True if ready, False on timeout.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            n = self.cache.size()
            if n >= min_prices:
                log.info("PriceFeed ready | %d / %d prices cached",
                         n, self.cache.market_count())
                return True
            time.sleep(0.1)
        log.warning("PriceFeed timeout | only %d prices after %.1fs",
                    self.cache.size(), timeout)
        return False

    def get_prices(self) -> dict[str, float]:
        """Returns {market_id: yes_price} for all markets with fresh data."""
        return self.cache.get_all_prices()

    def get_price(self, market_id: str) -> Optional[float]:
        """Returns the current YES price for a single market, or None."""
        yes_tid = self.cache._market_to_yes.get(market_id)
        if not yes_tid:
            return None
        return self.cache.get_mid(yes_tid)

    def status(self) -> str:
        n = self.cache.size()
        m = self.cache.market_count()
        return f"PriceFeed | {n}/{m} markets with fresh prices"
