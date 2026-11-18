from __future__ import annotations
"""
Layer 1 — Market Ingestion
Fetches active Polymarket markets via Gamma API and stores them in PostgreSQL.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

GAMMA_API_BASE = os.getenv("GAMMA_API_BASE", "https://gamma-api.polymarket.com")
DATABASE_URL = os.getenv("DATABASE_URL")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL_SECONDS", "60"))
HISTORY_RETENTION_HOURS = int(os.getenv("HISTORY_RETENTION_HOURS", "24"))
PAGE_SIZE = 500


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """Create tables if they don't exist (reads schema.sql)."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        ddl = f.read()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(ddl)
    log.info("DB schema ready")


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_markets() -> list[dict]:
    """
    Fetch all active, non-closed markets from the Gamma API.
    Handles pagination automatically.
    Returns a list of raw market dicts.
    """
    markets = []
    offset = 0
    session = requests.Session()

    while True:
        params = {
            "active": "true",
            "closed": "false",
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        try:
            resp = session.get(
                f"{GAMMA_API_BASE}/markets",
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error("Gamma API request failed (offset=%d): %s", offset, e)
            raise

        page = resp.json()

        if not page:
            break

        markets.extend(page)
        log.debug("Fetched %d markets (offset=%d)", len(page), offset)

        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    log.info("fetch_markets: %d markets retrieved", len(markets))
    return markets


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

def _parse_json_field(value) -> list | None:
    """Gamma API returns some array fields as JSON strings — parse them."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None


def store_markets(markets: list[dict]) -> int:
    """
    Upsert markets into PostgreSQL.
    Returns the number of rows upserted.
    """
    if not markets:
        return 0

    rows = []
    for m in markets:
        rows.append({
            "market_id": m.get("id") or m.get("conditionId"),
            "question":  m.get("question", ""),
            "outcomes":  json.dumps(_parse_json_field(m.get("outcomes"))),
            "end_date":  m.get("endDate"),
            "category":  m.get("category"),
            "tokens":    json.dumps(_parse_json_field(m.get("tokens") or m.get("clobTokenIds"))),
            "raw_json":  json.dumps(m),
        })

    # Drop rows with no market_id (malformed API response)
    rows = [r for r in rows if r["market_id"]]

    sql = """
        INSERT INTO markets (market_id, question, outcomes, end_date, category, tokens, raw_json, fetched_at, updated_at)
        VALUES (%(market_id)s, %(question)s, %(outcomes)s::jsonb, %(end_date)s, %(category)s, %(tokens)s::jsonb, %(raw_json)s::jsonb, NOW(), NOW())
        ON CONFLICT (market_id) DO UPDATE SET
            question   = EXCLUDED.question,
            outcomes   = EXCLUDED.outcomes,
            end_date   = EXCLUDED.end_date,
            category   = EXCLUDED.category,
            tokens     = EXCLUDED.tokens,
            raw_json   = EXCLUDED.raw_json,
            updated_at = NOW()
    """

    with get_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)

    log.info("store_markets: %d rows upserted", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Price history
# ---------------------------------------------------------------------------

def store_price_history(markets: list[dict]) -> int:
    """
    Insert a yes_price snapshot for each market into price_history.
    Purges entries older than HISTORY_RETENTION_HOURS.
    Returns number of rows inserted.
    """
    rows = []
    for m in markets:
        market_id = m.get("id") or m.get("conditionId")
        prices_raw = m.get("outcomePrices")
        if not market_id or not prices_raw:
            continue
        try:
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            yes_price = float(prices[0])
            if not (0.0 < yes_price < 1.0):
                continue
        except (IndexError, ValueError, TypeError):
            continue
        rows.append({"market_id": market_id, "yes_price": yes_price})

    if not rows:
        return 0

    sql_insert = (
        "INSERT INTO price_history (market_id, yes_price) "
        "VALUES (%(market_id)s, %(yes_price)s)"
    )
    sql_purge = (
        f"DELETE FROM price_history "
        f"WHERE recorded_at < NOW() - INTERVAL '{HISTORY_RETENTION_HOURS} hours'"
    )
    with get_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql_insert, rows, page_size=500)
        cur.execute(sql_purge)

    log.info("store_price_history: %d rows inserted", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Loop
# ---------------------------------------------------------------------------

def update_markets_loop():
    """
    Fetch and store markets every FETCH_INTERVAL seconds.
    Runs indefinitely. Errors are logged but do not stop the loop.
    """
    log.info("Starting ingestion loop (interval=%ds)", FETCH_INTERVAL)
    init_db()

    while True:
        start = time.monotonic()
        try:
            markets = fetch_markets()
            count = store_markets(markets)
            store_price_history(markets)
            elapsed = time.monotonic() - start
            log.info("Cycle done: %d markets in %.2fs", count, elapsed)
        except Exception as e:
            log.error("Ingestion cycle failed: %s", e)

        time.sleep(FETCH_INTERVAL)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    update_markets_loop()
