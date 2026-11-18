from __future__ import annotations
"""
Layer 2 — Deterministic Logical Arbitrage: Numeric Monotonicity

Detects pricing violations between threshold markets of the same asset.
Example: P(BTC > 110k) > P(BTC > 100k) is a guaranteed arbitrage.

No execution. Detection and profitability check only.
"""

import json
import logging
import re
from dataclasses import dataclass
from typing import Optional

from ingestion import get_connection
from correlation_grouper import get_market_to_group_map

log = logging.getLogger(__name__)

TAKER_FEE = 0.0156  # 1.56% worst-case taker fee


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ParsedMarket:
    market_id: str
    question: str
    asset: str
    direction: str      # "above" or "below"
    threshold: float
    yes_price: float
    end_date: str
    event_id: Optional[str] = None


@dataclass
class ArbOpportunity:
    asset: str
    direction: str
    end_date: str
    lower_threshold: float
    higher_threshold: float
    lower_market_id: str
    higher_market_id: str
    lower_yes_price: float
    higher_yes_price: float
    gross_spread: float
    total_fees: float
    net_profit: float
    is_profitable: bool

    def __str__(self) -> str:
        flag = "PROFITABLE" if self.is_profitable else "unprofitable"
        return (
            f"[{flag}] {self.asset} {self.direction.upper()} | "
            f"{self.lower_threshold:,.0f} (p={self.lower_yes_price:.3f}) vs "
            f"{self.higher_threshold:,.0f} (p={self.higher_yes_price:.3f}) | "
            f"spread={self.gross_spread:.4f} fees={self.total_fees:.4f} net={self.net_profit:.4f}"
        )


# ---------------------------------------------------------------------------
# Regex parsing
# ---------------------------------------------------------------------------

ASSET_ALIASES: dict[str, str] = {
    "bitcoin": "BTC", "btc": "BTC",
    "ethereum": "ETH", "eth": "ETH",
    "solana": "SOL", "sol": "SOL",
    "bnb": "BNB",
    "xrp": "XRP", "ripple": "XRP",
    "avalanche": "AVAX", "avax": "AVAX",
    "dogecoin": "DOGE", "doge": "DOGE",
    "chainlink": "LINK", "link": "LINK",
    "polygon": "MATIC", "matic": "MATIC",
    "cardano": "ADA", "ada": "ADA",
}

_ASSETS = "|".join(ASSET_ALIASES.keys())
_ABOVE  = r"above|over|exceed(?:s)?|greater than|higher than|surpass(?:es)?|at or above"
_BELOW  = r"below|under|less than|lower than|drop(?:s)? below|fall(?:s)? below|at or below"

_THRESHOLD_RE = re.compile(
    rf"\b({_ASSETS})\b"                         # group 1: asset name
    rf"(?:.*?)"
    rf"\b({_ABOVE}|{_BELOW})\b"                 # group 2: direction keyword
    rf"(?:.*?)"
    rf"\$?([\d,]+(?:\.\d+)?)\s*([kmb]?)\b",     # group 3: number, group 4: multiplier
    re.IGNORECASE,
)


def _parse_value(number: str, multiplier: str) -> float:
    value = float(number.replace(",", ""))
    mult = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}.get(multiplier.lower(), 1)
    return value * mult


def _normalize_direction(keyword: str) -> str:
    return "above" if re.search(_ABOVE, keyword, re.IGNORECASE) else "below"


def _extract_event_id(raw: dict) -> Optional[str]:
    """Extract event_id from raw market JSON (same logic as exhaustive_arb)."""
    events = raw.get("events")
    if events and isinstance(events, list) and len(events) > 0:
        ev_id = events[0].get("id") or events[0].get("slug")
        if ev_id:
            return str(ev_id)
    ev_id = raw.get("eventId") or raw.get("event_id") or raw.get("groupId")
    if ev_id:
        return str(ev_id)
    return None


def _extract_yes_price(raw: dict) -> Optional[float]:
    prices_raw = raw.get("outcomePrices")
    if not prices_raw:
        return None
    prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
    try:
        price = float(prices[0])
        return price if 0.0 < price < 1.0 else None
    except (IndexError, ValueError, TypeError):
        return None


def parse_market(row: dict, prices: Optional[dict] = None) -> Optional[ParsedMarket]:
    """
    Attempt to extract (asset, direction, threshold, yes_price) from a market row.
    Returns None if the market is not a threshold-type market.

    prices: optional {market_id: yes_price} from live WebSocket feed.
            If provided and the market is present, used instead of DB raw_json.
    """
    question = row.get("question", "")
    m = _THRESHOLD_RE.search(question)
    if not m:
        return None

    asset = ASSET_ALIASES.get(m.group(1).lower())
    if not asset:
        return None

    direction = _normalize_direction(m.group(2))

    try:
        threshold = _parse_value(m.group(3), m.group(4) or "")
    except (ValueError, AttributeError):
        return None

    market_id = row["market_id"]

    raw = row.get("raw_json") or {}
    if isinstance(raw, str):
        raw = json.loads(raw)

    # Prefer live WebSocket price over stale DB price
    if prices and market_id in prices:
        yes_price = prices[market_id]
    else:
        yes_price = _extract_yes_price(raw)

    if yes_price is None:
        return None

    return ParsedMarket(
        market_id=row["market_id"],
        question=question,
        asset=asset,
        direction=direction,
        threshold=threshold,
        yes_price=yes_price,
        end_date=str(row.get("end_date", ""))[:10],
        event_id=_extract_event_id(raw),
    )


# ---------------------------------------------------------------------------
# Database load
# ---------------------------------------------------------------------------

def _load_markets() -> list[dict]:
    sql = "SELECT market_id, question, end_date, raw_json FROM markets"
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Profitability
# ---------------------------------------------------------------------------

def _compute_opportunity(
    asset: str,
    direction: str,
    end_date: str,
    lower: ParsedMarket,
    higher: ParsedMarket,
) -> ArbOpportunity:
    """
    For an "above" violation where P(>high_T) > P(>low_T):
      Strategy : Buy YES on lower_T + Buy NO on higher_T
      Cost     : p_low + (1 - p_high)
      Worst-case payoff : 1.0  (guaranteed across all scenarios)
      Gross spread      : p_high - p_low  (= 1 - cost)
      Fees              : TAKER_FEE × cost

    Condition: net_profit > 2 × total_fees  (per project fee model)
    """
    p_low  = lower.yes_price
    p_high = higher.yes_price

    cost         = p_low + (1.0 - p_high)
    gross_spread = p_high - p_low          # guaranteed profit before fees
    total_fees   = TAKER_FEE * cost
    net_profit   = gross_spread - total_fees
    is_profitable = net_profit > 2.0 * total_fees

    return ArbOpportunity(
        asset=asset,
        direction=direction,
        end_date=end_date,
        lower_threshold=lower.threshold,
        higher_threshold=higher.threshold,
        lower_market_id=lower.market_id,
        higher_market_id=higher.market_id,
        lower_yes_price=round(p_low, 4),
        higher_yes_price=round(p_high, 4),
        gross_spread=round(gross_spread, 6),
        total_fees=round(total_fees, 6),
        net_profit=round(net_profit, 6),
        is_profitable=is_profitable,
    )


# ---------------------------------------------------------------------------
# Main detection function
# ---------------------------------------------------------------------------

def detect_numeric_arbitrage(prices: Optional[dict] = None) -> list[ArbOpportunity]:
    """
    Query all markets from the DB, parse numeric threshold markets,
    group by (asset, direction, end_date), sort by threshold,
    and detect monotonicity violations.

    prices: optional {market_id: yes_price} from live WebSocket feed.
            When provided, replaces stale DB prices with real-time values.

    For "above" markets: P(asset > T) must decrease as T increases.
    For "below" markets: P(asset < T) must increase as T increases.

    Returns all opportunities (check .is_profitable to filter to valid ones).
    """
    rows = _load_markets()
    log.info("Loaded %d markets from DB", len(rows))

    parsed: list[ParsedMarket] = []
    for row in rows:
        pm = parse_market(row, prices=prices)
        if pm:
            parsed.append(pm)

    log.info("Parsed %d/%d markets as numeric threshold markets", len(parsed), len(rows))

    # Group by (group_key, asset, direction, end_date_day)
    # Primary: correlation-based groups; fallback: event_id on cold start.
    group_map, is_corr = get_market_to_group_map(
        market_ids=[pm.market_id for pm in parsed]
    )

    groups: dict[tuple, list[ParsedMarket]] = {}
    skipped = 0
    for pm in parsed:
        if is_corr:
            gid = group_map.get(pm.market_id)
            if gid is None:
                skipped += 1
                continue
            key = (f"corr_{gid}", pm.asset, pm.direction, pm.end_date)
        else:
            if not pm.event_id:
                skipped += 1
                continue
            key = (pm.event_id, pm.asset, pm.direction, pm.end_date)
        groups.setdefault(key, []).append(pm)

    if skipped:
        log.info("Skipped %d isolated markets (no group / no event_id)", skipped)

    opportunities: list[ArbOpportunity] = []

    for (event_id, asset, direction, end_date), group in groups.items():
        if len(group) < 2:
            continue

        group.sort(key=lambda x: x.threshold)

        for i in range(len(group) - 1):
            lower  = group[i]
            higher = group[i + 1]

            if direction == "above":
                # P(>higher_T) must be <= P(>lower_T)
                if higher.yes_price > lower.yes_price:
                    opp = _compute_opportunity(asset, direction, end_date, lower, higher)
                    opportunities.append(opp)
            else:  # below
                # P(<higher_T) must be >= P(<lower_T)
                if higher.yes_price < lower.yes_price:
                    opp = _compute_opportunity(asset, direction, end_date, lower, higher)
                    opportunities.append(opp)

    profitable = [o for o in opportunities if o.is_profitable]

    log.info(
        "detect_numeric_arbitrage: %d violations, %d profitable after fees",
        len(opportunities),
        len(profitable),
    )
    for opp in profitable:
        log.info(str(opp))

    return opportunities


# ---------------------------------------------------------------------------
# Entry point (standalone test)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    opps = detect_numeric_arbitrage()
    profitable = [o for o in opps if o.is_profitable]
    print(f"\n{len(opps)} violations found, {len(profitable)} profitable.")
