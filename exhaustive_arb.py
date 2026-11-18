from __future__ import annotations
"""
Layer 3 — Contradiction Arbitrage: Exhaustive Mutually Exclusive Outcomes

Detects event groups where the sum of YES prices violates probability constraints.

Two cases:
  - sum < 1 → buy all YES (guaranteed one pays out)
  - sum > 1 → buy all NO  (guaranteed n-1 pay out)

No execution. Detection and profitability check only.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Optional

from ingestion import get_connection
from correlation_grouper import get_correlated_groups

log = logging.getLogger(__name__)

TAKER_FEE = 0.0156  # 1.56% worst-case taker fee

# Minimum number of markets required to form a valid exhaustive group
MIN_GROUP_SIZE = 2


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class GroupedMarket:
    market_id: str
    question: str
    yes_price: float


@dataclass
class ExhaustiveOpportunity:
    event_id: str
    event_title: str
    end_date: str
    direction: str          # "buy_all_yes" | "buy_all_no"
    markets: list[GroupedMarket]
    yes_sum: float
    gross_profit: float
    total_fees: float
    net_profit: float
    is_profitable: bool

    def __str__(self) -> str:
        flag = "PROFITABLE" if self.is_profitable else "unprofitable"
        legs = ", ".join(f"{m.question[:40]}…={m.yes_price:.3f}" for m in self.markets)
        return (
            f"[{flag}] {self.direction.upper()} | {self.event_title} ({self.end_date}) | "
            f"sum={self.yes_sum:.4f} net={self.net_profit:.4f} | [{legs}]"
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
# Grouping
# ---------------------------------------------------------------------------

def _extract_event_key(raw: dict) -> Optional[tuple[str, str]]:
    """
    Try to extract (event_id, event_title) from the raw market JSON.
    Tries multiple Gamma API field patterns.
    Returns None if no event info found.
    """
    # Pattern 1: events array (most common in Gamma API)
    events = raw.get("events")
    if events and isinstance(events, list) and len(events) > 0:
        ev = events[0]
        ev_id = ev.get("id") or ev.get("slug")
        ev_title = ev.get("title") or ev.get("slug") or str(ev_id)
        if ev_id:
            return str(ev_id), str(ev_title)

    # Pattern 2: flat eventId / eventSlug fields
    ev_id = raw.get("eventId") or raw.get("event_id") or raw.get("groupId")
    if ev_id:
        ev_title = raw.get("eventTitle") or raw.get("eventSlug") or str(ev_id)
        return str(ev_id), str(ev_title)

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


def _group_markets(rows: list[dict], prices: Optional[dict] = None) -> dict[tuple, list[GroupedMarket]]:
    """
    Group markets by correlation (primary) or event_id (cold-start fallback).

    prices: optional {market_id: yes_price} from live WebSocket feed.
            When provided, replaces stale DB prices with real-time values.
    """
    # Build market_data dict: market_id -> (yes_price, question, end_date, raw)
    market_data: dict[str, tuple] = {}
    row_by_id: dict[str, dict] = {}

    for row in rows:
        raw = row.get("raw_json") or {}
        if isinstance(raw, str):
            raw = json.loads(raw)

        market_id = row["market_id"]
        if prices and market_id in prices:
            yes_price = prices[market_id]
        else:
            yes_price = _extract_yes_price(raw)
        if yes_price is None:
            continue
        if not (0.02 <= yes_price <= 0.98):
            continue

        end_date = str(row.get("end_date", ""))[:10]
        market_data[market_id] = (yes_price, row.get("question", ""), end_date)
        row_by_id[market_id] = row

    if not market_data:
        return {}

    corr_groups, is_corr = get_correlated_groups(market_ids=list(market_data.keys()))

    groups: dict[tuple, list[GroupedMarket]] = {}

    if is_corr:
        for idx, group_ids in enumerate(corr_groups):
            key = (f"corr_group_{idx}", f"Group {idx}", "")
            for mid in group_ids:
                if mid not in market_data:
                    continue
                yes_price, question, _ = market_data[mid]
                groups.setdefault(key, []).append(
                    GroupedMarket(market_id=mid, question=question, yes_price=yes_price)
                )
    else:
        # Cold start: fall back to event_id grouping
        for market_id, (yes_price, question, end_date) in market_data.items():
            row = row_by_id[market_id]
            raw = row.get("raw_json") or {}
            if isinstance(raw, str):
                raw = json.loads(raw)
            event_info = _extract_event_key(raw)
            if not event_info:
                continue
            event_id, event_title = event_info
            key = (event_id, event_title, end_date)
            groups.setdefault(key, []).append(
                GroupedMarket(market_id=market_id, question=question, yes_price=yes_price)
            )

    return groups


# ---------------------------------------------------------------------------
# Profitability
# ---------------------------------------------------------------------------

def _compute_buy_all_yes(
    event_id: str,
    event_title: str,
    end_date: str,
    markets: list[GroupedMarket],
) -> ExhaustiveOpportunity:
    """
    Buy YES on all markets in the group.
    Cost     = Σ p_i
    Payoff   = 1.0  (exactly one YES resolves)
    Profit   = 1 - Σ p_i
    Fees     = TAKER_FEE × Σ p_i
    Net      = (1 - Σ p_i) - TAKER_FEE × Σ p_i
    """
    yes_sum    = sum(m.yes_price for m in markets)
    gross      = 1.0 - yes_sum
    fees       = TAKER_FEE * yes_sum
    net        = gross - fees

    return ExhaustiveOpportunity(
        event_id=event_id,
        event_title=event_title,
        end_date=end_date,
        direction="buy_all_yes",
        markets=markets,
        yes_sum=round(yes_sum, 6),
        gross_profit=round(gross, 6),
        total_fees=round(fees, 6),
        net_profit=round(net, 6),
        is_profitable=net > 2.0 * fees,
    )


def _compute_buy_all_no(
    event_id: str,
    event_title: str,
    end_date: str,
    markets: list[GroupedMarket],
) -> ExhaustiveOpportunity:
    """
    Buy NO on all markets in the group.
    n        = number of markets
    Cost     = Σ (1 - p_i) = n - Σ p_i
    Payoff   = n - 1  (exactly n-1 NOs resolve)
    Profit   = (n-1) - (n - Σ p_i) = Σ p_i - 1
    Fees     = TAKER_FEE × (n - Σ p_i)
    Net      = (Σ p_i - 1) - TAKER_FEE × (n - Σ p_i)
    """
    n          = len(markets)
    yes_sum    = sum(m.yes_price for m in markets)
    no_cost    = n - yes_sum
    gross      = yes_sum - 1.0
    fees       = TAKER_FEE * no_cost
    net        = gross - fees

    return ExhaustiveOpportunity(
        event_id=event_id,
        event_title=event_title,
        end_date=end_date,
        direction="buy_all_no",
        markets=markets,
        yes_sum=round(yes_sum, 6),
        gross_profit=round(gross, 6),
        total_fees=round(fees, 6),
        net_profit=round(net, 6),
        is_profitable=net > 2.0 * fees,
    )


# ---------------------------------------------------------------------------
# Main detection function
# ---------------------------------------------------------------------------

def detect_exhaustive_arbitrage(prices: Optional[dict] = None) -> list[ExhaustiveOpportunity]:
    """
    Query markets from DB, group by event, and detect exhaustive arb opportunities.

    prices: optional {market_id: yes_price} from live WebSocket feed.
            When provided, replaces stale DB prices with real-time values.

    A valid group has markets whose YES prices should sum to >= 1 (probability axiom:
    exactly one outcome resolves YES, so prices must cover the full probability space).

    Two violations:
      sum < 1 → buy_all_yes arb (sum of probabilities leaves free money)
      sum > 1 → buy_all_no  arb (sum exceeds 1, sell the overhang)

    Returns all opportunities (check .is_profitable to filter valid ones).
    """
    rows = _load_markets()
    log.info("Loaded %d markets from DB", len(rows))

    groups = _group_markets(rows, prices=prices)
    log.info("Found %d event groups", len(groups))

    opportunities: list[ExhaustiveOpportunity] = []

    for (event_id, event_title, end_date), markets in groups.items():
        if len(markets) < MIN_GROUP_SIZE:
            continue

        yes_sum = sum(m.yes_price for m in markets)

        # Reject non-exclusive groups: valid winner markets have yes_sum ≈ 1.
        # Groups with yes_sum far from 1 have multiple YES resolutions (invalid).
        # Allow ±30% tolerance around 1 to catch genuine arbs while excluding
        # multi-resolution groups (sports sub-markets, "Top 20 finish", etc.).
        if not (0.7 <= yes_sum <= 1.3):
            continue

        if yes_sum < 1.0:
            opp = _compute_buy_all_yes(event_id, event_title, end_date, markets)
            opportunities.append(opp)

        elif yes_sum > 1.0:
            opp = _compute_buy_all_no(event_id, event_title, end_date, markets)
            opportunities.append(opp)

    profitable = [o for o in opportunities if o.is_profitable]

    log.info(
        "detect_exhaustive_arbitrage: %d violations, %d profitable after fees",
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

    opps = detect_exhaustive_arbitrage()
    profitable = [o for o in opps if o.is_profitable]
    print(f"\n{len(opps)} violations found, {len(profitable)} profitable.")
    for o in profitable:
        print(o)
