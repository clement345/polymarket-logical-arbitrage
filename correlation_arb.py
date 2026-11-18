from __future__ import annotations
"""
Layer 3b — Correlation Arbitrage

Two strategies detected from signed Pearson ρ between market YES-price series:

1. buy_both_no  (ρ < 0)
   Negatively correlated markets are likely mutually exclusive outcomes.
   When ρ × (p_a + p_b) < -(1 + fees), buy NO on both.
   Guaranteed profit if markets are truly exclusive (deterministic arb).
   Entry: ρ × sum < -(1 + fees)

2. spread_trade (ρ > 0)
   Positively correlated markets track the same underlying.
   When they diverge (spread wide) and ρ × sum > 1 + fees, buy YES on the
   cheaper one and NO on the more expensive one.
   Statistical arb — convergence expected but not guaranteed.
   Entry: ρ × sum > 1 + fees  AND  spread > fees / ρ
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Optional

from correlation_grouper import get_correlated_pairs
from ingestion import get_connection

log = logging.getLogger(__name__)

TAKER_FEE       = 0.0156
_CAPITAL        = float(os.getenv("PAPER_CAPITAL_USDC", os.getenv("CAPITAL_USDC", "1000")))
_MAX_PCT        = 0.02
_MIN_BET        = float(os.getenv("MIN_BET_USDC", "20"))
MIN_ABS_RHO     = float(os.getenv("CORR_ARB_MIN_RHO", "0.7"))


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class CorrelationArbOpportunity:
    strategy: str           # "buy_both_no" | "spread_trade"
    market_id_a: str
    market_id_b: str
    question_a: str
    question_b: str
    yes_price_a: float      # current price of market A
    yes_price_b: float      # current price of market B
    rho: float
    rho_sum: float          # ρ × (p_a + p_b)
    cost: float             # total entry cost per unit
    gross_profit: float
    total_fees: float
    net_profit: float
    budget_usdc: float      # position size (scaled by |ρ| confidence)
    is_profitable: bool

    def __str__(self) -> str:
        flag = "PROFITABLE" if self.is_profitable else "unprofitable"
        q_a  = self.question_a[:45]
        q_b  = self.question_b[:45]
        return (
            f"[{flag}] {self.strategy.upper()} | ρ={self.rho:+.3f} "
            f"p_a={self.yes_price_a:.3f} p_b={self.yes_price_b:.3f} "
            f"rho_sum={self.rho_sum:+.4f} net={self.net_profit:.4f} "
            f"budget={self.budget_usdc:.2f}$ | {q_a} / {q_b}"
        )


# ---------------------------------------------------------------------------
# Market loader
# ---------------------------------------------------------------------------

def _load_market_info(market_ids: list[str]) -> dict[str, dict]:
    """Returns {market_id: {question, yes_price}} for given ids."""
    if not market_ids:
        return {}
    placeholders = ",".join(["%s"] * len(market_ids))
    sql = f"SELECT market_id, question, raw_json FROM markets WHERE market_id IN ({placeholders})"
    result = {}
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, market_ids)
        for mid, question, raw in cur.fetchall():
            raw_dict = raw if isinstance(raw, dict) else json.loads(raw)
            prices_raw = raw_dict.get("outcomePrices")
            if not prices_raw:
                continue
            try:
                prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
                yes_price = float(prices[0])
                if not (0.02 <= yes_price <= 0.98):
                    continue
            except (IndexError, ValueError, TypeError):
                continue
            result[mid] = {"question": question or "", "yes_price": yes_price}
    return result


# ---------------------------------------------------------------------------
# Strategy builders
# ---------------------------------------------------------------------------

def _check_buy_both_no(
    mid_a: str, mid_b: str,
    p_a: float, p_b: float,
    q_a: str, q_b: str,
    rho: float,
) -> Optional[CorrelationArbOpportunity]:
    """
    ρ < 0 case: buy NO on both.
    Entry condition: ρ × (p_a + p_b) < -(1 + fees_on_cost)
    Profit = (p_a + p_b) - 1 - fees    (guaranteed if markets are exclusive)
    """
    p_sum   = p_a + p_b
    rho_sum = rho * p_sum

    cost    = (1.0 - p_a) + (1.0 - p_b)   # = 2 - p_sum
    fees    = TAKER_FEE * cost
    gross   = p_sum - 1.0
    net     = gross - fees

    # Entry gate: signed condition AND positive gross after fees
    if rho_sum >= -(1.0 + fees):
        return None

    budget = round(max(_MIN_BET, _CAPITAL * _MAX_PCT * abs(rho)), 4)

    return CorrelationArbOpportunity(
        strategy="buy_both_no",
        market_id_a=mid_a,
        market_id_b=mid_b,
        question_a=q_a,
        question_b=q_b,
        yes_price_a=round(p_a, 4),
        yes_price_b=round(p_b, 4),
        rho=round(rho, 4),
        rho_sum=round(rho_sum, 6),
        cost=round(cost, 6),
        gross_profit=round(gross, 6),
        total_fees=round(fees, 6),
        net_profit=round(net, 6),
        budget_usdc=budget,
        is_profitable=net > 2.0 * fees,
    )


MIN_BOTH_YES_SUM = float(os.getenv("MIN_BOTH_YES_SUM", "0.85"))


def _check_buy_both_yes(
    mid_a: str, mid_b: str,
    p_a: float, p_b: float,
    q_a: str, q_b: str,
    rho: float,
) -> Optional[CorrelationArbOpportunity]:
    """
    ρ < 0, 0.85 < p_sum < 1: buy YES on both.
    Valid only when markets are nearly complementary (one MUST resolve YES).
    Requires p_sum > MIN_BOTH_YES_SUM to avoid false positives where both
    markets can resolve NO (e.g. two specific outcomes among many).
    Profit = 1 - (p_a + p_b) - fees  (guaranteed if markets are exclusive+exhaustive)
    """
    p_sum   = p_a + p_b

    # Guard: p_sum too low → markets are NOT complementary (both can resolve NO)
    if p_sum < MIN_BOTH_YES_SUM:
        return None

    q_sum      = 2.0 - p_sum
    rho_no_sum = rho * q_sum       # rho < 0, q_sum > 1 → negative

    cost    = p_sum                # buying YES_A + YES_B
    fees    = TAKER_FEE * cost
    gross   = 1.0 - p_sum
    net     = gross - fees

    # Entry gate (symmetric to buy_both_no applied to NO prices)
    if rho_no_sum >= -(1.0 + fees):
        return None

    budget = round(max(_MIN_BET, _CAPITAL * _MAX_PCT * abs(rho)), 4)

    return CorrelationArbOpportunity(
        strategy="buy_both_yes",
        market_id_a=mid_a,
        market_id_b=mid_b,
        question_a=q_a,
        question_b=q_b,
        yes_price_a=round(p_a, 4),
        yes_price_b=round(p_b, 4),
        rho=round(rho, 4),
        rho_sum=round(rho_no_sum, 6),
        cost=round(cost, 6),
        gross_profit=round(gross, 6),
        total_fees=round(fees, 6),
        net_profit=round(net, 6),
        budget_usdc=budget,
        is_profitable=net > 2.0 * fees,
    )


def _check_spread_trade(
    mid_a: str, mid_b: str,
    p_a: float, p_b: float,
    q_a: str, q_b: str,
    rho: float,
) -> Optional[CorrelationArbOpportunity]:
    """
    ρ > 0 case: long YES on cheaper market, short YES on more expensive (= buy NO).
    Entry condition: ρ × (p_a + p_b) > 1 + fees  AND  spread > fees / ρ
    Profit = spread - fees  (in correlated-outcome scenarios)
    """
    p_sum   = p_a + p_b
    rho_sum = rho * p_sum

    p_min   = min(p_a, p_b)
    p_max   = max(p_a, p_b)
    spread  = p_max - p_min

    cost    = p_min + (1.0 - p_max)   # = 1 - spread
    fees    = TAKER_FEE * cost
    gross   = spread
    net     = gross - fees

    # Entry gates
    if rho_sum <= 1.0 + fees:
        return None
    if spread <= fees / rho:          # spread must cover fees adjusted for confidence
        return None

    # Assign A/B to long/short direction
    if p_a < p_b:
        long_id,  long_q  = mid_a, q_a   # buy YES on A (cheaper)
        short_id, short_q = mid_b, q_b   # buy NO  on B (expensive)
    else:
        long_id,  long_q  = mid_b, q_b
        short_id, short_q = mid_a, q_a

    budget = round(max(_MIN_BET, _CAPITAL * _MAX_PCT * rho), 4)

    return CorrelationArbOpportunity(
        strategy="spread_trade",
        market_id_a=long_id,            # convention: A = long YES
        market_id_b=short_id,           # convention: B = short YES (buy NO)
        question_a=long_q,
        question_b=short_q,
        yes_price_a=round(p_min, 4),
        yes_price_b=round(p_max, 4),
        rho=round(rho, 4),
        rho_sum=round(rho_sum, 6),
        cost=round(cost, 6),
        gross_profit=round(gross, 6),
        total_fees=round(fees, 6),
        net_profit=round(net, 6),
        budget_usdc=budget,
        is_profitable=net > 2.0 * fees,
    )


# ---------------------------------------------------------------------------
# Main detection function
# ---------------------------------------------------------------------------

def detect_correlation_arbitrage(
    prices: Optional[dict[str, float]] = None,
) -> list[CorrelationArbOpportunity]:
    """
    Detect correlation-based arbitrage opportunities.

    prices: optional {market_id: yes_price} from live WebSocket feed.
            When provided, overrides stale DB prices.

    Returns all opportunities (check .is_profitable to filter).
    """
    pairs = get_correlated_pairs(min_abs_rho=MIN_ABS_RHO)
    if not pairs:
        log.debug("detect_correlation_arbitrage: no pairs (cold start or no history)")
        return []

    # Load market info for all unique ids in detected pairs
    all_ids = list({mid for mid_a, mid_b, _ in pairs for mid in (mid_a, mid_b)})
    market_info = _load_market_info(all_ids)

    opportunities: list[CorrelationArbOpportunity] = []

    for mid_a, mid_b, rho in pairs:
        info_a = market_info.get(mid_a)
        info_b = market_info.get(mid_b)
        if not info_a or not info_b:
            continue

        # Prefer live price if available
        p_a = prices.get(mid_a, info_a["yes_price"]) if prices else info_a["yes_price"]
        p_b = prices.get(mid_b, info_b["yes_price"]) if prices else info_b["yes_price"]
        q_a = info_a["question"]
        q_b = info_b["question"]

        if rho < 0:
            for checker in (_check_buy_both_no, _check_buy_both_yes):
                opp = checker(mid_a, mid_b, p_a, p_b, q_a, q_b, rho)
                if opp is not None:
                    opportunities.append(opp)
        else:
            opp = _check_spread_trade(mid_a, mid_b, p_a, p_b, q_a, q_b, rho)
            if opp is not None:
                opportunities.append(opp)

    profitable = [o for o in opportunities if o.is_profitable]
    log.info(
        "detect_correlation_arbitrage: %d pairs → %d opportunities, %d profitable",
        len(pairs), len(opportunities), len(profitable),
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

    opps = detect_correlation_arbitrage()
    profitable = [o for o in opps if o.is_profitable]
    print(f"\n{len(opps)} opportunities found, {len(profitable)} profitable.")
    for o in profitable:
        print(o)
