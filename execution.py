from __future__ import annotations
"""
Layer 4 — Execution Engine

Executes arbitrage opportunities detected by Layer 2 (numeric) or Layer 3 (exhaustive).

Safety rules:
- Never uses market orders (limit orders only)
- 300ms timeout for every leg after the first fill
- If any leg times out: cancel remaining + hedge open positions immediately
- Max 2% of capital per trade
- DRY_RUN=true by default (set to false to trade live)
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional, Union

from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

from ingestion import get_connection
from numeric_arb import ArbOpportunity
from exhaustive_arb import ExhaustiveOpportunity

load_dotenv()
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TAKER_FEE       = 0.0156
MAX_CAPITAL_PCT = 0.02    # 2% of capital per trade
LEG2_TIMEOUT_MS = 300     # ms to wait for legs 2..N to fill
LEG1_TIMEOUT_MS = 30_000  # ms to wait for leg 1 (30s)
PRICE_MOVE_LIMIT = 0.01   # abort if price moved > 1% since detection
HEDGE_DISCOUNT   = 0.03   # place hedge HEDGE_DISCOUNT below best bid for fast fill
POLL_INTERVAL_S  = 0.010  # 10ms polling interval
POLYGON_CHAIN_ID = 137

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Leg:
    market_id:   str
    token_id:    str
    side:        str        # always BUY (YES token = buy YES, NO token = buy NO)
    limit_price: float
    size_shares: float
    label:       str        # e.g. "YES@lower_threshold" — for logging


@dataclass
class ExecutionResult:
    status:  str            # "success" | "aborted" | "partial_fill" | "error"
    reason:  str = ""
    fills:   list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# CLOB client
# ---------------------------------------------------------------------------

def _get_client() -> ClobClient:
    return ClobClient(
        host=os.environ["CLOB_HOST"],
        key=os.environ["CLOB_PRIVATE_KEY"],
        chain_id=POLYGON_CHAIN_ID,
        creds=ApiCreds(
            api_key=os.environ["CLOB_API_KEY"],
            api_secret=os.environ["CLOB_API_SECRET"],
            api_passphrase=os.environ["CLOB_API_PASSPHRASE"],
        ),
    )


# ---------------------------------------------------------------------------
# Token IDs
# ---------------------------------------------------------------------------

def _get_token_ids(market_id: str) -> tuple[str, str]:
    """Return (yes_token_id, no_token_id) for a market. Reads from DB."""
    sql = "SELECT tokens, raw_json FROM markets WHERE market_id = %s"
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (market_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Market {market_id} not found in DB")

    tokens_raw, raw_json = row

    # Try structured tokens field first
    if tokens_raw:
        tokens = tokens_raw if isinstance(tokens_raw, list) else json.loads(tokens_raw)
        if isinstance(tokens, list) and len(tokens) >= 2:
            yes_id = tokens[0].get("token_id") if isinstance(tokens[0], dict) else tokens[0]
            no_id  = tokens[1].get("token_id") if isinstance(tokens[1], dict) else tokens[1]
            return str(yes_id), str(no_id)

    # Fallback: clobTokenIds from raw_json
    raw = raw_json if isinstance(raw_json, dict) else json.loads(raw_json)
    clob_raw = raw.get("clobTokenIds")
    if clob_raw:
        ids = json.loads(clob_raw) if isinstance(clob_raw, str) else clob_raw
        return str(ids[0]), str(ids[1])

    raise ValueError(f"No token IDs found for market {market_id}")


# ---------------------------------------------------------------------------
# Order book & slippage
# ---------------------------------------------------------------------------

def _compute_slippage(book: dict, size_shares: float) -> tuple[float, float]:
    """
    Walk the ask side of the order book for a BUY order.
    Returns (effective_price, fillable_shares).
    fillable_shares may be < size_shares if liquidity is insufficient.
    """
    asks = sorted(book.get("asks", []), key=lambda x: float(x["price"]))

    total_cost   = 0.0
    total_filled = 0.0
    remaining    = size_shares

    for level in asks:
        level_price = float(level["price"])
        level_size  = float(level["size"])
        fill        = min(remaining, level_size)
        total_cost  += fill * level_price
        total_filled += fill
        remaining   -= fill
        if remaining <= 0:
            break

    if total_filled == 0:
        return 0.0, 0.0

    return round(total_cost / total_filled, 6), round(total_filled, 4)


def _get_best_bid(book: dict) -> float:
    bids = book.get("bids", [])
    if not bids:
        return 0.0
    return max(float(b["price"]) for b in bids)


# ---------------------------------------------------------------------------
# Build legs from opportunity
# ---------------------------------------------------------------------------

def _build_legs_numeric(opp: ArbOpportunity, budget_usdc: float) -> list[Leg]:
    """
    Strategy: BUY YES on lower_threshold + BUY NO on higher_threshold.
    Equal share count across both legs.
    Cost per share = p_low + (1 - p_high)
    """
    yes_id_low, _  = _get_token_ids(opp.lower_market_id)
    _, no_id_high   = _get_token_ids(opp.higher_market_id)

    cost_per_share = opp.lower_yes_price + (1.0 - opp.higher_yes_price)
    if cost_per_share <= 0:
        raise ValueError("Invalid cost per share")

    size_shares = budget_usdc / cost_per_share

    return [
        Leg(
            market_id=opp.lower_market_id,
            token_id=yes_id_low,
            side=BUY,
            limit_price=opp.lower_yes_price,
            size_shares=round(size_shares, 4),
            label=f"YES@>{opp.lower_threshold:,.0f}",
        ),
        Leg(
            market_id=opp.higher_market_id,
            token_id=no_id_high,
            side=BUY,
            limit_price=round(1.0 - opp.higher_yes_price, 6),
            size_shares=round(size_shares, 4),
            label=f"NO@>{opp.higher_threshold:,.0f}",
        ),
    ]


def _build_legs_exhaustive(opp: ExhaustiveOpportunity, budget_usdc: float) -> list[Leg]:
    """
    buy_all_yes: BUY YES on each market — equal shares per leg.
    buy_all_no:  BUY NO  on each market — equal shares per leg.
    """
    legs = []
    n = len(opp.markets)

    for market in opp.markets:
        yes_id, no_id = _get_token_ids(market.market_id)

        if opp.direction == "buy_all_yes":
            token_id    = yes_id
            limit_price = market.yes_price
            label       = f"YES({market.question[:30]})"
        else:
            token_id    = no_id
            limit_price = round(1.0 - market.yes_price, 6)
            label       = f"NO({market.question[:30]})"

        # Equal USDC split across legs
        size_shares = round((budget_usdc / n) / limit_price, 4)

        legs.append(Leg(
            market_id=market.market_id,
            token_id=token_id,
            side=BUY,
            limit_price=limit_price,
            size_shares=size_shares,
            label=label,
        ))

    return legs


def _build_legs(
    opp: Union[ArbOpportunity, ExhaustiveOpportunity],
    budget_usdc: float,
) -> list[Leg]:
    if isinstance(opp, ArbOpportunity):
        return _build_legs_numeric(opp, budget_usdc)
    return _build_legs_exhaustive(opp, budget_usdc)


# ---------------------------------------------------------------------------
# Slippage pre-check
# ---------------------------------------------------------------------------

def _validate_legs_slippage(
    client: ClobClient,
    legs: list[Leg],
    opp: Union[ArbOpportunity, ExhaustiveOpportunity],
) -> bool:
    """
    For each leg, fetch the order book and compute effective fill price.
    Re-check that the opportunity remains profitable after slippage.
    Also abort if any price moved > PRICE_MOVE_LIMIT since detection.
    Returns True if still valid.
    """
    effective_prices = []

    for leg in legs:
        try:
            book = client.get_order_book(leg.token_id)
        except Exception as e:
            log.error("Failed to fetch order book for %s: %s", leg.label, e)
            return False

        eff_price, fillable = _compute_slippage(book, leg.size_shares)

        if fillable < leg.size_shares * 0.9:
            log.warning("Insufficient liquidity for leg %s (want %.2f, fillable %.2f)",
                        leg.label, leg.size_shares, fillable)
            return False

        price_drift = abs(eff_price - leg.limit_price) / leg.limit_price
        if price_drift > PRICE_MOVE_LIMIT:
            log.warning("Price moved %.2f%% for leg %s (limit: %.4f, effective: %.4f)",
                        price_drift * 100, leg.label, leg.limit_price, eff_price)
            return False

        effective_prices.append(eff_price)
        log.debug("Leg %s | limit=%.4f effective=%.4f fillable=%.2f",
                  leg.label, leg.limit_price, eff_price, fillable)

    # Re-check net profit using effective prices
    effective_cost = sum(p * leg.size_shares for p, leg in zip(effective_prices, legs))
    fees = TAKER_FEE * effective_cost

    if isinstance(opp, ArbOpportunity):
        gross = (opp.higher_yes_price - opp.lower_yes_price) * legs[0].size_shares
    else:
        gross = opp.gross_profit * legs[0].size_shares

    net = gross - fees
    if net <= 2.0 * fees:
        log.warning("Opportunity no longer profitable after slippage (net=%.4f fees=%.4f)", net, fees)
        return False

    log.info("Slippage check passed | effective_cost=%.2f net=%.4f", effective_cost, net)
    return True


# ---------------------------------------------------------------------------
# Order placement & monitoring
# ---------------------------------------------------------------------------

def _place_limit_order(client: ClobClient, leg: Leg) -> Optional[str]:
    """Place a GTC limit order. Returns order_id or None on failure."""
    if DRY_RUN:
        order_id = f"DRY-{leg.label}-{int(time.time()*1000)}"
        log.info("[DRY_RUN] Would place BUY %s | token=%s price=%.4f size=%.4f → order_id=%s",
                 leg.label, leg.token_id, leg.limit_price, leg.size_shares, order_id)
        return order_id

    try:
        args = OrderArgs(
            token_id=leg.token_id,
            price=leg.limit_price,
            size=leg.size_shares,
            side=leg.side,
        )
        signed = client.create_order(args)
        resp   = client.post_order(signed, OrderType.GTC)
        order_id = resp.get("orderID") or resp.get("id")
        log.info("Placed %s | order_id=%s price=%.4f size=%.4f",
                 leg.label, order_id, leg.limit_price, leg.size_shares)
        return order_id
    except Exception as e:
        log.error("Failed to place order for %s: %s", leg.label, e)
        return None


def _wait_for_fill(client: ClobClient, order_id: str, timeout_ms: int) -> bool:
    """Poll until order is filled or timeout. Returns True if filled."""
    if DRY_RUN:
        log.info("[DRY_RUN] Simulating fill for order %s", order_id)
        time.sleep(0.05)
        return True

    deadline = time.monotonic() + timeout_ms / 1000.0

    while time.monotonic() < deadline:
        try:
            order = client.get_order(order_id)
            status = (order.get("status") or "").upper()
            if status in ("MATCHED", "FILLED"):
                return True
            if status in ("CANCELED", "CANCELLED"):
                log.warning("Order %s was canceled externally", order_id)
                return False
        except Exception as e:
            log.warning("Error polling order %s: %s", order_id, e)
        time.sleep(POLL_INTERVAL_S)

    return False


def _cancel_order(client: ClobClient, order_id: str) -> None:
    if DRY_RUN:
        log.info("[DRY_RUN] Would cancel order %s", order_id)
        return
    try:
        client.cancel(order_id)
        log.info("Canceled order %s", order_id)
    except Exception as e:
        log.error("Failed to cancel order %s: %s", order_id, e)


def _hedge_position(client: ClobClient, leg: Leg) -> None:
    """
    Close an open filled leg by placing an aggressive limit order at
    (best_bid - HEDGE_DISCOUNT) to force a fast fill.
    """
    label = f"HEDGE:{leg.label}"

    if DRY_RUN:
        log.info("[DRY_RUN] Would hedge %s | size=%.4f", label, leg.size_shares)
        return

    try:
        book     = client.get_order_book(leg.token_id)
        best_bid = _get_best_bid(book)
        hedge_price = max(round(best_bid - HEDGE_DISCOUNT, 4), 0.01)

        # To close a BUY position we SELL at the hedge price
        from py_clob_client.order_builder.constants import SELL as SELL_SIDE
        args = OrderArgs(
            token_id=leg.token_id,
            price=hedge_price,
            size=leg.size_shares,
            side=SELL_SIDE,
        )
        signed = client.create_order(args)
        resp   = client.post_order(signed, OrderType.GTC)
        log.info("Hedge placed for %s | order_id=%s price=%.4f",
                 label, resp.get("orderID"), hedge_price)
    except Exception as e:
        log.error("HEDGE FAILED for %s — MANUAL INTERVENTION REQUIRED: %s", label, e)


# ---------------------------------------------------------------------------
# Main execution function
# ---------------------------------------------------------------------------

def execute_arbitrage(
    opportunity: Union[ArbOpportunity, ExhaustiveOpportunity],
) -> ExecutionResult:
    """
    Execute a multi-leg arbitrage opportunity.

    Flow:
      1. Build legs (token IDs + sizes) from the opportunity
      2. Validate slippage — abort if no longer profitable
      3. Place leg 0 (first leg), wait for fill (up to 30s)
      4. For each subsequent leg: place and wait 300ms
         → If timeout: cancel remaining + hedge filled legs immediately
      5. Return ExecutionResult with status and fill details
    """
    capital = float(os.environ.get("CAPITAL_USDC", "0"))
    if capital <= 0:
        log.error("CAPITAL_USDC not set or zero")
        return ExecutionResult(status="error", reason="no_capital")

    budget = capital * MAX_CAPITAL_PCT
    log.info("Starting execution | budget=%.2f USDC | DRY_RUN=%s", budget, DRY_RUN)

    # 1. Build legs
    try:
        legs = _build_legs(opportunity, budget)
    except Exception as e:
        log.error("Failed to build legs: %s", e)
        return ExecutionResult(status="error", reason=f"build_legs: {e}")

    log.info("Built %d legs: %s", len(legs), [l.label for l in legs])

    # 2. Slippage check
    client = _get_client()
    if not DRY_RUN and not _validate_legs_slippage(client, legs, opportunity):
        return ExecutionResult(status="aborted", reason="slippage_check_failed")

    # 3. Execute legs sequentially
    filled: list[dict] = []

    for i, leg in enumerate(legs):
        order_id = _place_limit_order(client, leg)

        if order_id is None:
            log.error("Leg %d placement failed — hedging %d filled legs", i, len(filled))
            for f in filled:
                _hedge_position(client, legs[f["leg_index"]])
            return ExecutionResult(status="error", reason=f"leg_{i}_placement_failed", fills=filled)

        timeout = LEG1_TIMEOUT_MS if i == 0 else LEG2_TIMEOUT_MS
        fill_ok = _wait_for_fill(client, order_id, timeout_ms=timeout)

        if not fill_ok:
            log.warning("Leg %d timed out after %dms — canceling + hedging", i, timeout)
            _cancel_order(client, order_id)
            for f in filled:
                _hedge_position(client, legs[f["leg_index"]])
            return ExecutionResult(
                status="partial_fill",
                reason=f"leg_{i}_timeout",
                fills=filled,
            )

        filled.append({
            "leg_index":   i,
            "label":       leg.label,
            "order_id":    order_id,
            "limit_price": leg.limit_price,
            "size_shares": leg.size_shares,
        })
        log.info("Leg %d filled | %s order_id=%s", i, leg.label, order_id)

    log.info("Arb executed successfully | %d legs | fills=%s", len(filled), filled)
    return ExecutionResult(status="success", fills=filled)


# ---------------------------------------------------------------------------
# Entry point (standalone test — DRY_RUN only)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not DRY_RUN:
        print("Set DRY_RUN=true before running standalone tests.")
    else:
        from numeric_arb import detect_numeric_arbitrage
        opps = [o for o in detect_numeric_arbitrage() if o.is_profitable]
        if not opps:
            print("No profitable numeric arb found.")
        else:
            result = execute_arbitrage(opps[0])
            print(f"Result: {result}")
