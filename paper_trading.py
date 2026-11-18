from __future__ import annotations
"""
Paper Trading — MVP Profitability Validation

Runs Layer 2 + Layer 3 detection live, simulates execution, and tracks
each opportunity over time using price snapshots (T+5s, T+30s, T+60s, T+5min).

Goal: accumulate enough data to validate the strategy before enabling live execution.
Go-live threshold: see print_metrics() output.
"""

import csv
import hashlib
import json
import logging
import math
import os
import signal
import sys
import time
from dataclasses import asdict
from datetime import datetime
from typing import Optional, Union

from dotenv import load_dotenv

from correlation_arb import CorrelationArbOpportunity, detect_correlation_arbitrage
from exhaustive_arb import ExhaustiveOpportunity, detect_exhaustive_arbitrage
from ingestion import get_connection
from numeric_arb import ArbOpportunity, detect_numeric_arbitrage
from price_feed import PriceFeed


def _db_prices(market_ids: list[str]) -> dict[str, float]:
    """Fetch current yes prices from DB for a list of market IDs (fallback when WS has no coverage)."""
    if not market_ids:
        return {}
    placeholders = ",".join(["%s"] * len(market_ids))
    sql = f"SELECT market_id, raw_json FROM markets WHERE market_id IN ({placeholders})"
    result = {}
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, market_ids)
        for mid, raw in cur.fetchall():
            raw_dict = raw if isinstance(raw, dict) else json.loads(raw)
            prices_raw = raw_dict.get("outcomePrices")
            if prices_raw:
                try:
                    prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
                    price = float(prices[0])
                    if 0.0 < price < 1.0:
                        result[mid] = price
                except (IndexError, ValueError, TypeError):
                    pass
    return result

load_dotenv()
log = logging.getLogger(__name__)

PAPER_CAPITAL_USDC = float(os.getenv("PAPER_CAPITAL_USDC", os.getenv("CAPITAL_USDC", "1000")))
MAX_CAPITAL_PCT    = 0.02
TAKER_FEE          = 0.0156
DETECTION_INTERVAL = 60                  # seconds between detection cycles
SNAPSHOT_INTERVALS = [5, 30, 60, 300]   # seconds after detection

# Go-live thresholds
MIN_TRADES_RESOLVED = 50
MIN_WIN_RATE        = 0.70
MIN_SHARPE          = 1.0


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

_CSV_HEADER = [
    "id", "detected_at", "opp_type", "event_title", "direction",
    "gross_spread", "total_fees", "net_profit_est", "budget_usdc",
    "simulated_pnl", "false_positive", "status",
]


_AnyOpp = Union[ArbOpportunity, ExhaustiveOpportunity, CorrelationArbOpportunity]


def _opp_label(opp: _AnyOpp) -> tuple[str, str]:
    """Returns (event_title, direction) for CSV display."""
    if isinstance(opp, ArbOpportunity):
        title = f"{opp.asset} {opp.direction} {opp.lower_threshold:,.0f}/{opp.higher_threshold:,.0f}"
        return title, opp.direction
    if isinstance(opp, CorrelationArbOpportunity):
        title = f"ρ={opp.rho:+.3f} {opp.question_a[:30]}…/{opp.question_b[:30]}…"
        return title[:70], opp.strategy
    return opp.event_title[:70], opp.direction


def _init_csv() -> str:
    """Create a timestamped CSV file and write the header. Returns the file path."""
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(os.path.dirname(__file__), f"trades_{ts}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(_CSV_HEADER)
    log.info("CSV trade log created: %s", path)
    return path


def _append_trade_csv(path: str, trade_id: int, opp: _AnyOpp):
    """Append a newly detected trade (status=open) to the CSV."""
    event_title, direction = _opp_label(opp)
    gross = opp.gross_spread if isinstance(opp, ArbOpportunity) else opp.gross_profit
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            trade_id,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "numeric" if isinstance(opp, ArbOpportunity) else ("correlation" if isinstance(opp, CorrelationArbOpportunity) else "exhaustive"),
            event_title,
            direction,
            round(gross, 6),
            round(opp.total_fees, 6),
            round(opp.net_profit, 6),
            _budget(opp),
            "",   # simulated_pnl — unknown yet
            "",   # false_positive — unknown yet
            "open",
        ])


def _export_all_trades_csv(path: str):
    """Overwrite the CSV with all trades from DB (captures resolved P&L)."""
    sql = """
        SELECT id, detected_at, opp_type, opp_json,
               gross_spread, total_fees, net_profit_est, budget_usdc,
               simulated_pnl, false_positive, status
        FROM paper_trades
        ORDER BY id
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(_CSV_HEADER)
        for row in rows:
            (trade_id, detected_at, opp_type, opp_json_raw,
             gross_spread, total_fees, net_profit_est, budget_usdc,
             simulated_pnl, false_positive, status) = row

            opp_data = opp_json_raw if isinstance(opp_json_raw, dict) else json.loads(opp_json_raw)
            if opp_type == "numeric":
                event_title = (
                    f"{opp_data.get('asset','')} {opp_data.get('direction','')} "
                    f"{opp_data.get('lower_threshold',0):,.0f}/{opp_data.get('higher_threshold',0):,.0f}"
                )
                direction = opp_data.get("direction", "")
            else:
                event_title = opp_data.get("event_title", "")[:70]
                direction   = opp_data.get("direction", "")

            writer.writerow([
                trade_id,
                detected_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(detected_at, "strftime") else detected_at,
                opp_type,
                event_title,
                direction,
                gross_spread,
                total_fees,
                net_profit_est,
                budget_usdc,
                simulated_pnl if simulated_pnl is not None else "",
                false_positive if false_positive is not None else "",
                status,
            ])

    log.info("CSV exported: %s (%d rows)", path, len(rows))


# ---------------------------------------------------------------------------
# Opportunity helpers
# ---------------------------------------------------------------------------

def _opp_key(opp: _AnyOpp) -> str:
    """Stable hash used for deduplication across detection cycles."""
    if isinstance(opp, ArbOpportunity):
        raw = f"numeric:{opp.lower_market_id}:{opp.higher_market_id}"
    elif isinstance(opp, CorrelationArbOpportunity):
        ids = sorted([opp.market_id_a, opp.market_id_b])
        raw = f"correlation:{opp.strategy}:{':'.join(ids)}"
    else:
        ids = sorted(m.market_id for m in opp.markets)
        raw = f"exhaustive:{':'.join(ids)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _market_ids(opp: _AnyOpp) -> list[str]:
    if isinstance(opp, ArbOpportunity):
        return [opp.lower_market_id, opp.higher_market_id]
    if isinstance(opp, CorrelationArbOpportunity):
        return [opp.market_id_a, opp.market_id_b]
    return [m.market_id for m in opp.markets]


def _opp_to_json(opp: _AnyOpp) -> str:
    """Serialize opportunity to JSON for storage."""
    if isinstance(opp, ArbOpportunity):
        return json.dumps({"type": "numeric", **asdict(opp)})
    if isinstance(opp, CorrelationArbOpportunity):
        return json.dumps({"type": "correlation", **asdict(opp)})
    d = asdict(opp)
    return json.dumps({"type": "exhaustive", **d})


def _budget(opp: _AnyOpp) -> float:
    if isinstance(opp, CorrelationArbOpportunity):
        return opp.budget_usdc   # already sized by |ρ| confidence
    return round(PAPER_CAPITAL_USDC * MAX_CAPITAL_PCT, 4)


def _simulated_pnl(
    opp: _AnyOpp,
    snapshot_5s: Optional[dict],
    budget: float,
) -> tuple[Optional[float], Optional[bool]]:
    """
    Compute (simulated_pnl_usdc, is_false_positive).

    false_positive=True  → spread was already gone at T+5s (stale price at detection)
    false_positive=False → spread confirmed real at T+5s
    simulated_pnl=None   → couldn't determine (missing prices)
    """
    if snapshot_5s is None:
        return None, None

    mids = _market_ids(opp)
    if not all(mid in snapshot_5s for mid in mids):
        return None, None  # incomplete snapshot

    if isinstance(opp, ArbOpportunity):
        p_low_5s  = snapshot_5s[opp.lower_market_id]
        p_high_5s = snapshot_5s[opp.higher_market_id]
        spread_real = p_high_5s > p_low_5s

        if not spread_real:
            return 0.0, True  # false positive

        cost_per_share = opp.lower_yes_price + (1.0 - opp.higher_yes_price)
        shares         = budget / cost_per_share
        gross          = opp.gross_spread * shares
        fees           = TAKER_FEE * cost_per_share * shares
        return round(gross - fees, 4), False

    if isinstance(opp, CorrelationArbOpportunity):
        p_a_5s = snapshot_5s[opp.market_id_a]
        p_b_5s = snapshot_5s[opp.market_id_b]

        if opp.strategy == "buy_both_no":
            # False positive if the sum has already collapsed back below 1
            sum_5s      = p_a_5s + p_b_5s
            spread_real = sum_5s > 1.0
            if not spread_real:
                return 0.0, True
            shares = budget / opp.cost
            gross  = opp.gross_profit * shares
            fees   = TAKER_FEE * opp.cost * shares
            return round(gross - fees, 4), False

        else:  # spread_trade: A = long YES (cheaper), B = short YES (expensive)
            # False positive if spread already closed or reversed at T+5s
            spread_real = p_b_5s > p_a_5s
            if not spread_real:
                return 0.0, True
            shares = budget / opp.cost
            gross  = opp.gross_profit * shares
            fees   = TAKER_FEE * opp.cost * shares
            return round(gross - fees, 4), False

    else:  # ExhaustiveOpportunity
        sum_5s    = sum(snapshot_5s[mid] for mid in mids)
        if opp.direction == "buy_all_yes":
            spread_real = sum_5s < 1.0
            cost        = opp.yes_sum
        else:
            spread_real = sum_5s > 1.0
            cost        = len(opp.markets) - opp.yes_sum

        if not spread_real:
            return 0.0, True  # false positive

        shares = budget / cost
        gross  = opp.gross_profit * shares
        fees   = TAKER_FEE * cost * shares
        return round(gross - fees, 4), False


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

def init_paper_db():
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        ddl = f.read()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(ddl)
    log.info("Paper trading DB schema ready")


def _has_open_trade(opp_key: str) -> bool:
    sql = "SELECT 1 FROM paper_trades WHERE opp_key = %s AND status = 'open' LIMIT 1"
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (opp_key,))
        return cur.fetchone() is not None


def _insert_trade(opp: Union[ArbOpportunity, ExhaustiveOpportunity]) -> int:
    budget = _budget(opp)
    sql = """
        INSERT INTO paper_trades
            (opp_type, opp_key, opp_json, gross_spread, total_fees, net_profit_est, budget_usdc)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    if isinstance(opp, ArbOpportunity):
        opp_type    = "numeric"
        gross_spread = opp.gross_spread
    elif isinstance(opp, CorrelationArbOpportunity):
        opp_type    = "correlation"
        gross_spread = opp.gross_profit
    else:
        opp_type    = "exhaustive"
        gross_spread = opp.gross_profit
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (
            opp_type,
            _opp_key(opp),
            _opp_to_json(opp),
            gross_spread,
            opp.total_fees,
            opp.net_profit,
            budget,
        ))
        return cur.fetchone()[0]


def _save_snapshot(trade_id: int, interval_s: int, prices: dict[str, float]):
    col_map = {5: "snapshot_5s", 30: "snapshot_30s", 60: "snapshot_60s", 300: "snapshot_5m"}
    col = col_map.get(interval_s)
    if not col:
        return
    sql = f"UPDATE paper_trades SET {col} = %s WHERE id = %s"
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (json.dumps(prices), trade_id))


def _resolve_trade(trade_id: int, simulated_pnl: Optional[float], false_positive: Optional[bool]):
    sql = """
        UPDATE paper_trades
        SET status = 'resolved', simulated_pnl = %s, false_positive = %s
        WHERE id = %s
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (simulated_pnl, false_positive, trade_id))


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def print_metrics() -> str:
    sql = """
        SELECT
            COUNT(*)                                              AS total,
            COUNT(*) FILTER (WHERE simulated_pnl > 0)            AS wins,
            COUNT(*) FILTER (WHERE false_positive = true)        AS false_positives,
            COALESCE(AVG(simulated_pnl), 0)                      AS avg_pnl,
            COALESCE(SUM(simulated_pnl), 0)                      AS total_pnl,
            COALESCE(STDDEV(simulated_pnl), 0)                   AS pnl_stddev
        FROM paper_trades
        WHERE status = 'resolved'
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    total, wins, fp, avg_pnl, total_pnl, pnl_stddev = row
    total = total or 0

    win_rate = (wins / total) if total > 0 else 0.0
    fp_rate  = (fp / total)   if total > 0 else 0.0
    sharpe   = (avg_pnl / pnl_stddev) * math.sqrt(252) if pnl_stddev and pnl_stddev > 0 else 0.0

    # Go-live readiness check
    ready = (
        total    >= MIN_TRADES_RESOLVED and
        win_rate >= MIN_WIN_RATE        and
        sharpe   >= MIN_SHARPE          and
        total_pnl > 0
    )

    lines = [
        "=" * 55,
        "  PAPER TRADING METRICS",
        "=" * 55,
        f"  Resolved trades    : {total}  (need {MIN_TRADES_RESOLVED})",
        f"  Win rate           : {win_rate:.1%}  (need {MIN_WIN_RATE:.0%})",
        f"  False positive rate: {fp_rate:.1%}",
        f"  Avg P&L / trade    : ${avg_pnl:.2f}",
        f"  Cumulative P&L     : ${total_pnl:.2f}",
        f"  Annualized Sharpe  : {sharpe:.2f}  (need {MIN_SHARPE:.1f})",
        "-" * 55,
        f"  GO-LIVE READY      : {'✓ YES' if ready else '✗ NOT YET'}",
        "=" * 55,
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def paper_trading_loop():
    """
    Main paper trading loop.

    Each cycle:
      1. Run detection (Layer 2 + Layer 3)
      2. Record new opportunities (skip duplicates already open)
      3. Take price snapshots for open trades at the right intervals
      4. Resolve trades that have completed all snapshots (T+5min elapsed)
      5. Print metrics every 10 cycles

    On Ctrl+C: exports all trades to CSV and exits.
    """
    init_paper_db()

    # CSV setup + graceful exit on Ctrl+C
    csv_path = _init_csv()

    def _on_exit(signum, frame):
        log.info("Interrupted — exporting trades to CSV...")
        try:
            _export_all_trades_csv(csv_path)
            print(f"\nCSV saved: {csv_path}")
        except Exception as e:
            log.error("CSV export failed: %s", e)
        sys.exit(0)

    signal.signal(signal.SIGINT, _on_exit)
    signal.signal(signal.SIGTERM, _on_exit)

    # Start WebSocket price feed if CLOB credentials are configured,
    # otherwise fall back to DB prices (good enough for local testing)
    _clob_ready = all(os.getenv(k) for k in ("CLOB_API_KEY", "CLOB_API_SECRET", "CLOB_API_PASSPHRASE"))
    feed: Optional[PriceFeed] = None
    if _clob_ready:
        feed = PriceFeed()
        feed.start()
        feed.wait_for_data(timeout=15.0, min_prices=10)
        log.info(feed.status())
    else:
        log.warning("CLOB credentials not set — using DB prices (60s stale). "
                    "Set CLOB_API_KEY/SECRET/PASSPHRASE to enable real-time WebSocket feed.")

    log.info(
        "Paper trading started | capital=%.0f USDC | budget/trade=%.2f USDC",
        PAPER_CAPITAL_USDC,
        PAPER_CAPITAL_USDC * MAX_CAPITAL_PCT,
    )

    # In-memory tracking: {trade_id: {opp, detected_at, snapshots_taken: set}}
    pending: dict[int, dict] = {}
    cycle = 0

    while True:
        cycle_start = time.monotonic()
        cycle += 1
        log.info("--- Cycle %d | %s ---", cycle, feed.status() if feed else "no WebSocket (DB prices)")

        # 1. Detect opportunities — live prices if WebSocket running, else DB prices
        live_prices = feed.get_prices() if feed else None
        try:
            numeric     = [o for o in detect_numeric_arbitrage(prices=live_prices)       if o.is_profitable]
            exhaustive  = [o for o in detect_exhaustive_arbitrage(prices=live_prices)    if o.is_profitable]
            correlation = [o for o in detect_correlation_arbitrage(prices=live_prices)   if o.is_profitable]
            all_opps    = numeric + exhaustive + correlation
            log.info(
                "Detected %d numeric + %d exhaustive + %d correlation profitable opportunities",
                len(numeric), len(exhaustive), len(correlation),
            )
        except Exception as e:
            log.error("Detection failed: %s", e)
            all_opps = []

        # 2. Record new opportunities
        for opp in all_opps:
            key = _opp_key(opp)
            if _has_open_trade(key):
                continue
            try:
                trade_id = _insert_trade(opp)
                pending[trade_id] = {
                    "opp":            opp,
                    "detected_at":    time.monotonic(),
                    "snapshots_taken": set(),
                }
                _append_trade_csv(csv_path, trade_id, opp)
                log.info("Recorded trade #%d | %s", trade_id, key)
            except Exception as e:
                log.error("Failed to record trade: %s", e)

        # 3. Snapshot pending trades
        now = time.monotonic()
        resolved_ids = []

        for trade_id, state in pending.items():
            age_s  = now - state["detected_at"]
            mids   = _market_ids(state["opp"])

            for interval in SNAPSHOT_INTERVALS:
                if age_s >= interval and interval not in state["snapshots_taken"]:
                    try:
                        # Live prices from WebSocket cache, or fall back to DB prices
                        if feed:
                            snapshot = {mid: p for mid in mids if (p := feed.get_price(mid)) is not None}
                            # Fallback to DB prices for markets not covered by WS feed
                            missing = [mid for mid in mids if mid not in snapshot]
                            if missing:
                                snapshot.update(_db_prices(missing))
                        else:
                            snapshot = {mid: live_prices[mid] for mid in mids if live_prices and mid in live_prices}
                            missing = [mid for mid in mids if mid not in snapshot]
                            if missing:
                                snapshot.update(_db_prices(missing))
                        _save_snapshot(trade_id, interval, snapshot)
                        state["snapshots_taken"].add(interval)
                        log.debug("Snapshot T+%ds saved for trade #%d (%d/%d prices)",
                                  interval, trade_id, len(snapshot), len(mids))
                    except Exception as e:
                        log.warning("Snapshot T+%ds failed for trade #%d: %s", interval, trade_id, e)

            # 4. Resolve when all snapshots taken
            if set(SNAPSHOT_INTERVALS).issubset(state["snapshots_taken"]):
                # Fetch snapshot_5s from DB for P&L computation
                try:
                    with get_connection() as conn, conn.cursor() as cur:
                        cur.execute("SELECT snapshot_5s, budget_usdc FROM paper_trades WHERE id = %s",
                                    (trade_id,))
                        row = cur.fetchone()
                    snap_5s = row[0] if row else None
                    budget  = row[1] if row else _budget(state["opp"])

                    pnl, fp = _simulated_pnl(state["opp"], snap_5s, budget)
                    _resolve_trade(trade_id, pnl, fp)
                    resolved_ids.append(trade_id)
                    log.info("Resolved trade #%d | pnl=%.4f false_positive=%s", trade_id, pnl or 0, fp)
                except Exception as e:
                    log.error("Failed to resolve trade #%d: %s", trade_id, e)

        for tid in resolved_ids:
            del pending[tid]

        # 5. Print metrics every 10 cycles
        if cycle % 10 == 0:
            try:
                log.info("\n%s", print_metrics())
            except Exception as e:
                log.warning("Metrics query failed: %s", e)

        # Sleep for remainder of interval
        elapsed = time.monotonic() - cycle_start
        sleep_s = max(0, DETECTION_INTERVAL - elapsed)
        log.info("Cycle done in %.1fs | sleeping %.1fs | %d trades pending",
                 elapsed, sleep_s, len(pending))
        time.sleep(sleep_s)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    paper_trading_loop()
