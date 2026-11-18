from __future__ import annotations
"""
Strategy Tester — Active trading simulation for correlation arb.

Positions are closed by buying the inverse (not waiting for market resolution).
P&L is computed with double fees: entry + exit.

Close conditions (both configurable via env):
  Take profit : unrealized net P&L >= TAKE_PROFIT_MULT × expected_net_profit
  Stop loss   : unrealized net P&L <= -STOP_LOSS_MULT  × expected_net_profit

Usage:
    python3 strat_tester.py            # both strategies
    python3 strat_tester.py --strat 1  # buy_both_no  (rho < 0)
    python3 strat_tester.py --strat 2  # spread_trade (rho > 0)
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv

from correlation_arb import CorrelationArbOpportunity, detect_correlation_arbitrage
from ingestion import get_connection

load_dotenv()
log = logging.getLogger(__name__)

TAKER_FEE          = 0.0156
DETECTION_INTERVAL = 60        # seconds
EXPIRY_DAYS        = 7
TAKE_PROFIT_MULT   = float(os.getenv("TAKE_PROFIT_MULT", "1.0"))   # 1× expected net
STOP_LOSS_MULT     = float(os.getenv("STOP_LOSS_MULT",   "2.0"))   # 2× expected net
_CAPITAL           = float(os.getenv("PAPER_CAPITAL_USDC", os.getenv("CAPITAL_USDC", "1000")))
SIM_LABEL          = os.getenv("SIM_LABEL", f"cap{int(_CAPITAL)}")


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS strat_test_positions (
    id                  SERIAL      PRIMARY KEY,
    strategy            TEXT        NOT NULL,
    opp_key             TEXT        NOT NULL,
    market_id_a         TEXT        NOT NULL,   -- long YES (spread) or NO (buy_both)
    market_id_b         TEXT        NOT NULL,   -- long NO  (spread) or NO (buy_both)
    question_a          TEXT        NOT NULL DEFAULT '',
    question_b          TEXT        NOT NULL DEFAULT '',
    rho                 FLOAT       NOT NULL,
    entry_cost          FLOAT       NOT NULL,   -- per unit
    budget_usdc         FLOAT       NOT NULL,
    units               FLOAT       NOT NULL,   -- budget / entry_cost
    yes_price_a_in      FLOAT       NOT NULL,
    yes_price_b_in      FLOAT       NOT NULL,
    expected_net_profit FLOAT       NOT NULL,   -- net per unit at entry (after fees)
    entry_fees          FLOAT       NOT NULL,   -- fees paid on entry (total $)
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at           TIMESTAMPTZ,
    yes_price_a_out     FLOAT,
    yes_price_b_out     FLOAT,
    exit_fees           FLOAT,
    realized_pnl        FLOAT,
    close_reason        TEXT,                   -- "take_profit" | "stop_loss" | "expired"
    status              TEXT        NOT NULL DEFAULT 'open',
    sim_label           TEXT        NOT NULL DEFAULT 'default'
);
CREATE INDEX IF NOT EXISTS idx_stp_status ON strat_test_positions (status, strategy);
CREATE INDEX IF NOT EXISTS idx_stp_sim    ON strat_test_positions (sim_label, status);
"""


def _init_db():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(_DDL)


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------

def _db_prices(market_ids: list[str]) -> dict[str, float]:
    if not market_ids:
        return {}
    ph  = ",".join(["%s"] * len(market_ids))
    sql = f"SELECT market_id, raw_json FROM markets WHERE market_id IN ({ph})"
    out: dict[str, float] = {}
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, market_ids)
        for mid, raw in cur.fetchall():
            raw_d = raw if isinstance(raw, dict) else json.loads(raw)
            pr = raw_d.get("outcomePrices")
            if not pr:
                continue
            try:
                prices = json.loads(pr) if isinstance(pr, str) else pr
                out[mid] = float(prices[0])
            except (IndexError, ValueError, TypeError):
                pass
    return out


# ---------------------------------------------------------------------------
# P&L engine
# ---------------------------------------------------------------------------

def _exit_value(strategy: str, p_a: float, p_b: float) -> float:
    """
    Current value of the position per unit at prices (p_a, p_b).

    buy_both_no : hold NO_A + NO_B  → value = (1−p_a) + (1−p_b)
    buy_both_yes: hold YES_A + YES_B → value = p_a + p_b
    spread_trade: hold YES_A + NO_B  → value = p_a + (1−p_b)
    """
    if strategy == "buy_both_no":
        return (1.0 - p_a) + (1.0 - p_b)
    if strategy == "buy_both_yes":
        return p_a + p_b
    return p_a + (1.0 - p_b)


def _unrealized_pnl(
    strategy: str,
    p_a_in: float, p_b_in: float,
    p_a_now: float, p_b_now: float,
    entry_cost: float,
    units: float,
) -> tuple[float, float]:
    """
    Returns (net_pnl_usdc, exit_fees_usdc).
    Counts fees on BOTH entry and exit.
    """
    ev_now  = _exit_value(strategy, p_a_now, p_b_now)
    gross   = (ev_now - entry_cost) * units
    ef      = TAKER_FEE * ev_now * units          # exit fees
    en      = TAKER_FEE * entry_cost * units      # entry fees (already paid)
    net     = gross - en - ef
    return round(net, 6), round(ef, 6)


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------

def _opp_key(opp: CorrelationArbOpportunity) -> str:
    ids = sorted([opp.market_id_a, opp.market_id_b])
    return hashlib.sha256(f"{opp.strategy}:{':'.join(ids)}".encode()).hexdigest()[:16]


def _has_open(key: str) -> bool:
    sql = "SELECT 1 FROM strat_test_positions WHERE opp_key=%s AND sim_label=%s AND status='open' LIMIT 1"
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (key, SIM_LABEL))
        return cur.fetchone() is not None


def _insert(opp: CorrelationArbOpportunity):
    units       = opp.budget_usdc / opp.cost
    entry_fees  = round(TAKER_FEE * opp.cost * units, 6)

    # Expected exit value at ideal close (for buy_both → 1 payout; spread → convergence at cost)
    exit_cost_per_unit = 1.0 if opp.strategy in ("buy_both_no", "buy_both_yes") else opp.cost
    exit_fees_per_unit = TAKER_FEE * exit_cost_per_unit
    # Round-trip net: gross − entry_fees_per_unit − exit_fees_per_unit
    expected_net_roundtrip = opp.net_profit - exit_fees_per_unit  # opp.net_profit already deducts entry fees

    sql = """
        INSERT INTO strat_test_positions
            (strategy, opp_key, market_id_a, market_id_b,
             question_a, question_b, rho, entry_cost, budget_usdc, units,
             yes_price_a_in, yes_price_b_in, expected_net_profit, entry_fees, sim_label)
        VALUES (%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (
            opp.strategy, _opp_key(opp),
            opp.market_id_a, opp.market_id_b,
            opp.question_a[:200], opp.question_b[:200],
            opp.rho, opp.cost, opp.budget_usdc, units,
            opp.yes_price_a, opp.yes_price_b,
            round(expected_net_roundtrip, 6),
            entry_fees,
            SIM_LABEL,
        ))


def _close_position(
    pos_id: int,
    p_a_out: float, p_b_out: float,
    realized_pnl: float, exit_fees: float,
    reason: str,
):
    sql = """
        UPDATE strat_test_positions
        SET status='closed', closed_at=NOW(),
            yes_price_a_out=%s, yes_price_b_out=%s,
            exit_fees=%s, realized_pnl=%s, close_reason=%s
        WHERE id=%s
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (p_a_out, p_b_out, exit_fees, realized_pnl, reason, pos_id))


def _expire_stale():
    sql = f"""
        UPDATE strat_test_positions SET status='expired', close_reason='expired'
        WHERE status='open' AND sim_label=%s AND detected_at < NOW() - INTERVAL '{EXPIRY_DAYS} days'
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (SIM_LABEL,))


def _load_open(strategy_filter: Optional[str]) -> list[dict]:
    sql = """
        SELECT id, strategy, market_id_a, market_id_b,
               question_a, question_b, rho, entry_cost, budget_usdc, units,
               yes_price_a_in, yes_price_b_in, expected_net_profit, entry_fees,
               detected_at
        FROM strat_test_positions WHERE status='open' AND sim_label=%s
    """
    params: list = [SIM_LABEL]
    if strategy_filter:
        sql += " AND strategy=%s"
        params.append(strategy_filter)
    sql += " ORDER BY detected_at DESC"
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _capital_used() -> float:
    """Total budget_usdc currently locked in open positions for this simulation."""
    sql = "SELECT COALESCE(SUM(budget_usdc), 0) FROM strat_test_positions WHERE status='open' AND sim_label=%s"
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (SIM_LABEL,))
        return float(cur.fetchone()[0])


def _stats(strategy: str) -> dict:
    sql = """
        SELECT
            COUNT(*)                                               AS closed,
            COUNT(*) FILTER (WHERE realized_pnl > 0)             AS wins,
            COUNT(*) FILTER (WHERE realized_pnl <= 0)            AS losses,
            COALESCE(SUM(realized_pnl), 0)                        AS total_pnl,
            COALESCE(AVG(realized_pnl), 0)                        AS avg_pnl,
            COALESCE(MAX(realized_pnl), 0)                        AS best,
            COALESCE(MIN(realized_pnl), 0)                        AS worst,
            COALESCE(SUM(entry_fees + COALESCE(exit_fees,0)), 0)  AS total_fees,
            COUNT(*) FILTER (WHERE close_reason='take_profit')    AS tp_count,
            COUNT(*) FILTER (WHERE close_reason='stop_loss')      AS sl_count
        FROM strat_test_positions
        WHERE status='closed' AND sim_label=%s AND strategy=%s
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (SIM_LABEL, strategy,))
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, cur.fetchone()))


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def _render(
    cycle: int,
    started_at: str,
    open_pos: list[dict],
    prices: dict[str, float],
    strategy_filter: Optional[str],
    last_closes: list[dict],
    capital_full: bool = False,
):
    s1 = _stats("buy_both_no")
    s2 = _stats("buy_both_yes")
    s3 = _stats("spread_trade")

    now_utc = datetime.now(timezone.utc)
    W = 74

    def _fmt(v: float, decimals: int = 2) -> str:
        s = f"+${v:.{decimals}f}" if v >= 0 else f"-${abs(v):.{decimals}f}"
        return s

    def _wr(s: dict) -> str:
        return f"{s['wins']/s['closed']:.1%}" if s["closed"] else "   —  "

    def _age(dt) -> str:
        if dt is None:
            return "?"
        if not hasattr(dt, "tzinfo") or dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        secs = max(0, (now_utc - dt).total_seconds())
        return f"{secs/60:.0f}m" if secs < 3600 else f"{secs/3600:.1f}h"

    def _bar(wins: int, total: int, w: int = 18) -> str:
        if not total:
            return "─" * w
        return "█" * round(wins / total * w) + "░" * (w - round(wins / total * w))

    lines: list[str] = []

    def row(s: str = ""):
        lines.append("║  " + s.ljust(W - 4) + "  ║")

    def divider(char: str = "═"):
        lines.append("╠" + char * W + "╣")

    used = sum(p["budget_usdc"] for p in open_pos)
    cap_pct = used / _CAPITAL * 100 if _CAPITAL else 0
    cap_status = "  ⚠ CAPITAL FULL — new positions blocked" if capital_full else f"  Capital: ${used:.0f} / ${_CAPITAL:.0f} ({cap_pct:.0f}%)"

    lines.append("╔" + "═" * W + "╗")
    lines.append("║" + f"  [{SIM_LABEL}]  │  cycle {cycle:04d}  │  TP={TAKE_PROFIT_MULT}×  SL={STOP_LOSS_MULT}×  │  capital=${_CAPITAL:.0f}  │  {started_at}".center(W) + "║")
    lines.append("║" + cap_status.ljust(W) + "║")
    divider()

    for label, strat, s, tag in [
        ("STRAT 1 — buy_both_no  (ρ<0 · p_sum>1 · buy NO on both)",  "buy_both_no",  s1, "BN"),
        ("STRAT 2 — buy_both_yes (ρ<0 · p_sum<1 · buy YES on both)", "buy_both_yes", s2, "BY"),
        ("STRAT 3 — spread_trade (ρ>0 · long cheap / short exp)",     "spread_trade", s3, "ST"),
    ]:
        if strategy_filter and strategy_filter != strat:
            continue
        open_count = sum(1 for p in open_pos if p["strategy"] == strat)
        lines.append("║  " + label.ljust(W - 2) + "║")
        row(
            f"Open: {open_count:3d}  │  Closed: {s['closed']:4d}  │  "
            f"TP: {s['tp_count']:3d}  SL: {s['sl_count']:3d}  │  "
            f"Win: {_wr(s):>6s}  │  Total: {_fmt(s['total_pnl']):>9s}"
        )
        row(
            f"Avg: {_fmt(s['avg_pnl']):>8s}  │  "
            f"Best: {_fmt(s['best']):>8s}  Worst: {_fmt(s['worst']):>8s}  │  "
            f"Fees: {_fmt(s['total_fees']):>8s}  │  {_bar(s['wins'], s['closed'])}"
        )
        divider("─")

    # Open positions
    lines.append("║  " + f"OPEN POSITIONS ({len(open_pos)})   TP≥{_fmt(TAKE_PROFIT_MULT, 0)}×expected   SL≤-{_fmt(STOP_LOSS_MULT, 0)}×expected".ljust(W - 2) + "║")

    shown = 0
    for pos in open_pos:
        if shown >= 6:
            row(f"  … {len(open_pos) - shown} more open positions")
            break
        p_a = prices.get(pos["market_id_a"], pos["yes_price_a_in"])
        p_b = prices.get(pos["market_id_b"], pos["yes_price_b_in"])
        upnl, _ = _unrealized_pnl(
            pos["strategy"],
            pos["yes_price_a_in"], pos["yes_price_b_in"],
            p_a, p_b,
            pos["entry_cost"], pos["units"],
        )
        target_tp = TAKE_PROFIT_MULT * pos["expected_net_profit"] * pos["units"]
        target_sl = -STOP_LOSS_MULT  * pos["expected_net_profit"] * pos["units"]
        tag = {"buy_both_no": "BN", "buy_both_yes": "BY"}.get(pos["strategy"], "ST")

        pnl_bar = f"[{_fmt(upnl):>8s} / TP:{_fmt(target_tp):>7s} SL:{_fmt(target_sl):>8s}]"
        row(
            f"[{tag}] ρ={pos['rho']:+.3f}  {_age(pos['detected_at'])} ago  "
            f"in={pos['yes_price_a_in']:.3f}/{pos['yes_price_b_in']:.3f}  "
            f"now={p_a:.3f}/{p_b:.3f}  {pnl_bar}"
        )
        row(f"     A: {pos['question_a'][:62]}")
        row(f"     B: {pos['question_b'][:62]}")
        shown += 1

    # Last closed positions
    if last_closes:
        divider("─")
        lines.append("║  " + "LAST CLOSED".ljust(W - 2) + "║")
        for c in last_closes[-4:]:
            icon   = "✓" if c["realized_pnl"] > 0 else "✗"
            reason = c.get("close_reason", "?")[:2].upper()
            row(
                f"  {icon} [{c['strategy'][:2].upper()}] {reason}  "
                f"pnl={_fmt(c['realized_pnl']):>8s}  "
                f"fees={_fmt(c.get('exit_fees', 0) + c.get('entry_fees', 0)):>7s}  "
                f"{c['question_a'][:38]}"
            )

    lines.append("╚" + "═" * W + "╝")

    print("\033[2J\033[H", end="")
    print("\n".join(lines))
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(strategy_filter: Optional[str] = None):
    _init_db()

    # strat_tester uses DB prices only (ingestion refreshes every 60s).
    # WebSocket feed opens 50+ connections which overwhelm the CLOB server.
    feed = None

    started_at  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cycle       = 0
    last_closes: list[dict] = []

    while True:
        t0 = time.monotonic()
        cycle += 1

        live: dict[str, float] = {}

        # ── 1. Detect new opportunities ───────────────────────────────────
        capital_full = False
        try:
            opps = [o for o in detect_correlation_arbitrage(prices=None)
                    if o.is_profitable]
            if strategy_filter:
                opps = [o for o in opps if o.strategy == strategy_filter]
            new = 0
            used = _capital_used()
            for opp in opps:
                if used + opp.budget_usdc > _CAPITAL:
                    capital_full = True
                    break
                key = _opp_key(opp)
                if not _has_open(key):
                    _insert(opp)
                    used += opp.budget_usdc
                    new += 1
            if new:
                log.warning("Opened %d new position(s)", new)
        except Exception as e:
            log.error("Detection: %s", e)

        # ── 2. Evaluate open positions + close if target hit ──────────────
        open_pos = _load_open(strategy_filter)
        all_mids = list({m for p in open_pos for m in (p["market_id_a"], p["market_id_b"])})
        prices   = _db_prices(all_mids)

        for pos in open_pos:
            p_a = prices.get(pos["market_id_a"])
            p_b = prices.get(pos["market_id_b"])
            if p_a is None or p_b is None:
                continue

            net_pnl, exit_fees = _unrealized_pnl(
                pos["strategy"],
                pos["yes_price_a_in"], pos["yes_price_b_in"],
                p_a, p_b,
                pos["entry_cost"], pos["units"],
            )

            expected_total = pos["expected_net_profit"] * pos["units"]
            tp_target =  TAKE_PROFIT_MULT * expected_total
            sl_target = -STOP_LOSS_MULT   * expected_total

            if net_pnl >= tp_target:
                reason = "take_profit"
            elif net_pnl <= sl_target:
                reason = "stop_loss"
            else:
                continue  # still running

            _close_position(pos["id"], p_a, p_b, net_pnl, exit_fees, reason)
            icon = "✓ TP" if reason == "take_profit" else "✗ SL"
            log.warning(
                "%s [%s] pnl=%.4f USDC | A=%s B=%s",
                icon, pos["strategy"], net_pnl,
                pos["question_a"][:40], pos["question_b"][:40],
            )
            # Track for display
            last_closes.append({
                "strategy":    pos["strategy"],
                "realized_pnl": net_pnl,
                "exit_fees":   exit_fees,
                "entry_fees":  pos["entry_fees"],
                "close_reason": reason,
                "question_a":  pos["question_a"],
            })
            if len(last_closes) > 20:
                last_closes.pop(0)

        # ── 3. Expire stale ───────────────────────────────────────────────
        _expire_stale()

        # ── 4. Render ─────────────────────────────────────────────────────
        open_pos = _load_open(strategy_filter)
        _render(cycle, started_at, open_pos, prices, strategy_filter, last_closes, capital_full)

        time.sleep(max(0, DETECTION_INTERVAL - (time.monotonic() - t0)))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
    load_dotenv()

    parser = argparse.ArgumentParser(description="Correlation arb active strategy tester")
    parser.add_argument("--strat", choices=["1", "2"],
                        help="1=buy_both_no  2=spread_trade  (default: both)")
    args = parser.parse_args()

    run(strategy_filter={"1": "buy_both_no", "2": "spread_trade"}.get(args.strat))
