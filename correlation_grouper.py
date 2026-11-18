from __future__ import annotations
"""
Correlation-based market grouper.

Replaces event_id grouping with temporal price correlation:
two markets that move together (|ρ| >= CORR_THRESHOLD) are assumed
to belong to the same real-world event.

Public API
----------
get_correlated_groups(market_ids) -> (list[list[str]], bool)
get_market_to_group_map(market_ids) -> (dict[str, int], bool)

The bool return value is `is_correlation_based`:
  True  = enough history for correlation → use correlation groups
  False = cold start → caller should fall back to event_id
"""

import logging
import os
import time
from collections import deque
from typing import Optional

from ingestion import get_connection

log = logging.getLogger(__name__)

CORR_THRESHOLD     = float(os.getenv("CORR_THRESHOLD", "0.85"))
MIN_HISTORY_POINTS = int(os.getenv("MIN_HISTORY_POINTS", "10"))
MAX_HISTORY_POINTS = int(os.getenv("MAX_HISTORY_POINTS", "60"))
# Only consider markets expiring within this window (reduces N² dramatically)
ACTIVE_DAYS_WINDOW  = int(os.getenv("ACTIVE_DAYS_WINDOW", "30"))
MIN_PRICE_STDDEV    = float(os.getenv("MIN_PRICE_STDDEV", "0.005"))  # skip flat markets

# Cache: TTL-based caches for groups and pairs
_CACHE_TTL = 300.0  # 5 minutes
_cache: dict = {}
_pairs_cache: dict = {}


# ---------------------------------------------------------------------------
# Active market filter
# ---------------------------------------------------------------------------

def _load_active_market_ids() -> list[str]:
    """
    Returns market_ids with end_date in the next ACTIVE_DAYS_WINDOW days
    AND non-trivial price variance (prices that actually moved).
    Limits N from ~34k to ~500-2k, making O(N²) feasible.
    """
    sql = """
        SELECT ph.market_id
        FROM price_history ph
        JOIN markets m ON m.market_id = ph.market_id
        WHERE m.end_date BETWEEN NOW() AND NOW() + INTERVAL %s
        GROUP BY ph.market_id
        HAVING COUNT(*) >= %s
           AND STDDEV(ph.yes_price) > %s
    """
    interval = f"{ACTIVE_DAYS_WINDOW} days"
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (interval, MIN_HISTORY_POINTS, MIN_PRICE_STDDEV))
        ids = [r[0] for r in cur.fetchall()]
    log.debug("_load_active_market_ids: %d markets (window=%s, stddev>%.3f)",
              len(ids), interval, MIN_PRICE_STDDEV)
    return ids


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_price_series(
    market_ids: list[str],
    n_points: int = MAX_HISTORY_POINTS,
) -> tuple[dict[str, list[float]], dict[str, Optional[str]], dict[str, Optional[str]]]:
    """
    Load the last n_points yes_price values for each market_id.
    Returns (series, end_dates, categories) for pre-filtering.

    Only returns markets with >= MIN_HISTORY_POINTS data points.
    """
    if not market_ids:
        return {}, {}, {}

    placeholders = ",".join(["%s"] * len(market_ids))

    sql_series = f"""
        WITH ranked AS (
            SELECT
                market_id,
                yes_price,
                ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY recorded_at DESC) AS rn
            FROM price_history
            WHERE market_id IN ({placeholders})
        )
        SELECT market_id, yes_price
        FROM ranked
        WHERE rn <= %s
        ORDER BY market_id, rn DESC
    """

    sql_meta = f"""
        SELECT market_id, end_date::TEXT,
               raw_json->'events'->0->'series'->0->>'slug' AS series_slug,
               raw_json->'events'->0->>'id'                AS event_id
        FROM markets
        WHERE market_id IN ({placeholders})
    """

    series: dict[str, list[float]] = {}
    end_dates: dict[str, Optional[str]] = {}
    categories: dict[str, Optional[str]] = {}  # series_slug or event_id fallback

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql_series, market_ids + [n_points])
        for mid, price in cur.fetchall():
            series.setdefault(mid, []).append(price)

        cur.execute(sql_meta, market_ids)
        for mid, ed, slug, ev_id in cur.fetchall():
            end_dates[mid] = str(ed)[:10] if ed else None
            # Use event_id as primary group key (most specific, always same-event pairs)
            # Fall back to series slug if no event_id, so same-sport markets can still pair
            if ev_id:
                categories[mid] = f"event:{ev_id}"
            elif slug:
                categories[mid] = f"series:{slug}"
            else:
                categories[mid] = None

    # Filter: only keep markets with enough history
    filtered = {
        mid: prices
        for mid, prices in series.items()
        if len(prices) >= MIN_HISTORY_POINTS
    }

    return filtered, end_dates, categories


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def _pearson(xs: list[float], ys: list[float]) -> Optional[float]:
    """Pearson correlation coefficient. Python 3.9 compatible."""
    n = len(xs)
    if n < 2:
        return None

    mean_x = sum(xs) / n
    mean_y = sum(ys) / n

    cov = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n))
    var_x = sum((v - mean_x) ** 2 for v in xs)
    var_y = sum((v - mean_y) ** 2 for v in ys)

    denom = (var_x * var_y) ** 0.5
    if denom == 0.0:
        return None

    return cov / denom


def _align_series(s1: list[float], s2: list[float]) -> tuple[list[float], list[float]]:
    """Align two series to the same length by trimming the longer one from the start."""
    n = min(len(s1), len(s2))
    return s1[-n:], s2[-n:]


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def compute_pair_rhos(
    series: dict[str, list[float]],
    end_dates: dict[str, Optional[str]],
    categories: Optional[dict[str, Optional[str]]] = None,
    min_abs_rho: float = 0.5,
) -> dict[tuple[str, str], float]:
    """
    Compute Pearson ρ for all market pairs with |ρ| >= min_abs_rho.
    Pre-filters by end_date AND category to cut O(N²) pairs.
    Returns {(mid_a, mid_b): rho} with mid_a < mid_b lexicographically.
    """
    result: dict[tuple[str, str], float] = {}
    market_ids = list(series.keys())

    for i in range(len(market_ids)):
        for j in range(i + 1, len(market_ids)):
            mid_a = market_ids[i]
            mid_b = market_ids[j]

            ed_a = end_dates.get(mid_a)
            ed_b = end_dates.get(mid_b)
            if ed_a and ed_b and ed_a != ed_b:
                continue

            # Skip cross-category pairs (e.g. crypto vs politics)
            if categories is not None:
                cat_a = categories.get(mid_a)
                cat_b = categories.get(mid_b)
                if cat_a and cat_b and cat_a != cat_b:
                    continue

            s1, s2 = _align_series(series[mid_a], series[mid_b])
            if len(s1) < MIN_HISTORY_POINTS:
                continue

            rho = _pearson(s1, s2)
            if rho is not None and abs(rho) >= min_abs_rho:
                key = (mid_a, mid_b) if mid_a < mid_b else (mid_b, mid_a)
                result[key] = rho

    return result


def build_correlation_graph(
    series: dict[str, list[float]],
    end_dates: dict[str, Optional[str]],
    categories: Optional[dict[str, Optional[str]]] = None,
    threshold: float = CORR_THRESHOLD,
) -> dict[str, set[str]]:
    """
    Build an adjacency graph: edge (A, B) iff |ρ(A, B)| >= threshold.
    """
    pair_rhos = compute_pair_rhos(series, end_dates, categories, min_abs_rho=threshold)
    adjacency: dict[str, set[str]] = {mid: set() for mid in series}
    for (mid_a, mid_b), rho in pair_rhos.items():
        if abs(rho) >= threshold:
            adjacency[mid_a].add(mid_b)
            adjacency[mid_b].add(mid_a)
    return adjacency


# ---------------------------------------------------------------------------
# Connected components (BFS)
# ---------------------------------------------------------------------------

def find_connected_components(adjacency: dict[str, set[str]]) -> list[list[str]]:
    """
    BFS over the adjacency graph.
    Returns only components with size >= 2 (isolated nodes excluded).
    """
    visited: set[str] = set()
    components: list[list[str]] = []

    for start in adjacency:
        if start in visited:
            continue
        component: list[str] = []
        queue: deque[str] = deque([start])
        visited.add(start)
        while queue:
            node = queue.popleft()
            component.append(node)
            for neighbor in adjacency[node]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
        if len(component) >= 2:
            components.append(component)

    return components


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_correlated_groups(
    market_ids: Optional[list[str]] = None,
) -> tuple[list[list[str]], bool]:
    """
    Compute correlation-based groups for the given market_ids.

    Returns:
        (groups, is_correlation_based)

    groups: list of lists of market_ids that are correlated.
    is_correlation_based: True if we have enough history to use correlation;
                          False = cold start, caller should fall back to event_id.
    """
    global _cache

    now = time.monotonic()

    # Cache hit
    cache_key = tuple(sorted(market_ids)) if market_ids else "__all__"
    if cache_key in _cache:
        ts, result = _cache[cache_key]
        if now - ts < _CACHE_TTL:
            return result

    # Load from DB — active markets only when no explicit list given
    ids_to_load = list(market_ids) if market_ids else _load_active_market_ids()

    series, end_dates, categories = load_price_series(ids_to_load)

    if len(series) < 2:
        log.debug("correlation_grouper: cold start (%d markets with history)", len(series))
        result = ([], False)
        _cache[cache_key] = (now, result)
        return result

    adjacency = build_correlation_graph(series, end_dates, categories)
    groups = find_connected_components(adjacency)

    log.debug(
        "correlation_grouper: %d groups from %d markets (threshold=%.2f)",
        len(groups), len(series), CORR_THRESHOLD,
    )

    result = (groups, True)
    _cache[cache_key] = (now, result)
    return result


def get_correlated_pairs(
    market_ids: Optional[list[str]] = None,
    min_abs_rho: float = 0.5,
) -> list[tuple[str, str, float]]:
    """
    Return all pairs (market_id_a, market_id_b, rho) with |ρ| >= min_abs_rho.
    Used by correlation_arb.py to detect arbitrage from signed ρ values.
    Cold-start safe: returns [] when not enough history.
    """
    global _pairs_cache

    now = time.monotonic()
    cache_key = (tuple(sorted(market_ids)) if market_ids else "__all__", min_abs_rho)
    if cache_key in _pairs_cache:
        ts, result = _pairs_cache[cache_key]
        if now - ts < _CACHE_TTL:
            return result

    ids_to_load = list(market_ids) if market_ids else _load_active_market_ids()

    series, end_dates, categories = load_price_series(ids_to_load)
    if len(series) < 2:
        result = []
        _pairs_cache[cache_key] = (now, result)
        return result

    pair_rhos = compute_pair_rhos(series, end_dates, categories, min_abs_rho=min_abs_rho)
    result = [(a, b, rho) for (a, b), rho in pair_rhos.items()]

    log.debug("get_correlated_pairs: %d pairs (min_abs_rho=%.2f)", len(result), min_abs_rho)
    _pairs_cache[cache_key] = (now, result)
    return result


def get_market_to_group_map(
    market_ids: Optional[list[str]] = None,
) -> tuple[dict[str, int], bool]:
    """
    Convenience wrapper: returns ({market_id: group_index}, is_correlation_based).
    Markets not in any group are absent from the dict (isolated nodes).
    """
    groups, is_corr = get_correlated_groups(market_ids)
    mapping: dict[str, int] = {}
    for idx, group in enumerate(groups):
        for mid in group:
            mapping[mid] = idx
    return mapping, is_corr
