from __future__ import annotations
"""
Displays the market groups detected by exhaustive_arb and numeric_arb.
Usage: python3 show_groups.py
"""

import os
from dotenv import load_dotenv
load_dotenv()

from exhaustive_arb import _load_markets, _group_markets
from numeric_arb import parse_market

# ─── Exhaustive groups (by event_id) ──────────────────────────────────────────
rows = _load_markets()
groups = _group_markets(rows)

print(f"\n{'='*70}")
print(f" EXHAUSTIVE GROUPS  ({len(groups)} groups, {len(rows)} markets total)")
print(f"{'='*70}")

for (event_id, event_title, end_date), markets in sorted(groups.items(), key=lambda x: -len(x[1])):
    yes_sum = sum(m.yes_price for m in markets)
    flag = ""
    if yes_sum < 1.0:
        flag = f"  ← BUY ALL YES (sum={yes_sum:.3f})"
    elif yes_sum > 1.0:
        flag = f"  ← BUY ALL NO  (sum={yes_sum:.3f})"
    else:
        flag = f"  (sum={yes_sum:.3f})"

    print(f"\n[{len(markets)} markets] {event_title[:60]}  end={end_date}{flag}")
    for m in markets:
        print(f"   p={m.yes_price:.3f}  {m.question[:80]}")

# ─── Numeric groups (asset regex + threshold) ─────────────────────────────────
from collections import defaultdict
num_groups: dict = defaultdict(list)
skipped = 0

for row in rows:
    pm = parse_market(row)
    if pm and pm.event_id:
        key = (pm.event_id, pm.asset, pm.direction, pm.end_date)
        num_groups[key].append(pm)
    elif pm:
        skipped += 1

multi = {k: v for k, v in num_groups.items() if len(v) >= 2}

print(f"\n\n{'='*70}")
print(f" NUMERIC GROUPS  ({len(multi)} groups with ≥2 markets, {skipped} without event_id)")
print(f"{'='*70}")

for (event_id, asset, direction, end_date), markets in sorted(multi.items(), key=lambda x: -len(x[1])):
    markets.sort(key=lambda m: m.threshold)
    print(f"\n[{len(markets)} markets] {asset} {direction.upper()}  end={end_date}")
    for m in markets:
        print(f"   threshold={m.threshold:>12,.0f}  p={m.yes_price:.3f}  {m.question[:70]}")
