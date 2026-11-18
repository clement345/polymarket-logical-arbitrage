# Polymarket Logical Arbitrage

A layered system that detects **model-free (logical) arbitrage** on the Polymarket prediction
market: price combinations that violate the laws of probability and therefore lock in a profit
whatever the outcome.

## Arbitrage types
| Layer | File | What it detects |
|---|---|---|
| 1: Ingestion | `ingestion.py` | Pulls all active markets from the Gamma API into PostgreSQL every 60 s |
| 2: Numeric monotonicity | `numeric_arb.py` | Threshold markets out of order, e.g. P(BTC > 110k) > P(BTC > 100k) |
| 3: Exhaustive groups | `exhaustive_arb.py` | Mutually exclusive outcomes whose prices sum below / above 1 |
| Correlation | `correlation_grouper.py`, `correlation_arb.py` | Groups related markets and checks joint consistency |
| 4: Execution | `execution.py` | Order placement with risk checks: **`DRY_RUN=true` by default** |
| Real-time prices | `price_feed.py` | Multi-connection CLOB WebSocket feed |
| Validation | `paper_trading.py` | Paper trades with snapshots at T+5 s / 30 s / 60 s / 5 min to measure decay |

Every opportunity is checked **after fees** (worst-case taker fee) and against order-book depth
before it is reported.

## Quick start
```bash
cp .env.example .env              # only DATABASE_URL is required
docker compose up -d              # PostgreSQL (or use a local instance)
psql "$DATABASE_URL" -f schema.sql
pip install -r requirements.txt
python ingestion.py               # terminal 1
python paper_trading.py           # terminal 2
```

## Status
Research prototype. Detection and paper trading are implemented; live execution stays disabled
until the paper-trading validation criteria (≥ 50 trades, win rate ≥ 70 %, Sharpe ≥ 1) are met.

Related research: [prediction-market-efficiency](https://github.com/clement345/prediction-market-efficiency).
