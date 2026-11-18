CREATE TABLE IF NOT EXISTS markets (
    market_id    TEXT        PRIMARY KEY,
    question     TEXT        NOT NULL,
    outcomes     JSONB,
    end_date     TIMESTAMPTZ,
    category     TEXT,
    tokens       JSONB,
    raw_json     JSONB       NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_markets_end_date  ON markets (end_date);
CREATE INDEX IF NOT EXISTS idx_markets_category  ON markets (category);

-- Paper trading: one row per detected opportunity
CREATE TABLE IF NOT EXISTS paper_trades (
    id              SERIAL      PRIMARY KEY,
    opp_type        TEXT        NOT NULL,               -- "numeric" | "exhaustive"
    opp_key         TEXT        NOT NULL,               -- sha256 of market IDs (dedup)
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    opp_json        JSONB       NOT NULL,               -- full serialized opportunity
    gross_spread    FLOAT       NOT NULL,
    total_fees      FLOAT       NOT NULL,
    net_profit_est  FLOAT       NOT NULL,               -- per unit, at detection
    budget_usdc     FLOAT       NOT NULL,               -- simulated capital allocated
    snapshot_5s     JSONB,                              -- {market_id: yes_price} at T+5s
    snapshot_30s    JSONB,
    snapshot_60s    JSONB,
    snapshot_5m     JSONB,
    simulated_pnl   FLOAT,                             -- net P&L in USDC (set at resolve)
    false_positive  BOOL,                              -- spread gone by T+5s
    status          TEXT        NOT NULL DEFAULT 'open' -- "open" | "resolved"
);

CREATE INDEX IF NOT EXISTS idx_paper_trades_status  ON paper_trades (status);
CREATE INDEX IF NOT EXISTS idx_paper_trades_opp_key ON paper_trades (opp_key, status);
CREATE INDEX IF NOT EXISTS idx_paper_trades_date    ON paper_trades (detected_at);

-- Price history for correlation-based market grouping
CREATE TABLE IF NOT EXISTS price_history (
    id          BIGSERIAL   PRIMARY KEY,
    market_id   TEXT        NOT NULL REFERENCES markets(market_id) ON DELETE CASCADE,
    yes_price   FLOAT       NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_price_history_market_time
    ON price_history (market_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_price_history_recorded_at
    ON price_history (recorded_at);

-- Strategy live tester: one row per open/resolved position
CREATE TABLE IF NOT EXISTS strat_test_positions (
    id              SERIAL      PRIMARY KEY,
    strategy        TEXT        NOT NULL,               -- "buy_both_no" | "spread_trade"
    opp_key         TEXT        NOT NULL,               -- dedup hash
    market_id_a     TEXT        NOT NULL,               -- long YES (spread) or NO (buy_both_no)
    market_id_b     TEXT        NOT NULL,               -- long NO  (spread) or NO (buy_both_no)
    question_a      TEXT        NOT NULL DEFAULT '',
    question_b      TEXT        NOT NULL DEFAULT '',
    rho             FLOAT       NOT NULL,
    cost            FLOAT       NOT NULL,               -- total entry cost per unit
    budget_usdc     FLOAT       NOT NULL,
    yes_price_a_in  FLOAT       NOT NULL,               -- entry price market A
    yes_price_b_in  FLOAT       NOT NULL,               -- entry price market B
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ,
    yes_price_a_out FLOAT,                             -- 0 or 1 at resolution
    yes_price_b_out FLOAT,
    realized_pnl    FLOAT,
    status          TEXT        NOT NULL DEFAULT 'open' -- "open" | "resolved" | "expired"
);

CREATE INDEX IF NOT EXISTS idx_strat_test_status ON strat_test_positions (status, strategy);
