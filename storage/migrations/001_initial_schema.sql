-- Core market-data schema. All time-series tables are hypertables.
-- Dedupe strategy: unique indexes + ON CONFLICT DO NOTHING at insert time,
-- so reconnect replays never double-count.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================
-- Perps: trades
-- ============================================================
CREATE TABLE IF NOT EXISTS trades (
    ts              timestamptz      NOT NULL,
    venue           text             NOT NULL,
    symbol          text             NOT NULL,
    trade_id        text             NOT NULL,
    price           double precision NOT NULL,
    qty             double precision NOT NULL,
    -- true = buyer was the passive side, i.e. an aggressive SELL hit the bid
    is_buyer_maker  boolean          NOT NULL
);
SELECT create_hypertable('trades', 'ts', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS trades_dedupe_idx ON trades (venue, symbol, trade_id, ts);
CREATE INDEX IF NOT EXISTS trades_venue_symbol_ts_idx ON trades (venue, symbol, ts DESC);

ALTER TABLE trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'venue, symbol',
    timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('trades', INTERVAL '7 days', if_not_exists => TRUE);
-- Raw ticks kept 1 year; 1m/5m continuous aggregates outlive them.
SELECT add_retention_policy('trades', INTERVAL '365 days', if_not_exists => TRUE);

-- ============================================================
-- Perps: liquidations (Binance forceOrder / Bybit liquidation)
-- ============================================================
CREATE TABLE IF NOT EXISTS liquidations (
    ts          timestamptz      NOT NULL,
    venue       text             NOT NULL,
    symbol      text             NOT NULL,
    -- side of the forced order: SELL = a long got liquidated, BUY = a short
    side        text             NOT NULL,
    price       double precision NOT NULL,
    qty         double precision NOT NULL
);
SELECT create_hypertable('liquidations', 'ts', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS liquidations_venue_symbol_ts_idx ON liquidations (venue, symbol, ts DESC);

ALTER TABLE liquidations SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'venue, symbol',
    timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('liquidations', INTERVAL '30 days', if_not_exists => TRUE);
-- No retention policy: liquidation history is small and central to the research.

-- ============================================================
-- Perps: funding / mark price (1s stream)
-- ============================================================
CREATE TABLE IF NOT EXISTS funding (
    ts                timestamptz      NOT NULL,
    venue             text             NOT NULL,
    symbol            text             NOT NULL,
    funding_rate      double precision,
    mark_price        double precision,
    index_price       double precision,
    next_funding_time timestamptz
);
SELECT create_hypertable('funding', 'ts', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS funding_dedupe_idx ON funding (venue, symbol, ts);

ALTER TABLE funding SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'venue, symbol',
    timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('funding', INTERVAL '7 days', if_not_exists => TRUE);

-- ============================================================
-- Perps: open interest (REST poll)
-- ============================================================
CREATE TABLE IF NOT EXISTS open_interest (
    ts                  timestamptz      NOT NULL,
    venue               text             NOT NULL,
    symbol              text             NOT NULL,
    open_interest       double precision NOT NULL,
    open_interest_value double precision
);
SELECT create_hypertable('open_interest', 'ts', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS open_interest_dedupe_idx ON open_interest (venue, symbol, ts);

ALTER TABLE open_interest SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'venue, symbol',
    timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('open_interest', INTERVAL '30 days', if_not_exists => TRUE);

-- ============================================================
-- Perps: L2 orderbook snapshots (1s), heaviest table by far
-- ============================================================
CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    ts      timestamptz NOT NULL,
    venue   text        NOT NULL,
    symbol  text        NOT NULL,
    -- [[price, qty], ...] best-first, depth capped by the collector
    bids    jsonb       NOT NULL,
    asks    jsonb       NOT NULL
);
SELECT create_hypertable('orderbook_snapshots', 'ts', chunk_time_interval => INTERVAL '6 hours', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS orderbook_venue_symbol_ts_idx ON orderbook_snapshots (venue, symbol, ts DESC);

ALTER TABLE orderbook_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'venue, symbol',
    timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('orderbook_snapshots', INTERVAL '2 days', if_not_exists => TRUE);
-- 90 days raw depth; derived features (imbalance, spread) should be extracted
-- to Parquet before this window rolls off.
SELECT add_retention_policy('orderbook_snapshots', INTERVAL '90 days', if_not_exists => TRUE);

-- ============================================================
-- Polymarket: markets, fills, wallet position snapshots
-- ============================================================
CREATE TABLE IF NOT EXISTS pm_markets (
    market_id    text PRIMARY KEY,
    question     text,
    category     text,
    end_date     timestamptz,
    active       boolean,
    liquidity    double precision,
    volume       double precision,
    raw          jsonb,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pm_fills (
    ts         timestamptz      NOT NULL,
    market_id  text             NOT NULL,
    asset_id   text,
    maker      text,
    taker      text,
    side       text,
    price      double precision NOT NULL,
    size       double precision NOT NULL,
    tx_hash    text
);
SELECT create_hypertable('pm_fills', 'ts', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS pm_fills_dedupe_idx ON pm_fills (market_id, tx_hash, taker, price, size, ts);
CREATE INDEX IF NOT EXISTS pm_fills_taker_ts_idx ON pm_fills (taker, ts DESC);

ALTER TABLE pm_fills SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'market_id',
    timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('pm_fills', INTERVAL '30 days', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS pm_positions (
    ts         timestamptz      NOT NULL,
    wallet     text             NOT NULL,
    market_id  text             NOT NULL,
    outcome    text,
    size       double precision NOT NULL,
    avg_price  double precision
);
SELECT create_hypertable('pm_positions', 'ts', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS pm_positions_wallet_ts_idx ON pm_positions (wallet, ts DESC);

-- ============================================================
-- Ops: collector health + gap log
-- ============================================================
CREATE TABLE IF NOT EXISTS collector_heartbeats (
    collector  text PRIMARY KEY,
    last_seen  timestamptz NOT NULL,
    msg_count  bigint      NOT NULL DEFAULT 0,
    details    jsonb
);

-- Every known hole in the data. A gap that isn't here is an *unexplained* gap,
-- which is the thing Milestone 1 forbids.
CREATE TABLE IF NOT EXISTS data_gaps (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    venue       text        NOT NULL,
    channel     text        NOT NULL,
    symbol      text,
    gap_start   timestamptz NOT NULL,
    gap_end     timestamptz,
    reason      text        NOT NULL,
    details     jsonb,
    detected_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS data_gaps_venue_channel_idx ON data_gaps (venue, channel, gap_start DESC);

-- Dead-collector check: anything not seen for 5 minutes.
-- Poll this from the health monitor / alerting job.
CREATE OR REPLACE VIEW dead_collectors AS
SELECT collector, last_seen, now() - last_seen AS silent_for
FROM collector_heartbeats
WHERE last_seen < now() - INTERVAL '5 minutes';
