-- 1m and 5m bars derived from raw trades. Notebooks and backtests read these
-- (or their Parquet exports), never the raw tick table.
-- buy_volume = aggressive buys (taker bought), sell_volume = aggressive sells.

CREATE MATERIALIZED VIEW IF NOT EXISTS trades_1m
WITH (timescaledb.continuous) AS
SELECT
    venue,
    symbol,
    time_bucket('1 minute', ts) AS bucket,
    first(price, ts)            AS open,
    max(price)                  AS high,
    min(price)                  AS low,
    last(price, ts)             AS close,
    sum(qty)                    AS volume,
    sum(price * qty)            AS quote_volume,
    sum(qty) FILTER (WHERE NOT is_buyer_maker) AS buy_volume,
    sum(qty) FILTER (WHERE is_buyer_maker)     AS sell_volume,
    count(*)                    AS trade_count
FROM trades
GROUP BY venue, symbol, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('trades_1m',
    start_offset      => INTERVAL '2 hours',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists     => TRUE);

CREATE MATERIALIZED VIEW IF NOT EXISTS trades_5m
WITH (timescaledb.continuous) AS
SELECT
    venue,
    symbol,
    time_bucket('5 minutes', ts) AS bucket,
    first(price, ts)             AS open,
    max(price)                   AS high,
    min(price)                   AS low,
    last(price, ts)              AS close,
    sum(qty)                     AS volume,
    sum(price * qty)             AS quote_volume,
    sum(qty) FILTER (WHERE NOT is_buyer_maker) AS buy_volume,
    sum(qty) FILTER (WHERE is_buyer_maker)     AS sell_volume,
    count(*)                     AS trade_count
FROM trades
GROUP BY venue, symbol, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('trades_5m',
    start_offset      => INTERVAL '6 hours',
    end_offset        => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE);
