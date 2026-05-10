-- Migration: retention and compression policies for Synology DS920+
-- 4GB RAM, limited disk — aggressive compression + tiered retention

-- 1) Compression policies (continuous aggregates + raw hypertables)
-- Trade data: compress after 3 days (active trading window), drop after 30 days
ALTER TABLE trades SET (timescaledb.compress, timescaledb.compress_segmentby = 'exchange,market_type');
SELECT add_compression_policy('trades', INTERVAL '3 days');
SELECT add_retention_policy('trades', INTERVAL '30 days');

-- Liquidations: same as trades
ALTER TABLE liquidations SET (timescaledb.compress, timescaledb.compress_segmentby = 'exchange');
SELECT add_compression_policy('liquidations', INTERVAL '3 days');
SELECT add_retention_policy('liquidations', INTERVAL '30 days');

-- Orderbook snapshots: high volume, compress after 1 day, keep 7 days
ALTER TABLE orderbook_snapshots SET (timescaledb.compress, timescaledb.compress_segmentby = 'exchange,market_type');
SELECT add_compression_policy('orderbook_snapshots', INTERVAL '1 day');
SELECT add_retention_policy('orderbook_snapshots', INTERVAL '7 days');

-- Market stats: lower volume, keep 90 days
ALTER TABLE market_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'exchange,market_type');
SELECT add_compression_policy('market_stats', INTERVAL '7 days');
SELECT add_retention_policy('market_stats', INTERVAL '90 days');

-- 2) Continuous aggregates for long-term analytics (immune to raw retention)
-- Hourly trade aggregates: keep 90 days
CREATE MATERIALIZED VIEW trades_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', ts) AS bucket,
    exchange,
    market_type,
    symbol,
    count(*) AS trade_count,
    sum(qty) AS total_volume,
    sum(qty * price) AS total_value,
    avg(price) AS avg_price,
    min(price) AS min_price,
    max(price) AS max_price
FROM trades
GROUP BY bucket, exchange, market_type, symbol;

SELECT add_continuous_aggregate_policy('trades_1h',
    start_offset => INTERVAL '90 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Daily trade aggregates: keep 1 year
CREATE MATERIALIZED VIEW trades_1d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', ts) AS bucket,
    exchange,
    market_type,
    symbol,
    count(*) AS trade_count,
    sum(qty) AS total_volume,
    sum(qty * price) AS total_value,
    avg(price) AS avg_price,
    min(price) AS min_price,
    max(price) AS max_price
FROM trades
GROUP BY bucket, exchange, market_type, symbol;

SELECT add_continuous_aggregate_policy('trades_1d',
    start_offset => INTERVAL '1 year',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');

-- 3) Indexes on continuous aggregates for fast queries
CREATE INDEX idx_trades_1h_bucket ON trades_1h (bucket DESC, exchange, market_type);
CREATE INDEX idx_trades_1d_bucket ON trades_1d (bucket DESC, exchange, market_type);
