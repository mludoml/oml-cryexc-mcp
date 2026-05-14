SELECT create_hypertable(
  'trades', 'time',
  chunk_time_interval => INTERVAL '1 hour',
  if_not_exists => TRUE
);

ALTER TABLE trades SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'exchange, market_type',
  timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('trades', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('trades', INTERVAL '365 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS trades_time_exchange_idx ON trades (time DESC, exchange, market_type);
CREATE INDEX IF NOT EXISTS trades_pair_time_idx ON trades (pair, time DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS trades_1m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 minute', time)                           AS bucket,
  exchange,
  market_type,
  SUM(size) FILTER (WHERE side = 'buy')                  AS buy_volume,
  SUM(size) FILTER (WHERE side = 'sell')                 AS sell_volume,
  SUM(size * CASE WHEN side = 'buy' THEN 1 ELSE -1 END) AS delta,
  SUM(size) FILTER (WHERE liquidation)                   AS liq_volume,
  COUNT(*)                                               AS trade_count
FROM trades
GROUP BY bucket, exchange, market_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
  'trades_1m',
  start_offset => INTERVAL '10 minutes',
  end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute'
);
