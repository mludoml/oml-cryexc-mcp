CREATE TABLE IF NOT EXISTS orderbook_snapshots (
  time         TIMESTAMPTZ      NOT NULL,
  exchange     TEXT             NOT NULL,
  pair         TEXT             NOT NULL,
  best_bid     DOUBLE PRECISION,
  best_ask     DOUBLE PRECISION,
  spread       DOUBLE PRECISION,
  mid_price    DOUBLE PRECISION,
  imbalance    DOUBLE PRECISION,
  bid_depth    DOUBLE PRECISION,
  ask_depth    DOUBLE PRECISION,
  levels_top   JSONB,

  symbol       TEXT,
  market_type  TEXT,
  tick_size    DOUBLE PRECISION,
  price        DOUBLE PRECISION,
  bid_qty      DOUBLE PRECISION,
  ask_qty      DOUBLE PRECISION
);

SELECT create_hypertable(
  'orderbook_snapshots', 'time',
  chunk_time_interval => INTERVAL '1 hour',
  if_not_exists => TRUE
);

ALTER TABLE orderbook_snapshots SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'exchange, pair',
  timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('orderbook_snapshots', INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('orderbook_snapshots', INTERVAL '7 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS orderbook_snapshots_time_exchange_pair_idx ON orderbook_snapshots (time DESC, exchange, pair);
