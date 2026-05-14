CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS trades (
  time         TIMESTAMPTZ      NOT NULL,
  exchange     TEXT             NOT NULL,
  pair         TEXT             NOT NULL,
  market_type  TEXT             NOT NULL CHECK (market_type IN ('spot', 'perp')),
  price        DOUBLE PRECISION NOT NULL,
  size         DOUBLE PRECISION NOT NULL,
  side         TEXT             NOT NULL CHECK (side IN ('buy', 'sell')),
  liquidation  BOOLEAN          NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS exchange_status (
  time         TIMESTAMPTZ NOT NULL,
  exchange     TEXT        NOT NULL,
  connected    BOOLEAN     NOT NULL,
  pairs_count  INTEGER     NOT NULL
);

CREATE TABLE IF NOT EXISTS liquidations (
  time         TIMESTAMPTZ      NOT NULL,
  exchange     TEXT             NOT NULL,
  symbol       TEXT             NOT NULL,
  market_type  TEXT             NOT NULL,
  side         TEXT             NOT NULL,
  price        DOUBLE PRECISION NOT NULL,
  qty          DOUBLE PRECISION NOT NULL,
  quote_qty    DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS market_stats (
  time                TIMESTAMPTZ      NOT NULL,
  exchange            TEXT             NOT NULL,
  symbol              TEXT             NOT NULL,
  market_type         TEXT             NOT NULL,
  mark_price          DOUBLE PRECISION,
  index_price         DOUBLE PRECISION,
  funding_rate        DOUBLE PRECISION,
  next_funding_time   TIMESTAMPTZ,
  open_interest       DOUBLE PRECISION,
  long_short_ratio    DOUBLE PRECISION,
  long_account_ratio  DOUBLE PRECISION,
  short_account_ratio DOUBLE PRECISION
);
