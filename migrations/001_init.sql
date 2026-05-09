CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Trades: all ticks from all exchanges
CREATE TABLE IF NOT EXISTS trades (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(32) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    market_type VARCHAR(16) NOT NULL, -- spot, perp
    price DOUBLE PRECISION NOT NULL,
    qty DOUBLE PRECISION NOT NULL,
    quote_qty DOUBLE PRECISION NOT NULL,
    side VARCHAR(4) NOT NULL, -- buy, sell
    is_buyer_maker BOOLEAN NOT NULL,
    is_liquidation BOOLEAN NOT NULL DEFAULT FALSE,
    trade_id VARCHAR(64)
);

SELECT create_hypertable('trades', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 hour');

CREATE INDEX idx_trades_lookup ON trades (exchange, symbol, market_type, time DESC);

-- Orderbook snapshots: periodic snapshots of top levels per exchange
CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(32) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    market_type VARCHAR(16) NOT NULL,
    tick_size DOUBLE PRECISION NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    bid_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
    ask_qty DOUBLE PRECISION NOT NULL DEFAULT 0
);

SELECT create_hypertable('orderbook_snapshots', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 hour');

CREATE INDEX idx_ob_snapshots_lookup ON orderbook_snapshots (exchange, symbol, market_type, tick_size, time DESC);

-- Liquidations
CREATE TABLE IF NOT EXISTS liquidations (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(32) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    market_type VARCHAR(16) NOT NULL,
    side VARCHAR(4) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    qty DOUBLE PRECISION NOT NULL,
    quote_qty DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('liquidations', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 hour');

CREATE INDEX idx_liq_lookup ON liquidations (exchange, symbol, market_type, time DESC);

-- Market stats: funding, OI, mark/index price (perp only)
CREATE TABLE IF NOT EXISTS market_stats (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(32) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    market_type VARCHAR(16) NOT NULL,
    mark_price DOUBLE PRECISION,
    index_price DOUBLE PRECISION,
    funding_rate DOUBLE PRECISION,
    next_funding_time TIMESTAMPTZ,
    open_interest DOUBLE PRECISION,
    long_short_ratio DOUBLE PRECISION,
    long_account_ratio DOUBLE PRECISION,
    short_account_ratio DOUBLE PRECISION
);

SELECT create_hypertable('market_stats', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 hour');

CREATE INDEX idx_stats_lookup ON market_stats (exchange, symbol, market_type, time DESC);

-- CVD candles: pre-computed per exchange + aggregate
CREATE TABLE IF NOT EXISTS cvd_candles (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(32) NOT NULL, -- 'AGGREGATE' for all combined
    symbol VARCHAR(32) NOT NULL,
    market_type VARCHAR(16) NOT NULL,
    interval_ms BIGINT NOT NULL,
    buy_vol DOUBLE PRECISION NOT NULL DEFAULT 0,
    sell_vol DOUBLE PRECISION NOT NULL DEFAULT 0,
    delta DOUBLE PRECISION NOT NULL DEFAULT 0,
    cum_delta DOUBLE PRECISION NOT NULL DEFAULT 0
);

SELECT create_hypertable('cvd_candles', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 hour');

CREATE INDEX idx_cvd_lookup ON cvd_candles (exchange, symbol, market_type, interval_ms, time DESC);