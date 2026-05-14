ALTER TABLE exchange_status SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'exchange',
  timescaledb.compress_orderby = 'time DESC'
);

ALTER TABLE system_heartbeat SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'time DESC'
);

ALTER TABLE system_events SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'component',
  timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('exchange_status', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('system_heartbeat', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('system_events', INTERVAL '7 days', if_not_exists => TRUE);

SELECT add_retention_policy('exchange_status', INTERVAL '365 days', if_not_exists => TRUE);
SELECT add_retention_policy('system_heartbeat', INTERVAL '365 days', if_not_exists => TRUE);
SELECT add_retention_policy('system_events', INTERVAL '365 days', if_not_exists => TRUE);
