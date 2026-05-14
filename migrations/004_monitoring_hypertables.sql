SELECT create_hypertable(
  'exchange_status', 'time',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE
);

SELECT create_hypertable(
  'system_heartbeat', 'time',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE
);

SELECT create_hypertable(
  'system_events', 'time',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS exchange_status_time_exchange_idx ON exchange_status (time DESC, exchange);
CREATE INDEX IF NOT EXISTS system_heartbeat_time_idx ON system_heartbeat (time DESC);
CREATE INDEX IF NOT EXISTS system_events_time_component_idx ON system_events (time DESC, component);
