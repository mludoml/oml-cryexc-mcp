ALTER TABLE exchange_status
  ADD COLUMN IF NOT EXISTS trades_per_min INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_trade_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_message_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reconnects INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS downtime_since TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'down',
  ADD COLUMN IF NOT EXISTS status_reason TEXT NOT NULL DEFAULT 'unknown';

CREATE TABLE IF NOT EXISTS system_heartbeat (
  time            TIMESTAMPTZ NOT NULL,
  started_at      TIMESTAMPTZ NOT NULL,
  uptime_sec      INTEGER     NOT NULL,
  status          TEXT        NOT NULL,
  db_connected    BOOLEAN     NOT NULL,
  exchange_count  INTEGER     NOT NULL,
  up_count        INTEGER     NOT NULL,
  degraded_count  INTEGER     NOT NULL,
  down_count      INTEGER     NOT NULL
);

CREATE TABLE IF NOT EXISTS system_events (
  time        TIMESTAMPTZ NOT NULL,
  component   TEXT        NOT NULL,
  event_type  TEXT        NOT NULL,
  status      TEXT        NOT NULL,
  reason      TEXT        NOT NULL,
  details     JSONB       NOT NULL DEFAULT '{}'::jsonb
);
