package store

import (
	"context"
	"encoding/json"
	"time"
)

type ExchangeStatusRecord struct {
	Time          time.Time
	Exchange      string
	Connected     bool
	PairsCount    int
	TradesPerMin  int
	LastTradeAt   *time.Time
	LastMessageAt *time.Time
	Reconnects    int
	DowntimeSince *time.Time
	Status        string
	StatusReason  string
}

type SystemHeartbeatRecord struct {
	Time          time.Time
	StartedAt     time.Time
	UptimeSec     int
	Status        string
	DBConnected   bool
	ExchangeCount int
	UpCount       int
	DegradedCount int
	DownCount     int
}

type SystemEventRecord struct {
	Time      time.Time
	Component string
	EventType string
	Status    string
	Reason    string
	Details   map[string]any
}

func (s *Store) InsertExchangeStatusBatch(ctx context.Context, statuses []ExchangeStatusRecord) error {
	if len(statuses) == 0 {
		return nil
	}

	_, err := s.pool.CopyFrom(ctx,
		[]string{"exchange_status"},
		[]string{"time", "exchange", "connected", "pairs_count", "trades_per_min", "last_trade_at", "last_message_at", "reconnects", "downtime_since", "status", "status_reason"},
		copyFromSlice(len(statuses), func(i int) ([]any, error) {
			record := statuses[i]
			return []any{record.Time, record.Exchange, record.Connected, record.PairsCount, record.TradesPerMin, record.LastTradeAt, record.LastMessageAt, record.Reconnects, record.DowntimeSince, record.Status, record.StatusReason}, nil
		}),
	)
	return err
}

func (s *Store) InsertSystemHeartbeatBatch(ctx context.Context, heartbeats []SystemHeartbeatRecord) error {
	if len(heartbeats) == 0 {
		return nil
	}

	_, err := s.pool.CopyFrom(ctx,
		[]string{"system_heartbeat"},
		[]string{"time", "started_at", "uptime_sec", "status", "db_connected", "exchange_count", "up_count", "degraded_count", "down_count"},
		copyFromSlice(len(heartbeats), func(i int) ([]any, error) {
			record := heartbeats[i]
			return []any{record.Time, record.StartedAt, record.UptimeSec, record.Status, record.DBConnected, record.ExchangeCount, record.UpCount, record.DegradedCount, record.DownCount}, nil
		}),
	)
	return err
}

func (s *Store) InsertSystemEventsBatch(ctx context.Context, events []SystemEventRecord) error {
	if len(events) == 0 {
		return nil
	}

	_, err := s.pool.CopyFrom(ctx,
		[]string{"system_events"},
		[]string{"time", "component", "event_type", "status", "reason", "details"},
		copyFromSlice(len(events), func(i int) ([]any, error) {
			record := events[i]
			details, marshalErr := json.Marshal(record.Details)
			if marshalErr != nil {
				return nil, marshalErr
			}
			return []any{record.Time, record.Component, record.EventType, record.Status, record.Reason, details}, nil
		}),
	)
	return err
}
