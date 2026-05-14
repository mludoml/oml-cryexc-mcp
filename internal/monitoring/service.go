package monitoring

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"oml-aggr-mcp/internal/exchange"
	"oml-aggr-mcp/internal/store"
)

type Service struct {
	mu        sync.RWMutex
	startedAt time.Time

	store        *store.Store
	connectors   []exchange.Connector
	heartbeatInterval time.Duration
}

func NewService(s *store.Store, connectors []exchange.Connector) *Service {
	return &Service{
		startedAt:         time.Now().UTC(),
		store:             s,
		connectors:        connectors,
		heartbeatInterval: 15 * time.Second,
	}
}

func (svc *Service) Start(ctx context.Context) {
	ticker := time.NewTicker(svc.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			svc.emitSnapshot(ctx)
		}
	}
}

func (svc *Service) emitSnapshot(ctx context.Context) {
	now := time.Now().UTC()
	uptimeSec := int(now.Sub(svc.startedAt).Seconds())

	var statuses []store.ExchangeStatusRecord
	var upCount, degradedCount, downCount int
	for _, c := range svc.connectors {
		lastMsg := c.LastMessageAt()
		lastTrade := c.LastTradeAt()
		reconnects := c.Reconnects()
		var downtimeSince *time.Time
		if d := c.DowntimeSince(); d != nil {
			t := *d
			downtimeSince = &t
		}

		status := svc.classifyStatus(lastMsg, reconnects)
		switch status {
		case "up":
			upCount++
		case "degraded":
			degradedCount++
		case "down":
			downCount++
		}

		var lastTradeAt, lastMessageAt *time.Time
		if !lastTrade.IsZero() {
			t := lastTrade
			lastTradeAt = &t
		}
		if !lastMsg.IsZero() {
			t := lastMsg
			lastMessageAt = &t
		}

		statuses = append(statuses, store.ExchangeStatusRecord{
			Time:          now,
			Exchange:      c.Name(),
			Connected:     status == "up" || status == "degraded",
			PairsCount:    len(c.MarketTypes()),
			TradesPerMin:  0,
			LastTradeAt:   lastTradeAt,
			LastMessageAt: lastMessageAt,
			Reconnects:    reconnects,
			DowntimeSince: downtimeSince,
			Status:        status,
			StatusReason:  c.StatusReason(),
		})
	}

	if err := svc.store.InsertExchangeStatusBatch(ctx, statuses); err != nil {
		slog.Warn("insert exchange_status batch error", "err", err)
	}

	dbConnected := svc.store.Ping(ctx) == nil
	heartbeat := store.SystemHeartbeatRecord{
		Time:          now,
		StartedAt:     svc.startedAt,
		UptimeSec:     uptimeSec,
		Status:        svc.classifyGlobalStatus(upCount, degradedCount, downCount),
		DBConnected:   dbConnected,
		ExchangeCount: len(svc.connectors),
		UpCount:       upCount,
		DegradedCount: degradedCount,
		DownCount:     downCount,
	}
	if err := svc.store.InsertSystemHeartbeatBatch(ctx, []store.SystemHeartbeatRecord{heartbeat}); err != nil {
		slog.Warn("insert system_heartbeat error", "err", err)
	}
}

func (svc *Service) classifyStatus(lastMsg time.Time, reconnects int) string {
	if lastMsg.IsZero() {
		return "down"
	}
	elapsed := time.Since(lastMsg)
	switch {
	case elapsed < 30*time.Second:
		return "up"
	case elapsed < 2*time.Minute:
		return "degraded"
	default:
		return "down"
	}
}

func (svc *Service) classifyGlobalStatus(up, degraded, down int) string {
	total := up + degraded + down
	if total == 0 {
		return "unknown"
	}
	if down == 0 && degraded == 0 {
		return "healthy"
	}
	if float64(down)/float64(total) > 0.5 {
		return "critical"
	}
	return "degraded"
}

func (svc *Service) RecordEvent(ctx context.Context, rec store.SystemEventRecord) {
	if err := svc.store.InsertSystemEventsBatch(ctx, []store.SystemEventRecord{rec}); err != nil {
		slog.Warn("insert system_events error", "err", err)
	}
}

func (svc *Service) StartedAt() time.Time { return svc.startedAt }
func (svc *Service) UptimeSec() int       { return int(time.Since(svc.startedAt).Seconds()) }
