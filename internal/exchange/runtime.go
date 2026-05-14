package exchange

import (
	"sync"
	"time"
)

type ConnectorRuntime struct {
	mu            sync.RWMutex
	lastMessageAt time.Time
	lastTradeAt   time.Time
	reconnects    int
	downtimeSince *time.Time
	statusReason  string
	backoff       Backoff
}

func (r *ConnectorRuntime) LastMessageAt() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastMessageAt
}

func (r *ConnectorRuntime) LastTradeAt() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastTradeAt
}

func (r *ConnectorRuntime) Reconnects() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.reconnects
}

func (r *ConnectorRuntime) DowntimeSince() *time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.downtimeSince == nil {
		return nil
	}
	copy := *r.downtimeSince
	return &copy
}

func (r *ConnectorRuntime) StatusReason() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.statusReason
}

func (r *ConnectorRuntime) MarkConnecting(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	if r.downtimeSince == nil {
		r.downtimeSince = &now
	}
	r.statusReason = reason
}

func (r *ConnectorRuntime) MarkReconnectScheduled(reason string) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	if r.downtimeSince == nil {
		r.downtimeSince = &now
	}
	r.reconnects++
	r.statusReason = reason
	return r.backoff.Next()
}

func (r *ConnectorRuntime) MarkConnected(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backoff.Reset()
	now := time.Now().UTC()
	r.lastMessageAt = now
	r.downtimeSince = nil
	r.statusReason = reason
}

func (r *ConnectorRuntime) MarkMessageReceived() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastMessageAt = time.Now().UTC()
	r.downtimeSince = nil
	if r.statusReason == "" || r.statusReason == "connecting" {
		r.statusReason = "live"
	}
}

func (r *ConnectorRuntime) MarkTrade(ts time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastTradeAt = ts.UTC()
	r.lastMessageAt = time.Now().UTC()
	r.downtimeSince = nil
	r.statusReason = "live"
}

func (r *ConnectorRuntime) MarkDisconnected(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	if r.downtimeSince == nil {
		r.downtimeSince = &now
	}
	r.statusReason = reason
}
