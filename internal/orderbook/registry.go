package orderbook

import (
	"context"
	"sync"
	"time"
)

// Registry holds per-(exchange, pair) order book states and schedules snapshots.
type Registry struct {
	mu      sync.RWMutex
	states  map[string]*State // key = exchange+"|"+pair
	snapInt time.Duration
	topN    int
}

// NewRegistry creates a Registry that snapshots every interval.
func NewRegistry(snapInterval time.Duration, topN int) *Registry {
	return &Registry{
		states:  make(map[string]*State),
		snapInt: snapInterval,
		topN:    topN,
	}
}

// ApplyDelta updates the order book for a given exchange+pair.
func (r *Registry) ApplyDelta(exchange, pair string, bidUpdates, askUpdates []Level) {
	r.mu.Lock()
	key := exchange + "|" + pair
	st, ok := r.states[key]
	if !ok {
		st = NewState()
		r.states[key] = st
	}
	r.mu.Unlock()
	st.ApplyDelta(bidUpdates, askUpdates)
}

// GetState returns a copy snapshot of the current top-N levels.
func (r *Registry) GetState(exchange, pair string) Snapshot {
	r.mu.RLock()
	key := exchange + "|" + pair
	st, ok := r.states[key]
	r.mu.RUnlock()
	if !ok {
		return Snapshot{Exchange: exchange, Pair: pair}
	}
	return st.Snapshot(exchange, pair, r.topN)
}

// Start begins the snapshot scheduler loop.
func (r *Registry) Start(ctx context.Context) {
	ticker := time.NewTicker(r.snapInt)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.snapshotAll()
		}
	}
}

func (r *Registry) snapshotAll() {
	r.mu.RLock()
	keys := make([]string, 0, len(r.states))
	for k := range r.states {
		keys = append(keys, k)
	}
	r.mu.RUnlock()

	for _, key := range keys {
		r.mu.RLock()
		st, ok := r.states[key]
		r.mu.RUnlock()
		if !ok {
			continue
		}
		parts := splitKey(key)
		_ = st.Snapshot(parts[0], parts[1], r.topN)
	}
}

func splitKey(key string) [2]string {
	for i := 0; i < len(key); i++ {
		if key[i] == '|' {
			return [2]string{key[:i], key[i+1:]}
		}
	}
	return [2]string{key, ""}
}
