package metrics

import (
	"sync"
	"time"
)

// TradeEvent is a minimal copy of exchange.Trade for metrics aggregation.
type TradeEvent struct {
	Exchange   string
	Symbol     string
	MarketType string
	Price      float64
	Qty        float64
	QuoteQty   float64
	Side       string
	Timestamp  time.Time
}

// LiquidationEvent represents a liquidation for metrics.
type LiquidationEvent struct {
	Exchange   string
	Symbol     string
	MarketType string
	Side       string
	Qty        float64
	QuoteQty   float64
	Timestamp  time.Time
}

// WindowKey identifies a metric window (exchange + market_type).
type WindowKey struct {
	Exchange   string
	MarketType string
}

// CVDWindow holds a sliding window of trade events for a single key.
type CVDWindow struct {
	mu     sync.RWMutex
	window time.Duration
	evts   []TradeEvent
}

// NewCVDWindow creates a CVDWindow with the given duration.
func NewCVDWindow(window time.Duration) *CVDWindow {
	return &CVDWindow{window: window}
}

// Add inserts a trade event and prunes events older than the window.
func (w *CVDWindow) Add(e TradeEvent) {
	w.mu.Lock()
	defer w.mu.Unlock()
	cutoff := time.Now().UTC().Add(-w.window)
	// prune
	i := 0
	for i < len(w.evts) && w.evts[i].Timestamp.Before(cutoff) {
		i++
	}
	if i > 0 {
		w.evts = append(w.evts[:0:0], w.evts[i:]...)
	}
	w.evts = append(w.evts, e)
}

// Snapshot returns buyVolume, sellVolume, delta, and cvd (cumulative) for the current window.
// cvdStart is the cumulative delta from previous windows (maintained by the caller).
func (w *CVDWindow) Snapshot(cvdStart float64) (buyVol, sellVol, delta, cvd float64) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	cutoff := time.Now().UTC().Add(-w.window)
	for _, e := range w.evts {
		if e.Timestamp.Before(cutoff) {
			continue
		}
		if e.Side == "buy" {
			buyVol += e.Qty
		} else if e.Side == "sell" {
			sellVol += e.Qty
		}
	}
	delta = buyVol - sellVol
	cvd = cvdStart + delta
	return
}

// LiquidationWindow holds a sliding window of liquidation events.
type LiquidationWindow struct {
	mu     sync.RWMutex
	window time.Duration
	evts   []LiquidationEvent
}

// NewLiquidationWindow creates a LiquidationWindow.
func NewLiquidationWindow(window time.Duration) *LiquidationWindow {
	return &LiquidationWindow{window: window}
}

// Add inserts a liquidation event.
func (w *LiquidationWindow) Add(e LiquidationEvent) {
	w.mu.Lock()
	defer w.mu.Unlock()
	cutoff := time.Now().UTC().Add(-w.window)
	i := 0
	for i < len(w.evts) && w.evts[i].Timestamp.Before(cutoff) {
		i++
	}
	if i > 0 {
		w.evts = append(w.evts[:0:0], w.evts[i:]...)
	}
	w.evts = append(w.evts, e)
}

// Snapshot returns long and short liquidation volumes and counts.
func (w *LiquidationWindow) Snapshot() (longVol, shortVol float64, longCount, shortCount int64) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	cutoff := time.Now().UTC().Add(-w.window)
	for _, e := range w.evts {
		if e.Timestamp.Before(cutoff) {
			continue
		}
		if e.Side == "buy" { // in crypto, buy liquidation = long liquidated
			longVol += e.Qty
			longCount++
		} else if e.Side == "sell" { // sell liquidation = short liquidated
			shortVol += e.Qty
			shortCount++
		}
	}
	return
}
