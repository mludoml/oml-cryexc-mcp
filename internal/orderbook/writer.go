package orderbook

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"oml-aggr-mcp/internal/store"
)

// Writer batches order book snapshots to TimescaleDB.
type Writer struct {
	store    *store.Store
	buf      []Snapshot
	bufMu    sync.Mutex
	flushInt time.Duration
}

// NewWriter creates a Writer that flushes every interval.
func NewWriter(s *store.Store, interval time.Duration) *Writer {
	return &Writer{
		store:    s,
		flushInt: interval,
	}
}

// Start begins the flush loop.
func (w *Writer) Start(ctx context.Context) {
	ticker := time.NewTicker(w.flushInt)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.flush()
			return
		case <-ticker.C:
			w.flush()
		}
	}
}

func (w *Writer) flush() {
	w.bufMu.Lock()
	snaps := w.buf
	w.buf = nil
	w.bufMu.Unlock()

	if len(snaps) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := w.store.CopyFrom(ctx, pgx.Identifier{"orderbook_snapshots"},
		[]string{"time", "exchange", "pair", "best_bid", "best_ask", "spread", "mid_price", "imbalance", "bid_depth", "ask_depth", "levels_top"},
		store.CopyFromSlice(len(snaps), func(i int) ([]any, error) {
			s := snaps[i]
			levelsTop, _ := json.Marshal(map[string]interface{}{
				"top_bids": s.TopBids,
				"top_asks": s.TopAsks,
			})
			return []any{
				s.Timestamp, s.Exchange, s.Pair,
				s.BestBid, s.BestAsk, s.Spread, s.MidPrice,
				s.Imbalance, s.BidDepth, s.AskDepth,
				levelsTop,
			}, nil
		}),
	); err != nil {
		slog.Error("orderbook flush error", "err", err, "count", len(snaps))
		w.requeue(snaps)
	}
}

func (w *Writer) requeue(snaps []Snapshot) {
	if len(snaps) == 0 {
		return
	}
	w.bufMu.Lock()
	defer w.bufMu.Unlock()
	w.buf = append(w.buf, snaps...)
}

// Push adds a snapshot to the batch buffer.
func (w *Writer) Push(s Snapshot) {
	w.bufMu.Lock()
	defer w.bufMu.Unlock()
	w.buf = append(w.buf, s)
}
