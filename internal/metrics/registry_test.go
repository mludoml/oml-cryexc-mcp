package metrics

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSnapshotAllJSON(t *testing.T) {
	r := NewRegistry(60 * time.Second)
	r.RecordTrade(TradeEvent{
		Exchange:   "BINANCE",
		Symbol:     "btcusdt",
		MarketType: "spot",
		Price:      65000,
		Qty:        0.1,
		QuoteQty:   6500,
		Side:       "buy",
		Timestamp:  time.Now().UTC(),
	})
	per, global, liqs := r.SnapshotAll()
	resp := map[string]interface{}{
		"perExchange":  per,
		"global":       global,
		"liquidations": liqs,
	}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("marshal produced empty output")
	}
	t.Logf("output: %s", string(b))
}
