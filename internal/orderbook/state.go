package orderbook

import (
	"sort"
	"sync"
	"time"
)

// Level represents one price level in the order book.
type Level struct {
	Price float64 `json:"price"`
	Qty   float64 `json:"qty"`
}

// Snapshot holds a single snapshot of the order book for a (exchange, pair).
type Snapshot struct {
	Exchange  string    `json:"exchange"`
	Pair      string    `json:"pair"`
	BestBid   float64   `json:"best_bid"`
	BestAsk   float64   `json:"best_ask"`
	Spread    float64   `json:"spread"`
	MidPrice  float64   `json:"mid_price"`
	Imbalance float64   `json:"imbalance"`
	BidDepth  float64   `json:"bid_depth"`
	AskDepth  float64   `json:"ask_depth"`
	TopBids   []Level   `json:"top_bids"`
	TopAsks   []Level   `json:"top_asks"`
	Timestamp time.Time `json:"timestamp"`
}

// State holds live order book state for one (exchange, pair).
type State struct {
	mu   sync.RWMutex
	bids map[float64]float64 // price -> qty (0 = remove)
	asks map[float64]float64 // price -> qty (0 = remove)
}

// NewState creates an empty order book state.
func NewState() *State {
	return &State{
		bids: make(map[float64]float64),
		asks: make(map[float64]float64),
	}
}

// ApplyDelta updates levels from an exchange delta message.
func (s *State) ApplyDelta(bidUpdates, askUpdates []Level) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range bidUpdates {
		if l.Qty == 0 {
			delete(s.bids, l.Price)
		} else {
			s.bids[l.Price] = l.Qty
		}
	}
	for _, l := range askUpdates {
		if l.Qty == 0 {
			delete(s.asks, l.Price)
		} else {
			s.asks[l.Price] = l.Qty
		}
	}
}

// Snapshot builds a Snapshot with top-N levels and precomputed metrics.
func (s *State) Snapshot(exchange, pair string, topN int) Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bidPrices := make([]float64, 0, len(s.bids))
	for p := range s.bids {
		bidPrices = append(bidPrices, p)
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(bidPrices)))

	askPrices := make([]float64, 0, len(s.asks))
	for p := range s.asks {
		askPrices = append(askPrices, p)
	}
	sort.Float64s(askPrices)

	bestBid := 0.0
	if len(bidPrices) > 0 {
		bestBid = bidPrices[0]
	}
	bestAsk := 0.0
	if len(askPrices) > 0 {
		bestAsk = askPrices[0]
	}
	spread := bestAsk - bestBid
	midPrice := (bestBid + bestAsk) / 2

	var bidDepth, askDepth float64
	topBids := make([]Level, 0, topN)
	for i, p := range bidPrices {
		if i >= topN {
			bidDepth += s.bids[p] * p
			continue
		}
		topBids = append(topBids, Level{Price: p, Qty: s.bids[p]})
		bidDepth += s.bids[p] * p
	}
	topAsks := make([]Level, 0, topN)
	for i, p := range askPrices {
		if i >= topN {
			askDepth += s.asks[p] * p
			continue
		}
		topAsks = append(topAsks, Level{Price: p, Qty: s.asks[p]})
		askDepth += s.asks[p] * p
	}

	imbalance := 0.0
	if askDepth > 0 {
		imbalance = bidDepth / askDepth
	}

	return Snapshot{
		Exchange:  exchange,
		Pair:      pair,
		BestBid:   bestBid,
		BestAsk:   bestAsk,
		Spread:    spread,
		MidPrice:  midPrice,
		Imbalance: imbalance,
		BidDepth:  bidDepth,
		AskDepth:  askDepth,
		TopBids:   topBids,
		TopAsks:   topAsks,
		Timestamp: time.Now().UTC(),
	}
}
