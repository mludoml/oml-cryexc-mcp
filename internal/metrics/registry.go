package metrics

import (
	"sync"
	"time"
)

// Registry holds sliding-window CVD, liquidation, and global metrics per exchange+market_type.
type Registry struct {
	mu            sync.RWMutex
	defaultWindow time.Duration
	cvd           map[WindowKey]*CVDWindow
	liqs          map[WindowKey]*LiquidationWindow
	// global aggregates
	globalCVD  *CVDWindow
	globalLiqs *LiquidationWindow
}

// NewRegistry creates a new metrics registry with the given default window.
func NewRegistry(defaultWindow time.Duration) *Registry {
	return &Registry{
		defaultWindow: defaultWindow,
		cvd:           make(map[WindowKey]*CVDWindow),
		liqs:          make(map[WindowKey]*LiquidationWindow),
		globalCVD:     NewCVDWindow(defaultWindow),
		globalLiqs:    NewLiquidationWindow(defaultWindow),
	}
}

func (r *Registry) cvdWindow(key WindowKey) *CVDWindow {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.cvd[key]
	if !ok {
		w = NewCVDWindow(r.defaultWindow)
		r.cvd[key] = w
	}
	return w
}

func (r *Registry) liqWindow(key WindowKey) *LiquidationWindow {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.liqs[key]
	if !ok {
		w = NewLiquidationWindow(r.defaultWindow)
		r.liqs[key] = w
	}
	return w
}

// RecordTrade adds a trade to both per-key and global CVD windows.
func (r *Registry) RecordTrade(e TradeEvent) {
	r.globalCVD.Add(e)
	r.cvdWindow(WindowKey{Exchange: e.Exchange, MarketType: e.MarketType}).Add(e)
}

// RecordLiquidation adds a liquidation to both per-key and global liquidation windows.
func (r *Registry) RecordLiquidation(e LiquidationEvent) {
	r.globalLiqs.Add(e)
	r.liqWindow(WindowKey{Exchange: e.Exchange, MarketType: e.MarketType}).Add(e)
}

// PerExchangeSnapshot returns CVD data for a single exchange+market_type pair.
type PerExchangeSnapshot struct {
	Exchange   string  `json:"exchange"`
	MarketType string  `json:"market_type"`
	BuyVolume  float64 `json:"buy_volume"`
	SellVolume float64 `json:"sell_volume"`
	Delta      float64 `json:"delta"`
	CVD        float64 `json:"cvd"`
}

// GlobalSnapshot returns aggregate CVD data across all exchanges.
type GlobalSnapshot struct {
	BuyVolume  float64 `json:"buy_volume"`
	SellVolume float64 `json:"sell_volume"`
	Delta      float64 `json:"delta"`
	CVD        float64 `json:"cvd"`
}

// LiquidationSnapshot returns liquidation volumes and counts per exchange and globally.
type LiquidationSnapshot struct {
	PerExchange map[WindowKey]struct {
		LongVol   float64 `json:"long_vol"`
		ShortVol  float64 `json:"short_vol"`
		LongCount int64   `json:"long_count"`
		ShortCount int64  `json:"short_count"`
	} `json:"per_exchange"`
	Global struct {
		LongVol   float64 `json:"long_vol"`
		ShortVol  float64 `json:"short_vol"`
		LongCount int64   `json:"long_count"`
		ShortCount int64  `json:"short_count"`
	} `json:"global"`
}

// SnapshotAll returns per-exchange and global CVD snapshots, plus liquidation data.
// cvdStart is a map of cumulative delta offsets per key (caller maintains).
func (r *Registry) SnapshotAll() (
	perExchange []PerExchangeSnapshot,
	global GlobalSnapshot,
	liqs LiquidationSnapshot,
) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	global.BuyVolume, global.SellVolume, global.Delta, global.CVD =
		r.globalCVD.Snapshot(0)

	for key, w := range r.cvd {
		buy, sell, delta, cvd := w.Snapshot(0)
		perExchange = append(perExchange, PerExchangeSnapshot{
			Exchange:   key.Exchange,
			MarketType: key.MarketType,
			BuyVolume:  buy,
			SellVolume: sell,
			Delta:      delta,
			CVD:        cvd,
		})
	}

	liqs.PerExchange = make(map[WindowKey]struct {
		LongVol    float64 `json:"long_vol"`
		ShortVol   float64 `json:"short_vol"`
		LongCount  int64   `json:"long_count"`
		ShortCount int64   `json:"short_count"`
	})
	for key, w := range r.liqs {
		lv, sv, lc, sc := w.Snapshot()
		liqs.PerExchange[key] = struct {
			LongVol    float64 `json:"long_vol"`
			ShortVol   float64 `json:"short_vol"`
			LongCount  int64   `json:"long_count"`
			ShortCount int64   `json:"short_count"`
		}{LongVol: lv, ShortVol: sv, LongCount: lc, ShortCount: sc}
	}
	liqs.Global.LongVol, liqs.Global.ShortVol, liqs.Global.LongCount, liqs.Global.ShortCount =
		r.globalLiqs.Snapshot()

	return
}
