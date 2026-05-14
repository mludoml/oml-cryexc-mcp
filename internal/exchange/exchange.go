package exchange

import (
	"context"
	"time"

	"oml-aggr-mcp/internal/config"
)

// Trade represents a single trade tick
type Trade struct {
	Exchange      string
	Symbol        string
	MarketType    string // spot, perp
	Price         float64
	Qty           float64
	QuoteQty      float64
	Side          string // buy, sell
	IsBuyerMaker  bool
	IsLiquidation bool
	Timestamp     time.Time
	TradeID       string
}

// OrderbookLevel represents a single price level
type OrderbookLevel struct {
	Price  float64
	BidQty float64
	AskQty float64
}

// OrderbookSnapshot represents a snapshot of the orderbook
type OrderbookSnapshot struct {
	Exchange   string
	Symbol     string
	MarketType string
	TickSize   float64
	Levels     []OrderbookLevel
	Timestamp  time.Time
}

// Liquidation represents a liquidation event
type Liquidation struct {
	Exchange   string
	Symbol     string
	MarketType string
	Side       string
	Price      float64
	Qty        float64
	QuoteQty   float64
	Timestamp  time.Time
}

// MarketStat represents a market stats update
type MarketStat struct {
	Exchange          string
	Symbol            string
	MarketType        string
	MarkPrice         float64
	IndexPrice        float64
	FundingRate       float64
	NextFundingTime   time.Time
	OpenInterest      float64
	LongShortRatio    float64
	LongAccountRatio  float64
	ShortAccountRatio float64
	Timestamp         time.Time
}

// Connector is the interface for exchange data sources
type Connector interface {
	Name() string
	MarketTypes() []string // e.g. ["spot", "perp"]
	Connect(markets []config.MarketConfig) error
	Disconnect()
	Run(ctx context.Context) error
	LastMessageAt() time.Time
	LastTradeAt() time.Time
	Reconnects() int
	DowntimeSince() *time.Time
	StatusReason() string
	
	// Callbacks set by the hub
	OnTrade(cb func(Trade))
	OnOrderbookSnapshot(cb func(OrderbookSnapshot))
	OnLiquidation(cb func(Liquidation))
	OnMarketStat(cb func(MarketStat))
}
