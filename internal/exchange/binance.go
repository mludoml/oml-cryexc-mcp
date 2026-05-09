package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// BinanceConnector connects to Binance WebSocket APIs
type BinanceConnector struct {
	name         string
	spotWSURL    string
	futuresWSURL string
	symbol       string
	marketType   string
	
	ws       *websocket.Conn
	mu       sync.RWMutex
	running  bool
	ctx      context.Context
	cancel   context.CancelFunc
	
	// Orderbook state (for full depth rebuild)
	orderbooks map[string]*binanceOrderbook // key: "spot" or "perp"
	
	onTrade            func(Trade)
	onOrderbookSnapshot func(OrderbookSnapshot)
	onLiquidation      func(Liquidation)
	onMarketStat       func(MarketStat)
}

type binanceOrderbook struct {
	lastUpdateID int64
	bids         map[string]float64 // price -> qty
	asks         map[string]float64
	mu           sync.RWMutex
}

func NewBinanceConnector() *BinanceConnector {
	return &BinanceConnector{
		name:         "BINANCE",
		spotWSURL:    "wss://stream.binance.com:9443/ws",
		futuresWSURL: "wss://fstream.binance.com/ws",
		orderbooks:   make(map[string]*binanceOrderbook),
	}
}

func (b *BinanceConnector) Name() string { return b.name }

func (b *BinanceConnector) MarketTypes() []string { return []string{"spot", "perp"} }

func (b *BinanceConnector) OnTrade(cb func(Trade))            { b.onTrade = cb }
func (b *BinanceConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { b.onOrderbookSnapshot = cb }
func (b *BinanceConnector) OnLiquidation(cb func(Liquidation)) { b.onLiquidation = cb }
func (b *BinanceConnector) OnMarketStat(cb func(MarketStat))    { b.onMarketStat = cb }

func (b *BinanceConnector) Connect(symbol, marketType string) error {
	b.symbol = strings.ToUpper(symbol)
	b.marketType = marketType
	b.orderbooks[marketType] = &binanceOrderbook{
		bids: make(map[string]float64),
		asks: make(map[string]float64),
	}
	return nil
}

func (b *BinanceConnector) Disconnect() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.running = false
	if b.cancel != nil {
		b.cancel()
	}
	if b.ws != nil {
		b.ws.Close()
	}
}

func (b *BinanceConnector) Run(ctx context.Context) error {
	b.ctx, b.cancel = context.WithCancel(ctx)
	defer b.cancel()
	
	b.mu.Lock()
	b.running = true
	b.mu.Unlock()
	
	for {
		select {
		case <-b.ctx.Done():
			return nil
		default:
		}
		
		if err := b.connectAndStream(); err != nil {
			slog.Error("binance stream error", "exchange", b.name, "market", b.marketType, "err", err)
			select {
			case <-b.ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}
}

func (b *BinanceConnector) connectAndStream() error {
	streams := b.buildStreams()
	wsURL := b.spotWSURL
	if b.marketType == "perp" {
		wsURL = b.futuresWSURL
	}
	
	u, _ := url.Parse(wsURL)
	u.Path = "/stream"
	q := u.Query()
	q.Set("streams", strings.Join(streams, "/"))
	u.RawQuery = q.Encode()
	
	slog.Info("connecting to binance", "url", u.String()[:80])
	
	ws, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	b.ws = ws
	
	// Fetch initial orderbook snapshot for depth rebuild
	if err := b.fetchSnapshot(); err != nil {
		slog.Warn("failed to fetch snapshot", "err", err)
	}
	
	for {
		select {
		case <-b.ctx.Done():
			return nil
		default:
		}
		
		ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		
		if err := b.handleMessage(msg); err != nil {
			slog.Warn("handle message error", "err", err)
		}
	}
}

func (b *BinanceConnector) buildStreams() []string {
	sym := strings.ToLower(b.symbol)
	if b.marketType == "spot" {
		return []string{
			sym + "@trade",
			sym + "@depth@100ms",
			sym + "@ticker",
		}
	}
	// perp
	return []string{
		sym + "@aggTrade",
		sym + "@depth@100ms",
		sym + "@markPrice@1s",
		sym + "@forceOrder",
	}
}

func (b *BinanceConnector) handleMessage(msg []byte) error {
	var wrapper struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(msg, &wrapper); err != nil {
		// Try single message (non-combined stream)
		return b.handleSingleMessage(msg)
	}
	return b.handleSingleMessage(wrapper.Data)
}

func (b *BinanceConnector) handleSingleMessage(data []byte) error {
	// Detect message type
	var typeDetect struct {
		E string `json:"e"` // event type
	}
	if err := json.Unmarshal(data, &typeDetect); err != nil {
		return err
	}
	
	switch typeDetect.E {
	case "trade", "aggTrade":
		return b.handleTrade(data)
	case "depthUpdate":
		return b.handleDepthUpdate(data)
	case "markPriceUpdate":
		return b.handleMarkPrice(data)
	case "forceOrder":
		return b.handleLiquidation(data)
	case "24hrTicker":
		// ticker has no "e" field in some versions, skip for now
		return nil
	default:
		// Try ticker detection by fields
		var tickerDetect struct {
			C string `json:"c"` // last price
			V string `json:"v"` // volume
		}
		if err := json.Unmarshal(data, &tickerDetect); err == nil && tickerDetect.C != "" {
			return nil // skip ticker for now
		}
		return nil
	}
}

func (b *BinanceConnector) handleTrade(data []byte) error {
	var t struct {
		E string `json:"E"` // event time
		S string `json:"s"` // symbol
		P string `json:"p"` // price
		Q string `json:"q"` // qty
		M bool   `json:"m"` // is buyer maker
		A int64  `json:"a"` // agg trade id (perp)
		T int64  `json:"T"` // trade time
	}
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}
	
	price, _ := strconv.ParseFloat(t.P, 64)
	qty, _ := strconv.ParseFloat(t.Q, 64)
	
	side := "buy"
	if t.M {
		side = "sell"
	}
	
	trade := Trade{
		Exchange:     b.name,
		Symbol:       b.symbol,
		MarketType:   b.marketType,
		Price:        price,
		Qty:          qty,
		QuoteQty:     price * qty,
		Side:         side,
		IsBuyerMaker: t.M,
		Timestamp:    time.Unix(0, t.T*1e6),
	}
	
	if b.onTrade != nil {
		b.onTrade(trade)
	}
	return nil
}

func (b *BinanceConnector) handleDepthUpdate(data []byte) error {
	var d struct {
		E  string     `json:"E"`
		S  string     `json:"s"`
		U  int64      `json:"U"`  // first update id
		UF int64      `json:"u"`  // final update id
		Pu int64      `json:"pu"` // previous update id
		B  [][2]string `json:"b"` // bids [price, qty]
		A  [][2]string `json:"a"` // asks [price, qty]
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	
	ob := b.orderbooks[b.marketType]
	if ob == nil {
		return nil
	}
	
	ob.mu.Lock()
	defer ob.mu.Unlock()
	
	// Apply bid updates
	for _, bid := range d.B {
		price := bid[0]
		qty, _ := strconv.ParseFloat(bid[1], 64)
		if qty == 0 {
			delete(ob.bids, price)
		} else {
			ob.bids[price] = qty
		}
	}
	
	// Apply ask updates
	for _, ask := range d.A {
		price := ask[0]
		qty, _ := strconv.ParseFloat(ask[1], 64)
		if qty == 0 {
			delete(ob.asks, price)
		} else {
			ob.asks[price] = qty
		}
	}
	
		ob.lastUpdateID = d.UF
	
	return nil
}

func (b *BinanceConnector) handleMarkPrice(data []byte) error {
	var mp struct {
		E string `json:"E"`
		S string `json:"s"`
		P string `json:"p"` // mark price
		I string `json:"i"` // index price
		r string `json:"r"` // funding rate
		T int64  `json:"T"` // next funding time
	}
	if err := json.Unmarshal(data, &mp); err != nil {
		return err
	}
	
	markPrice, _ := strconv.ParseFloat(mp.P, 64)
	indexPrice, _ := strconv.ParseFloat(mp.I, 64)
	fundingRate, _ := strconv.ParseFloat(mp.r, 64)
	
	stat := MarketStat{
		Exchange:        b.name,
		Symbol:          b.symbol,
		MarketType:      b.marketType,
		MarkPrice:       markPrice,
		IndexPrice:      indexPrice,
		FundingRate:     fundingRate,
		NextFundingTime: time.Unix(0, mp.T*1e6),
		Timestamp:       time.Now(),
	}
	
	if b.onMarketStat != nil {
		b.onMarketStat(stat)
	}
	return nil
}

func (b *BinanceConnector) handleLiquidation(data []byte) error {
	var liq struct {
		E string `json:"E"`
		S string `json:"s"`
		S2 string `json:"S"` // side (in nested "o" for some versions)
		P string `json:"p"` // price
		Q string `json:"q"` // qty
		O struct {
			S string `json:"S"`
			P string `json:"p"`
			Q string `json:"q"`
		} `json:"o"`
		T int64 `json:"T"`
	}
	if err := json.Unmarshal(data, &liq); err != nil {
		return err
	}
	
	side := liq.S2
	if side == "" && liq.O.S != "" {
		side = liq.O.S
	}
	
	price := liq.P
	qty := liq.Q
	if price == "" && liq.O.P != "" {
		price = liq.O.P
	}
	if qty == "" && liq.O.Q != "" {
		qty = liq.O.Q
	}
	
	p, _ := strconv.ParseFloat(price, 64)
	q, _ := strconv.ParseFloat(qty, 64)
	
	liquidation := Liquidation{
		Exchange:   b.name,
		Symbol:     b.symbol,
		MarketType: b.marketType,
		Side:       strings.ToLower(side),
		Price:      p,
		Qty:        q,
		QuoteQty:   p * q,
		Timestamp:  time.Unix(0, liq.T*1e6),
	}
	
	if b.onLiquidation != nil {
		b.onLiquidation(liquidation)
	}
	return nil
}

func (b *BinanceConnector) fetchSnapshot() error {
	// This requires REST API call - we'll implement later
	// For now, depth updates work without initial snapshot (just start from first update)
	return nil
}

// EmitOrderbookSnapshot should be called periodically (e.g. every 500ms-1s) by the hub
func (b *BinanceConnector) EmitOrderbookSnapshot(tickSize float64) {
	ob := b.orderbooks[b.marketType]
	if ob == nil {
		return
	}
	
	ob.mu.RLock()
	
	// Collect all price levels
	allPrices := make(map[string]struct{})
	for p := range ob.bids {
		allPrices[p] = struct{}{}
	}
	for p := range ob.asks {
		allPrices[p] = struct{}{}
	}
	
	var levels []OrderbookLevel
	for p := range allPrices {
		price, _ := strconv.ParseFloat(p, 64)
		if math.IsNaN(price) || price == 0 {
			continue
		}
		
		// Round to tick size
		rounded := math.Round(price/tickSize) * tickSize
		
		bidQty := ob.bids[p]
		askQty := ob.asks[p]
		
		levels = append(levels, OrderbookLevel{
			Price:  rounded,
			BidQty: bidQty,
			AskQty: askQty,
		})
	}
	
	ob.mu.RUnlock()
	
	if b.onOrderbookSnapshot != nil {
		b.onOrderbookSnapshot(OrderbookSnapshot{
			Exchange:   b.name,
			Symbol:     b.symbol,
			MarketType: b.marketType,
			TickSize:   tickSize,
			Levels:     levels,
			Timestamp:  time.Now(),
		})
	}
}
