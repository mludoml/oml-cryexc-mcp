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
	"oml-aggr-mcp/internal/config"
)

// BinanceConnector connects to Binance WebSocket APIs
type BinanceConnector struct {
	ConnectorRuntime
	name         string
	spotWSURL    string
	futuresWSURL string
	markets      []config.MarketConfig
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
	lastUpdateID float64
	bids         map[string]float64
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

func (b *BinanceConnector) Connect(markets []config.MarketConfig) error {
	if len(markets) == 0 {
		return fmt.Errorf("no markets configured")
	}
	b.markets = append([]config.MarketConfig(nil), markets...)
	first := markets[0]
	b.symbol = strings.ToLower(first.Pair)
	b.marketType = string(first.Type)
	for _, market := range markets {
		if market.Type != first.Type {
			continue
		}
		symbol := strings.ToLower(market.Pair)
		b.orderbooks[symbol] = &binanceOrderbook{
			bids: make(map[string]float64),
			asks: make(map[string]float64),
		}
	}
	return nil
}

func (b *BinanceConnector) Disconnect() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.running = false
	b.MarkDisconnected("shutdown")
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
	b.MarkConnecting("connecting")
	
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
			delay := b.MarkReconnectScheduled("reconnect_scheduled")
			select {
			case <-b.ctx.Done():
				return nil
			case <-time.After(delay):
				continue
			}
		}
	}
}

func (b *BinanceConnector) connectAndStream() error {
	streams := b.buildStreams()
	wsURL := b.spotWSURL
	path := "/stream"
	if b.marketType == "perp" {
		wsURL = b.futuresWSURL
		path = "/market/stream"
	}
	
	u, _ := url.Parse(wsURL)
	u.Path = path
	q := u.Query()
	q.Set("streams", strings.Join(streams, "/"))
	u.RawQuery = q.Encode()
	
	slog.Info("connecting to binance", "url", u.String()[:80])
	
	ws, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		b.MarkDisconnected("dial_error")
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	b.ws = ws
	ws.SetPingHandler(func(appData string) error {
		deadline := time.Now().Add(5 * time.Second)
		return ws.WriteControl(websocket.PongMessage, []byte(appData), deadline)
	})
	b.MarkConnected("connected")
	
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
			b.MarkDisconnected("read_error")
			return fmt.Errorf("read: %w", err)
		}
		b.MarkMessageReceived()
		
		if err := b.handleMessage(msg); err != nil {
			slog.Warn("handle message error", "err", err)
		}
	}
}

func (b *BinanceConnector) buildStreams() []string {
	symbols := make([]string, 0, len(b.markets))
	seen := make(map[string]struct{})
	for _, market := range b.markets {
		if string(market.Type) != b.marketType {
			continue
		}
		symbol := strings.ToLower(market.Pair)
		if _, ok := seen[symbol]; ok {
			continue
		}
		seen[symbol] = struct{}{}
		symbols = append(symbols, symbol)
	}
	if len(symbols) == 0 && b.symbol != "" {
		symbols = append(symbols, strings.ToLower(b.symbol))
	}

	streams := make([]string, 0, len(symbols)*3)
	if b.marketType == "spot" {
		for _, symbol := range symbols {
			streams = append(streams,
				symbol+"@trade",
				symbol+"@depth@100ms",
				symbol+"@ticker",
			)
		}
		return streams
	}
	for _, symbol := range symbols {
		streams = append(streams,
			symbol+"@aggTrade",
			symbol+"@depth@100ms",
			symbol+"@markPrice@1s",
			symbol+"@forceOrder",
		)
	}
	return streams
}

func (b *BinanceConnector) handleMessage(msg []byte) error {
	var wrapper struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return b.handleSingleMessage(msg)
	}
	return b.handleSingleMessage(wrapper.Data)
}

func (b *BinanceConnector) handleSingleMessage(data []byte) error {
	var typeDetect map[string]json.RawMessage
	if err := json.Unmarshal(data, &typeDetect); err != nil {
		return err
	}
	
	eventType := ""
	if eVal, ok := typeDetect["e"]; ok {
		var eStr string
		json.Unmarshal(eVal, &eStr)
		eventType = eStr
	}

	switch eventType {
	case "trade", "aggTrade":
		return b.handleTrade(data)
	case "depthUpdate":
		return b.handleDepthUpdate(data)
	case "markPriceUpdate":
		return b.handleMarkPrice(data)
	case "forceOrder":
		return b.handleLiquidation(data)
	case "24hrTicker":
		return nil
	default:
		if eventType == "" {
			var tickerDetect struct {
				C string `json:"c"`
				V string `json:"v"`
			}
			if err := json.Unmarshal(data, &tickerDetect); err == nil && tickerDetect.C != "" {
				return nil
			}
		}
		return nil
	}
}

func (b *BinanceConnector) handleTrade(data []byte) error {
	var t struct {
		S string  `json:"s"`
		P string  `json:"p"`
		Q string  `json:"q"`
		M bool    `json:"m"`
		A float64 `json:"a"`
		T float64 `json:"T"`
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
		Symbol:       strings.ToLower(t.S),
		MarketType:   b.marketType,
		Price:        price,
		Qty:          qty,
		QuoteQty:     price * qty,
		Side:         side,
		IsBuyerMaker: t.M,
		Timestamp:    time.UnixMilli(int64(t.T)).UTC(),
	}
	b.MarkTrade(trade.Timestamp)
	
	if b.onTrade != nil {
		b.onTrade(trade)
	}
	return nil
}

func (b *BinanceConnector) handleDepthUpdate(data []byte) error {
	var d struct {
		S  string      `json:"s"`
		U  float64     `json:"U"`
		UF float64     `json:"u"`
		Pu float64     `json:"pu"`
		B  [][2]string `json:"b"`
		A  [][2]string `json:"a"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	
	ob := b.orderbooks[strings.ToLower(d.S)]
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
		S string `json:"s"`
		P string `json:"p"`
		I string `json:"i"`
		r string `json:"r"`
		T float64  `json:"T"`
	}
	if err := json.Unmarshal(data, &mp); err != nil {
		return err
	}
	
	markPrice, _ := strconv.ParseFloat(mp.P, 64)
	indexPrice, _ := strconv.ParseFloat(mp.I, 64)
	fundingRate, _ := strconv.ParseFloat(mp.r, 64)
	
	stat := MarketStat{
		Exchange:        b.name,
		Symbol:          strings.ToLower(mp.S),
		MarketType:      b.marketType,
		MarkPrice:       markPrice,
		IndexPrice:      indexPrice,
		FundingRate:     fundingRate,
		NextFundingTime: time.UnixMilli(int64(mp.T)).UTC(),
		Timestamp:       time.UnixMilli(int64(mp.T)).UTC(),
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
		Symbol:     strings.ToLower(liq.S),
		MarketType: b.marketType,
		Side:       strings.ToLower(side),
		Price:      p,
		Qty:        q,
		QuoteQty:   p * q,
		Timestamp:  time.UnixMilli(liq.T).UTC(),
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
	for symbol, ob := range b.orderbooks {
		ob.mu.RLock()

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

			rounded := math.Round(price/tickSize) * tickSize
			levels = append(levels, OrderbookLevel{
				Price:  rounded,
				BidQty: ob.bids[p],
				AskQty: ob.asks[p],
			})
		}

		ob.mu.RUnlock()

		if b.onOrderbookSnapshot != nil {
			b.onOrderbookSnapshot(OrderbookSnapshot{
				Exchange:   b.name,
				Symbol:     symbol,
				MarketType: b.marketType,
				TickSize:   tickSize,
				Levels:     levels,
				Timestamp:  time.Now().UTC(),
			})
		}
	}
}
