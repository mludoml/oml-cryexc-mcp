package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"oml-aggr-mcp/internal/config"
)

type BybitConnector struct {
	ConnectorRuntime
	name         string
	spotWSURL    string
	linearWSURL  string
	inverseWSURL string
	markets      []config.MarketConfig
	marketType   string

	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc
	sockets map[string]*websocket.Conn

	groups     []bybitSubscriptionGroup
	orderbooks map[string]*bybitOrderbook

	onTrade             func(Trade)
	onOrderbookSnapshot func(OrderbookSnapshot)
	onLiquidation       func(Liquidation)
	onMarketStat        func(MarketStat)
}

type bybitSubscriptionGroup struct {
	key     string
	wsURL   string
	symbols []string
}

type bybitOrderbook struct {
	bids map[string]float64
	asks map[string]float64
	mu   sync.RWMutex
}

func NewBybitConnector() *BybitConnector {
	return &BybitConnector{
		name:         "BYBIT",
		spotWSURL:    "wss://stream.bybit.com/v5/public/spot",
		linearWSURL:  "wss://stream.bybit.com/v5/public/linear",
		inverseWSURL: "wss://stream.bybit.com/v5/public/inverse",
		sockets:      make(map[string]*websocket.Conn),
		orderbooks:   make(map[string]*bybitOrderbook),
	}
}

func (b *BybitConnector) Name() string { return b.name }

func (b *BybitConnector) MarketTypes() []string { return []string{"spot", "perp"} }

func (b *BybitConnector) OnTrade(cb func(Trade)) { b.onTrade = cb }

func (b *BybitConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { b.onOrderbookSnapshot = cb }

func (b *BybitConnector) OnLiquidation(cb func(Liquidation)) { b.onLiquidation = cb }

func (b *BybitConnector) OnMarketStat(cb func(MarketStat)) { b.onMarketStat = cb }

func (b *BybitConnector) Connect(markets []config.MarketConfig) error {
	if len(markets) == 0 {
		return fmt.Errorf("no markets configured")
	}

	b.markets = append([]config.MarketConfig(nil), markets...)
	b.marketType = string(markets[0].Type)
	b.groups = b.groups[:0]
	b.orderbooks = make(map[string]*bybitOrderbook, len(markets))

	spotSymbols := make([]string, 0)
	linearSymbols := make([]string, 0)
	inverseSymbols := make([]string, 0)

	for _, market := range markets {
		symbol := strings.ToUpper(market.Pair)
		b.orderbooks[symbol] = &bybitOrderbook{
			bids: make(map[string]float64),
			asks: make(map[string]float64),
		}

		switch {
		case market.Type == config.MarketTypeSpot:
			spotSymbols = append(spotSymbols, symbol)
		case bybitIsInverseSymbol(symbol):
			inverseSymbols = append(inverseSymbols, symbol)
		default:
			linearSymbols = append(linearSymbols, symbol)
		}
	}

	if len(spotSymbols) > 0 {
		b.groups = append(b.groups, bybitSubscriptionGroup{key: "spot", wsURL: b.spotWSURL, symbols: spotSymbols})
	}
	if len(linearSymbols) > 0 {
		b.groups = append(b.groups, bybitSubscriptionGroup{key: "linear", wsURL: b.linearWSURL, symbols: linearSymbols})
	}
	if len(inverseSymbols) > 0 {
		b.groups = append(b.groups, bybitSubscriptionGroup{key: "inverse", wsURL: b.inverseWSURL, symbols: inverseSymbols})
	}

	if len(b.groups) == 0 {
		return fmt.Errorf("no bybit subscription groups configured")
	}

	return nil
}

func (b *BybitConnector) Disconnect() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.running = false
	b.MarkDisconnected("shutdown")
	if b.cancel != nil {
		b.cancel()
	}
	for key, ws := range b.sockets {
		if ws != nil {
			_ = ws.Close()
		}
		delete(b.sockets, key)
	}
}

func (b *BybitConnector) Run(ctx context.Context) error {
	b.ctx, b.cancel = context.WithCancel(ctx)
	defer b.cancel()
	b.MarkConnecting("connecting")

	b.mu.Lock()
	b.running = true
	b.mu.Unlock()

	for _, group := range b.groups {
		group := group
		go b.runGroup(group)
	}

	<-b.ctx.Done()
	return nil
}

func (b *BybitConnector) runGroup(group bybitSubscriptionGroup) {
	for {
		select {
		case <-b.ctx.Done():
			return
		default:
		}

		if err := b.connectAndStream(group); err != nil {
			slog.Error("bybit stream error", "exchange", b.name, "group", group.key, "err", err)
			delay := b.MarkReconnectScheduled("reconnect_scheduled")
			select {
			case <-b.ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	}
}

func (b *BybitConnector) connectAndStream(group bybitSubscriptionGroup) error {
	slog.Info("connecting to bybit", "url", group.wsURL, "group", group.key, "symbols", group.symbols)

	ws, _, err := websocket.DefaultDialer.Dial(group.wsURL, nil)
	if err != nil {
		b.MarkDisconnected("dial_error")
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	b.setSocket(group.key, ws)
	defer b.clearSocket(group.key)

	if err := ws.WriteJSON(map[string]any{
		"op":    "subscribe",
		"reqId": "audit-" + group.key,
		"args":  b.buildArgs(group.symbols),
	}); err != nil {
		b.MarkDisconnected("subscribe_error")
		return fmt.Errorf("subscribe: %w", err)
	}

	b.MarkConnected("connected")
	go b.pingLoop(ws)

	for {
		select {
		case <-b.ctx.Done():
			return nil
		default:
		}

		_ = ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			b.MarkDisconnected("read_error")
			return fmt.Errorf("read: %w", err)
		}
		b.MarkMessageReceived()

		if err := b.handleMessage(msg); err != nil {
			slog.Warn("bybit handle message", "group", group.key, "err", err)
		}
	}
}

func (b *BybitConnector) setSocket(key string, ws *websocket.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sockets[key] = ws
}

func (b *BybitConnector) clearSocket(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.sockets, key)
}

func (b *BybitConnector) pingLoop(ws *websocket.Conn) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			if err := ws.WriteJSON(map[string]string{"op": "ping"}); err != nil {
				return
			}
		}
	}
}

func (b *BybitConnector) buildArgs(symbols []string) []string {
	args := make([]string, 0, len(symbols)*3)
	for _, symbol := range symbols {
		args = append(args,
			"tickers."+symbol,
			"publicTrade."+symbol,
			"orderbook.1."+symbol,
		)
	}
	return args
}

func (b *BybitConnector) handleMessage(msg []byte) error {
	var wrapper struct {
		Topic   string          `json:"topic"`
		Type    string          `json:"type"`
		Data    json.RawMessage `json:"data"`
		Success bool            `json:"success"`
		RetMsg  string          `json:"ret_msg"`
		Op      string          `json:"op"`
	}

	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return err
	}

	if wrapper.Op == "pong" || wrapper.RetMsg == "pong" {
		return nil
	}

	if !wrapper.Success && wrapper.RetMsg != "" {
		slog.Warn("bybit ws message", "ret_msg", wrapper.RetMsg)
		return nil
	}

	if wrapper.Topic == "" {
		return nil
	}

	switch {
	case strings.HasPrefix(wrapper.Topic, "publicTrade."):
		return b.handleTrade(wrapper.Data)
	case strings.HasPrefix(wrapper.Topic, "orderbook."):
		if wrapper.Type == "snapshot" {
			return b.handleOrderbookSnapshot(wrapper.Data)
		}
		return b.handleOrderbookDelta(wrapper.Data)
	case strings.HasPrefix(wrapper.Topic, "tickers."):
		return b.handleTicker(wrapper.Topic, wrapper.Data)
	case strings.HasPrefix(wrapper.Topic, "liquidation."):
		return b.handleLiquidation(wrapper.Data)
	}

	return nil
}

func (b *BybitConnector) handleTrade(data []byte) error {
	var trades []struct {
		T  json.Number `json:"T"`
		S  string      `json:"s"`
		V  string      `json:"v"`
		P  string      `json:"p"`
		L  string      `json:"L"`
		I  string      `json:"i"`
		BT bool        `json:"BT"`
	}
	if err := json.Unmarshal(data, &trades); err != nil {
		return err
	}

	for _, t := range trades {
		price, _ := strconv.ParseFloat(t.P, 64)
		qty, _ := strconv.ParseFloat(t.V, 64)
		if price == 0 || qty == 0 {
			continue
		}

		symbol := strings.ToUpper(t.S)
		side := strings.ToLower(t.L)
		if side == "" {
			side = "buy"
		}

		trade := Trade{
			Exchange:     b.name,
			Symbol:       symbol,
			MarketType:   bybitMarketType(symbol),
			Price:        price,
			Qty:          qty,
			QuoteQty:     bybitQuoteQty(symbol, price, qty),
			Side:         side,
			IsBuyerMaker: t.BT,
			Timestamp:    bybitTimestamp(t.T),
			TradeID:      t.I,
		}
		b.MarkTrade(trade.Timestamp)
		if b.onTrade != nil {
			b.onTrade(trade)
		}
	}

	return nil
}

func bybitTimestamp(ts json.Number) time.Time {
	ms, _ := ts.Int64()
	return time.UnixMilli(ms).UTC()
}

func bybitMarketType(symbol string) string {
	if strings.Contains(strings.ToUpper(symbol), "SPOT") {
		return "spot"
	}
	return "perp"
}

func bybitIsInverseSymbol(symbol string) bool {
	return strings.EqualFold(symbol, "BTCUSD")
}

func bybitQuoteQty(symbol string, price, qty float64) float64 {
	if bybitIsInverseSymbol(symbol) {
		return qty
	}
	return price * qty
}

func (b *BybitConnector) handleOrderbookSnapshot(data []byte) error {
	var ob struct {
		S string      `json:"s"`
		B [][2]string `json:"b"`
		A [][2]string `json:"a"`
	}
	if err := json.Unmarshal(data, &ob); err != nil {
		return err
	}

	book := b.orderbooks[strings.ToUpper(ob.S)]
	if book == nil {
		return nil
	}

	book.mu.Lock()
	book.bids = make(map[string]float64)
	book.asks = make(map[string]float64)
	for _, bid := range ob.B {
		qty, _ := strconv.ParseFloat(bid[1], 64)
		if qty == 0 {
			continue
		}
		book.bids[bid[0]] = qty
	}
	for _, ask := range ob.A {
		qty, _ := strconv.ParseFloat(ask[1], 64)
		if qty == 0 {
			continue
		}
		book.asks[ask[0]] = qty
	}
	book.mu.Unlock()
	return nil
}

func (b *BybitConnector) handleOrderbookDelta(data []byte) error {
	var ob struct {
		S string      `json:"s"`
		B [][2]string `json:"b"`
		A [][2]string `json:"a"`
	}
	if err := json.Unmarshal(data, &ob); err != nil {
		return err
	}

	book := b.orderbooks[strings.ToUpper(ob.S)]
	if book == nil {
		return nil
	}

	book.mu.Lock()
	for _, bid := range ob.B {
		qty, _ := strconv.ParseFloat(bid[1], 64)
		if qty == 0 {
			delete(book.bids, bid[0])
		} else {
			book.bids[bid[0]] = qty
		}
	}
	for _, ask := range ob.A {
		qty, _ := strconv.ParseFloat(ask[1], 64)
		if qty == 0 {
			delete(book.asks, ask[0])
		} else {
			book.asks[ask[0]] = qty
		}
	}
	book.mu.Unlock()
	return nil
}

func (b *BybitConnector) handleTicker(topic string, data []byte) error {
	var payload struct {
		Data struct {
			Symbol          string `json:"symbol"`
			FundingRate     string `json:"fundingRate"`
			MarkPrice       string `json:"markPrice"`
			IndexPrice      string `json:"indexPrice"`
			OpenInterest    string `json:"openInterest"`
			NextFundingTime int64  `json:"nextFundingTime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	symbol := strings.ToUpper(payload.Data.Symbol)
	if symbol == "" {
		symbol = strings.TrimPrefix(topic, "tickers.")
	}

	mp, _ := strconv.ParseFloat(payload.Data.MarkPrice, 64)
	ip, _ := strconv.ParseFloat(payload.Data.IndexPrice, 64)
	fr, _ := strconv.ParseFloat(payload.Data.FundingRate, 64)
	oi, _ := strconv.ParseFloat(payload.Data.OpenInterest, 64)

	stat := MarketStat{
		Exchange:        b.name,
		Symbol:          symbol,
		MarketType:      bybitMarketType(symbol),
		MarkPrice:       mp,
		IndexPrice:      ip,
		FundingRate:     fr,
		OpenInterest:    oi,
		NextFundingTime: time.UnixMilli(payload.Data.NextFundingTime).UTC(),
		Timestamp:       time.Now().UTC(),
	}

	if b.onMarketStat != nil {
		b.onMarketStat(stat)
	}
	return nil
}

func (b *BybitConnector) handleLiquidation(data []byte) error {
	var liqs struct {
		Data []struct {
			S  string `json:"symbol"`
			Sd string `json:"side"`
			P  string `json:"price"`
			Q  string `json:"size"`
			T  int64  `json:"updatedTime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &liqs); err != nil {
		return err
	}

	for _, l := range liqs.Data {
		price, _ := strconv.ParseFloat(l.P, 64)
		qty, _ := strconv.ParseFloat(l.Q, 64)
		symbol := strings.ToUpper(l.S)

		liq := Liquidation{
			Exchange:   b.name,
			Symbol:     symbol,
			MarketType: bybitMarketType(symbol),
			Side:       strings.ToLower(l.Sd),
			Price:      price,
			Qty:        qty,
			QuoteQty:   bybitQuoteQty(symbol, price, qty),
			Timestamp:  time.UnixMilli(l.T).UTC(),
		}

		if b.onLiquidation != nil {
			b.onLiquidation(liq)
		}
	}

	return nil
}

func (b *BybitConnector) EmitOrderbookSnapshot(tickSize float64) {
	for symbol, book := range b.orderbooks {
		book.mu.RLock()
		allPrices := make(map[string]struct{}, len(book.bids)+len(book.asks))
		for p := range book.bids {
			allPrices[p] = struct{}{}
		}
		for p := range book.asks {
			allPrices[p] = struct{}{}
		}

		levels := make([]OrderbookLevel, 0, len(allPrices))
		for p := range allPrices {
			price, _ := strconv.ParseFloat(p, 64)
			if math.IsNaN(price) || price == 0 {
				continue
			}
			rounded := math.Round(price/tickSize) * tickSize
			levels = append(levels, OrderbookLevel{
				Price:  rounded,
				BidQty: book.bids[p],
				AskQty: book.asks[p],
			})
		}
		book.mu.RUnlock()

		if b.onOrderbookSnapshot != nil {
			b.onOrderbookSnapshot(OrderbookSnapshot{
				Exchange:   b.name,
				Symbol:     symbol,
				MarketType: bybitMarketType(symbol),
				TickSize:   tickSize,
				Levels:     levels,
				Timestamp:  time.Now().UTC(),
			})
		}
	}
}
