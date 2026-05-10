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
)

type BybitConnector struct {
	name       string
	spotWSURL  string
	perpWSURL  string
	symbol     string
	marketType string

	ws      *websocket.Conn
	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	orderbooks map[string]*bybitOrderbook

	onTrade             func(Trade)
	onOrderbookSnapshot func(OrderbookSnapshot)
	onLiquidation       func(Liquidation)
	onMarketStat        func(MarketStat)
}

type bybitOrderbook struct {
	bids map[string]float64
	asks map[string]float64
	mu   sync.RWMutex
}

func NewBybitConnector() *BybitConnector {
	return &BybitConnector{
		name:       "BYBIT",
		spotWSURL:  "wss://stream.bybit.com/v5/public/spot",
		perpWSURL:  "wss://stream.bybit.com/v5/public/linear",
		orderbooks: make(map[string]*bybitOrderbook),
	}
}

func (b *BybitConnector) Name() string         { return b.name }
func (b *BybitConnector) MarketTypes() []string { return []string{"spot", "perp"} }

func (b *BybitConnector) OnTrade(cb func(Trade))                        { b.onTrade = cb }
func (b *BybitConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { b.onOrderbookSnapshot = cb }
func (b *BybitConnector) OnLiquidation(cb func(Liquidation))            { b.onLiquidation = cb }
func (b *BybitConnector) OnMarketStat(cb func(MarketStat))               { b.onMarketStat = cb }

func (b *BybitConnector) Connect(symbol, marketType string) error {
	b.symbol = strings.ToUpper(symbol)
	b.marketType = marketType
	b.orderbooks[marketType] = &bybitOrderbook{
		bids: make(map[string]float64),
		asks: make(map[string]float64),
	}
	return nil
}

func (b *BybitConnector) Disconnect() {
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

func (b *BybitConnector) Run(ctx context.Context) error {
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
			slog.Error("bybit stream error", "exchange", b.name, "market", b.marketType, "err", err)
			select {
			case <-b.ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}
}

func (b *BybitConnector) connectAndStream() error {
	wsURL := b.spotWSURL
	if b.marketType == "perp" {
		wsURL = b.perpWSURL
	}

	slog.Info("connecting to bybit", "url", wsURL, "symbol", b.symbol)

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	b.ws = ws

	sub := map[string]interface{}{
		"op":      "subscribe",
		"reqId":   "test-" + b.marketType,
		"args":    b.buildArgs(),
	}
	if err := ws.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
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
			slog.Warn("bybit handle message", "err", err)
		}
	}
}

func (b *BybitConnector) buildArgs() []string {
	sym := b.symbol
	if b.marketType == "perp" {
		return []string{
			"tickers." + sym,
			"publicTrade." + sym,
			"orderbook.1." + sym,
			"liquidation." + sym,
		}
	}
	return []string{
		"tickers." + sym,
		"publicTrade." + sym,
		"orderbook.1." + sym,
	}
}

func (b *BybitConnector) handleMessage(msg []byte) error {
	var wrapper struct {
		Topic       string          `json:"topic"`
		Type        string          `json:"type"`
		Data        json.RawMessage `json:"data"`
		Success     bool            `json:"success"`
		RetMsg      string          `json:"ret_msg"`
		ConnID      string          `json:"conn_id"`
	}

	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return err
	}

	if !wrapper.Success && wrapper.RetMsg != "" {
		if wrapper.RetMsg == "" {
			return nil
		}
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
		return b.handleTicker(wrapper.Data)
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
		side := strings.ToLower(t.L)
		if side == "" {
			side = "buy"
		}
		trade := Trade{
			Exchange:     b.name,
			Symbol:       b.symbol,
			MarketType:   b.marketType,
			Price:        price,
			Qty:          qty,
			QuoteQty:     price * qty,
			Side:         side,
			IsBuyerMaker: t.BT,
			Timestamp:    time.Now(),
			TradeID:      t.I,
		}
		if b.onTrade != nil {
			b.onTrade(trade)
		}
	}
	return nil
}

func (b *BybitConnector) handleOrderbookSnapshot(data []byte) error {
	var ob struct {
		S  string     `json:"s"`
		B  [][2]string `json:"b"`
		A  [][2]string `json:"a"`
		U  int64      `json:"u"`
		Seq int64     `json:"seq"`
	}
	if err := json.Unmarshal(data, &ob); err != nil {
		return err
	}

	book := b.orderbooks[b.marketType]
	if book == nil {
		return nil
	}

	book.mu.Lock()
	book.bids = make(map[string]float64)
	book.asks = make(map[string]float64)

	for _, bid := range ob.B {
		price := bid[0]
		qty, _ := strconv.ParseFloat(bid[1], 64)
		if qty == 0 {
			delete(book.bids, price)
		} else {
			book.bids[price] = qty
		}
	}
	for _, ask := range ob.A {
		price := ask[0]
		qty, _ := strconv.ParseFloat(ask[1], 64)
		if qty == 0 {
			delete(book.asks, price)
		} else {
			book.asks[price] = qty
		}
	}
	book.mu.Unlock()
	return nil
}

func (b *BybitConnector) handleOrderbookDelta(data []byte) error {
	var ob struct {
		S  string     `json:"s"`
		B  [][2]string `json:"b"`
		A  [][2]string `json:"a"`
		U  int64      `json:"u"`
		Seq int64     `json:"seq"`
	}
	if err := json.Unmarshal(data, &ob); err != nil {
		return err
	}

	book := b.orderbooks[b.marketType]
	if book == nil {
		return nil
	}

	book.mu.Lock()
	for _, bid := range ob.B {
		price := bid[0]
		qty, _ := strconv.ParseFloat(bid[1], 64)
		if qty == 0 {
			delete(book.bids, price)
		} else {
			book.bids[price] = qty
		}
	}
	for _, ask := range ob.A {
		price := ask[0]
		qty, _ := strconv.ParseFloat(ask[1], 64)
		if qty == 0 {
			delete(book.asks, price)
		} else {
			book.asks[price] = qty
		}
	}
	book.mu.Unlock()
	return nil
}

func (b *BybitConnector) handleTicker(data []byte) error {
	var tickers struct {
		Data struct {
			F string `json:"fundingRate"`
			MP string `json:"markPrice"`
			IP string `json:"indexPrice"`
			OI string `json:"openInterest"`
			N  int64  `json:"nextFundingTime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &tickers); err != nil {
		return err
	}

	mp, _ := strconv.ParseFloat(tickers.Data.MP, 64)
	ip, _ := strconv.ParseFloat(tickers.Data.IP, 64)
	fr, _ := strconv.ParseFloat(tickers.Data.F, 64)
	oi, _ := strconv.ParseFloat(tickers.Data.OI, 64)

	stat := MarketStat{
		Exchange:        b.name,
		Symbol:          b.symbol,
		MarketType:      b.marketType,
		MarkPrice:       mp,
		IndexPrice:      ip,
		FundingRate:     fr,
		OpenInterest:    oi,
		NextFundingTime: time.Unix(tickers.Data.N/1000, 0),
		Timestamp:       time.Now(),
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

		liq := Liquidation{
			Exchange:   b.name,
			Symbol:     b.symbol,
			MarketType: b.marketType,
			Side:       strings.ToLower(l.Sd),
			Price:      price,
			Qty:        qty,
			QuoteQty:   price * qty,
			Timestamp:  time.Unix(l.T/1000, 0),
		}

		if b.onLiquidation != nil {
			b.onLiquidation(liq)
		}
	}
	return nil
}

func (b *BybitConnector) EmitOrderbookSnapshot(tickSize float64) {
	book := b.orderbooks[b.marketType]
	if book == nil {
		return
	}

	book.mu.RLock()
	allPrices := make(map[string]struct{})
	for p := range book.bids {
		allPrices[p] = struct{}{}
	}
	for p := range book.asks {
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
			BidQty: book.bids[p],
			AskQty: book.asks[p],
		})
	}
	book.mu.RUnlock()

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
