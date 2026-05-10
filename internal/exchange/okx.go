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

type OKXConnector struct {
	name       string
	wsURL      string
	symbol     string
	marketType string
	instID     string

	ws      *websocket.Conn
	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	orderbooks map[string]*okxOrderbook

	onTrade             func(Trade)
	onOrderbookSnapshot func(OrderbookSnapshot)
	onLiquidation       func(Liquidation)
	onMarketStat        func(MarketStat)
}

type okxOrderbook struct {
	bids map[string]float64
	asks map[string]float64
	mu   sync.RWMutex
}

func NewOKXConnector() *OKXConnector {
	return &OKXConnector{
		name:       "OKX",
		wsURL:      "wss://ws.okx.com:8443/ws/v5/public",
		orderbooks: make(map[string]*okxOrderbook),
	}
}

func (o *OKXConnector) Name() string            { return o.name }
func (o *OKXConnector) MarketTypes() []string { return []string{"spot", "perp"} }

func (o *OKXConnector) OnTrade(cb func(Trade))                        { o.onTrade = cb }
func (o *OKXConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { o.onOrderbookSnapshot = cb }
func (o *OKXConnector) OnLiquidation(cb func(Liquidation))            { o.onLiquidation = cb }
func (o *OKXConnector) OnMarketStat(cb func(MarketStat))               { o.onMarketStat = cb }

func (o *OKXConnector) Connect(symbol, marketType string) error {
	o.symbol = strings.ToUpper(symbol)
	o.marketType = marketType
	o.orderbooks[marketType] = &okxOrderbook{bids: make(map[string]float64), asks: make(map[string]float64)}
	if o.symbol == "BTCUSDT" {
		o.symbol = "BTC-USDT"
	}
	if marketType == "spot" {
		o.instID = o.symbol
	} else {
		o.instID = o.symbol + "-SWAP"
	}
	return nil
}

func (o *OKXConnector) Disconnect() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.running = false
	if o.cancel != nil {
		o.cancel()
	}
	if o.ws != nil {
		o.ws.Close()
	}
}

func (o *OKXConnector) Run(ctx context.Context) error {
	o.ctx, o.cancel = context.WithCancel(ctx)
	defer o.cancel()
	o.mu.Lock()
	o.running = true
	o.mu.Unlock()
	for {
		select {
		case <-o.ctx.Done():
			return nil
		default:
		}
		if err := o.connectAndStream(); err != nil {
			slog.Error("okx stream error", "exchange", o.name, "err", err)
			select {
			case <-o.ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}
}

func (o *OKXConnector) connectAndStream() error {
	ws, _, err := websocket.DefaultDialer.Dial(o.wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	o.ws = ws

	args := []map[string]string{
		{"channel": "trades", "instId": o.instID},
		{"channel": "books", "instId": o.instID},
		{"channel": "tickers", "instId": o.instID},
	}
	if o.marketType == "perp" {
		args = append(args, map[string]string{"channel": "liquidation-orders", "instType": "SWAP", "mgnMode": "", "instId": o.instID})
	}
	if err := ws.WriteJSON(map[string]interface{}{"op": "subscribe", "args": args}); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	go o.pingLoop(ws)

	for {
		select {
		case <-o.ctx.Done():
			return nil
		default:
		}
		ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if err := o.handleMessage(msg); err != nil {
			slog.Warn("okx handle message", "err", err)
		}
	}
}

func (o *OKXConnector) handleMessage(msg []byte) error {
	var wrapper struct {
		Event   string          `json:"event"`
		Arg     map[string]string `json:"arg"`
		Data    json.RawMessage `json:"data"`
		Action  string          `json:"action"`
		Code    string          `json:"code"`
		Msg     string          `json:"msg"`
	}
	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return err
	}
	if wrapper.Event != "" {
		return nil
	}
	ch := wrapper.Arg["channel"]
	switch ch {
	case "trades":
		return o.handleTrade(wrapper.Data)
	case "books":
		if wrapper.Action == "snapshot" {
			return o.handleOrderbookSnapshot(wrapper.Data)
		}
		return o.handleOrderbookDelta(wrapper.Data)
	case "tickers":
		return o.handleTicker(wrapper.Data)
	case "liquidation-orders":
		return o.handleLiquidation(wrapper.Data)
	}
	return nil
}

func (o *OKXConnector) handleTrade(data []byte) error {
	var trades []struct {
		InstID string `json:"instId"`
		TradeID string `json:"tradeId"`
		Px     string `json:"px"`
		Sz     string `json:"sz"`
		Side   string `json:"side"`
		Ts     string `json:"ts"`
	}
	if err := json.Unmarshal(data, &trades); err != nil {
		return err
	}
	for _, t := range trades {
		price, _ := strconv.ParseFloat(t.Px, 64)
		qty, _ := strconv.ParseFloat(t.Sz, 64)
		if price == 0 || qty == 0 {
			continue
		}
		isMaker := t.Side == "sell"
		trade := Trade{
			Exchange:     o.name,
			Symbol:       o.symbol,
			MarketType:   o.marketType,
			Price:        price,
			Qty:          qty,
			QuoteQty:     price * qty,
			Side:         strings.ToLower(t.Side),
			IsBuyerMaker: isMaker,
			Timestamp:    time.Now(),
			TradeID:      t.TradeID,
		}
		if o.onTrade != nil {
			o.onTrade(trade)
		}
	}
	return nil
}

func (o *OKXConnector) handleOrderbookSnapshot(data []byte) error {
	var obs []struct {
		Bids [][2]string `json:"bids"`
		Asks [][2]string `json:"asks"`
		Ts   string      `json:"ts"`
	}
	if err := json.Unmarshal(data, &obs); err != nil {
		return err
	}
	book := o.orderbooks[o.marketType]
	if book == nil || len(obs) == 0 {
		return nil
	}
	ob := obs[0]
	book.mu.Lock()
	book.bids = make(map[string]float64)
	book.asks = make(map[string]float64)
	for _, b := range ob.Bids {
		price := b[0]
		qty, _ := strconv.ParseFloat(b[1], 64)
		if qty == 0 {
			delete(book.bids, price)
		} else {
			book.bids[price] = qty
		}
	}
	for _, a := range ob.Asks {
		price := a[0]
		qty, _ := strconv.ParseFloat(a[1], 64)
		if qty == 0 {
			delete(book.asks, price)
		} else {
			book.asks[price] = qty
		}
	}
	book.mu.Unlock()
	return nil
}

func (o *OKXConnector) handleOrderbookDelta(data []byte) error {
	var obs []struct {
		Bids [][2]string `json:"bids"`
		Asks [][2]string `json:"asks"`
		Ts   string      `json:"ts"`
	}
	if err := json.Unmarshal(data, &obs); err != nil {
		return err
	}
	book := o.orderbooks[o.marketType]
	if book == nil || len(obs) == 0 {
		return nil
	}
	ob := obs[0]
	book.mu.Lock()
	for _, b := range ob.Bids {
		price := b[0]
		qty, _ := strconv.ParseFloat(b[1], 64)
		if qty == 0 {
			delete(book.bids, price)
		} else {
			book.bids[price] = qty
		}
	}
	for _, a := range ob.Asks {
		price := a[0]
		qty, _ := strconv.ParseFloat(a[1], 64)
		if qty == 0 {
			delete(book.asks, price)
		} else {
			book.asks[price] = qty
		}
	}
	book.mu.Unlock()
	return nil
}

func (o *OKXConnector) handleTicker(data []byte) error {
	var tickers []struct {
		MarkPx     string `json:"markPx"`
		IdxPx      string `json:"idxPx"`
		FundingRate string `json:"fundingRate"`
		NextFundingTime string `json:"nextFundingTime"`
		Oi         string `json:"oi"`
	}
	if err := json.Unmarshal(data, &tickers); err != nil {
		return err
	}
	if len(tickers) == 0 {
		return nil
	}
	t := tickers[0]
	mp, _ := strconv.ParseFloat(t.MarkPx, 64)
	ip, _ := strconv.ParseFloat(t.IdxPx, 64)
	fr, _ := strconv.ParseFloat(t.FundingRate, 64)
	oi, _ := strconv.ParseFloat(t.Oi, 64)
	nft, _ := strconv.ParseInt(t.NextFundingTime, 10, 64)
	stat := MarketStat{
		Exchange:        o.name,
		Symbol:          o.symbol,
		MarketType:      o.marketType,
		MarkPrice:       mp,
		IndexPrice:      ip,
		FundingRate:     fr,
		OpenInterest:    oi,
		NextFundingTime: time.Unix(nft/1000, 0),
		Timestamp:       time.Now(),
	}
	if o.onMarketStat != nil {
		o.onMarketStat(stat)
	}
	return nil
}

func (o *OKXConnector) handleLiquidation(data []byte) error {
	var liqs []struct {
		InstID string `json:"instId"`
		Side   string `json:"posSide"`
		Sz     string `json:"sz"`
		Px     string `json:"bkPx"`
		Ts     string `json:"ts"`
	}
	if err := json.Unmarshal(data, &liqs); err != nil {
		return err
	}
	for _, l := range liqs {
		price, _ := strconv.ParseFloat(l.Px, 64)
		qty, _ := strconv.ParseFloat(l.Sz, 64)
		liq := Liquidation{
			Exchange:   o.name,
			Symbol:     o.symbol,
			MarketType: o.marketType,
			Side:       strings.ToLower(l.Side),
			Price:      price,
			Qty:        qty,
			QuoteQty:   price * qty,
			Timestamp:  time.Now(),
		}
		if o.onLiquidation != nil {
			o.onLiquidation(liq)
		}
	}
	return nil
}

func (o *OKXConnector) pingLoop(ws *websocket.Conn) {
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-o.ctx.Done():
			return
		case <-ticker.C:
			if err := ws.WriteJSON(map[string]string{"op": "ping"}); err != nil {
				return
			}
		}
	}
}

func (o *OKXConnector) EmitOrderbookSnapshot(tickSize float64) {
	book := o.orderbooks[o.marketType]
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
	if o.onOrderbookSnapshot != nil {
		o.onOrderbookSnapshot(OrderbookSnapshot{
			Exchange:   o.name,
			Symbol:     o.symbol,
			MarketType: o.marketType,
			TickSize:   tickSize,
			Levels:     levels,
			Timestamp:  time.Now(),
		})
	}
}
