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

// BitgetConnector — spot + perp
type BitgetConnector struct {
	name       string
	spotWSURL  string
	perpWSURL  string
	symbol     string
	marketType string
	instType   string

	ws      *websocket.Conn
	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	orderbooks map[string]*bitgetOrderbook

	onTrade             func(Trade)
	onOrderbookSnapshot func(OrderbookSnapshot)
	onLiquidation       func(Liquidation)
	onMarketStat        func(MarketStat)
}

type bitgetOrderbook struct {
	bids map[string]float64
	asks map[string]float64
	mu   sync.RWMutex
}

func NewBitgetConnector() *BitgetConnector {
	return &BitgetConnector{
		name:       "BITGET",
		spotWSURL:  "wss://ws.bitget.com/v2/ws/public",
		perpWSURL:  "wss://ws.bitget.com/v2/ws/public",
		orderbooks: make(map[string]*bitgetOrderbook),
	}
}

func (bg *BitgetConnector) Name() string              { return bg.name }
func (bg *BitgetConnector) MarketTypes() []string { return []string{"spot", "perp"} }

func (bg *BitgetConnector) OnTrade(cb func(Trade))                        { bg.onTrade = cb }
func (bg *BitgetConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { bg.onOrderbookSnapshot = cb }
func (bg *BitgetConnector) OnLiquidation(cb func(Liquidation))            { bg.onLiquidation = cb }
func (bg *BitgetConnector) OnMarketStat(cb func(MarketStat))               { bg.onMarketStat = cb }

func (bg *BitgetConnector) Connect(symbol, marketType string) error {
	bg.symbol = strings.ToUpper(symbol)
	bg.marketType = marketType
	bg.orderbooks[marketType] = &bitgetOrderbook{bids: make(map[string]float64), asks: make(map[string]float64)}
	if marketType == "spot" {
		bg.instType = "SPOT"
	} else {
		bg.instType = "USDT-FUTURES"
	}
	return nil
}

func (bg *BitgetConnector) Disconnect() {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	bg.running = false
	if bg.cancel != nil {
		bg.cancel()
	}
	if bg.ws != nil {
		bg.ws.Close()
	}
}

func (bg *BitgetConnector) Run(ctx context.Context) error {
	bg.ctx, bg.cancel = context.WithCancel(ctx)
	defer bg.cancel()
	bg.mu.Lock()
	bg.running = true
	bg.mu.Unlock()
	for {
		select {
		case <-bg.ctx.Done():
			return nil
		default:
		}
		if err := bg.connectAndStream(); err != nil {
			slog.Error("bitget stream error", "err", err)
			select {
			case <-bg.ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}
}

func (bg *BitgetConnector) connectAndStream() error {
	wsURL := bg.spotWSURL
	if bg.marketType == "perp" {
		wsURL = bg.perpWSURL
	}

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	bg.ws = ws

	args := []map[string]interface{}{
		{"instType": bg.instType, "channel": "trade", "instId": bg.symbol},
		{"instType": bg.instType, "channel": "books", "instId": bg.symbol},
		{"instType": bg.instType, "channel": "ticker", "instId": bg.symbol},
	}
	if bg.marketType == "perp" {
		args = append(args, map[string]interface{}{"instType": bg.instType, "channel": "liquidation-order", "instId": bg.symbol})
	}

	if err := ws.WriteJSON(map[string]interface{}{"op": "subscribe", "args": args}); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	for {
		select {
		case <-bg.ctx.Done():
			return nil
		default:
		}
		ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if err := bg.handleMessage(msg); err != nil {
			slog.Warn("bitget handle message", "err", err)
		}
	}
}

func (bg *BitgetConnector) handleMessage(msg []byte) error {
	var wrapper struct {
		Event  string            `json:"event"`
		Arg    map[string]string `json:"arg"`
		Data   json.RawMessage   `json:"data"`
		Action string            `json:"action"`
		Code   json.Number       `json:"code"`
		Msg    string            `json:"msg"`
	}
	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return err
	}
	if wrapper.Event != "" {
		return nil
	}
	ch := wrapper.Arg["channel"]
	switch ch {
	case "trade":
		return bg.handleTrade(wrapper.Data)
	case "books":
		if wrapper.Action == "snapshot" {
			return bg.handleOrderbookSnapshot(wrapper.Data)
		}
		return bg.handleOrderbookDelta(wrapper.Data)
	case "ticker":
		return bg.handleTicker(wrapper.Data)
	case "liquidation-order":
		return bg.handleLiquidation(wrapper.Data)
	}
	return nil
}

func (bg *BitgetConnector) handleTrade(data []byte) error {
	var trades []struct {
		InstId string `json:"instId"`
		TradeId string `json:"tradeId"`
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
			Exchange:     bg.name,
			Symbol:       bg.symbol,
			MarketType:   bg.marketType,
			Price:        price,
			Qty:          qty,
			QuoteQty:     price * qty,
			Side:         strings.ToLower(t.Side),
			IsBuyerMaker: isMaker,
			Timestamp:    time.Now(),
			TradeID:      t.TradeId,
		}
		if bg.onTrade != nil {
			bg.onTrade(trade)
		}
	}
	return nil
}

func (bg *BitgetConnector) handleOrderbookSnapshot(data []byte) error {
	var obs []struct {
		Bids [][2]string `json:"bids"`
		Asks [][2]string `json:"asks"`
		Ts   string      `json:"ts"`
	}
	if err := json.Unmarshal(data, &obs); err != nil {
		return err
	}
	book := bg.orderbooks[bg.marketType]
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

func (bg *BitgetConnector) handleOrderbookDelta(data []byte) error {
	var obs []struct {
		Bids [][2]string `json:"bids"`
		Asks [][2]string `json:"asks"`
		Ts   string      `json:"ts"`
	}
	if err := json.Unmarshal(data, &obs); err != nil {
		return err
	}
	book := bg.orderbooks[bg.marketType]
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

func (bg *BitgetConnector) handleTicker(data []byte) error {
	var tickers []struct {
		MarkPx      string `json:"markPrice"`
		IdxPx       string `json:"indexPrice"`
		FundingRate string `json:"fundingRate"`
		NextFunding string `json:"nextFundingTime"`
		Oi          string `json:"openInterest"`
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
	nft, _ := strconv.ParseInt(t.NextFunding, 10, 64)
	stat := MarketStat{
		Exchange:        bg.name,
		Symbol:          bg.symbol,
		MarketType:      bg.marketType,
		MarkPrice:       mp,
		IndexPrice:      ip,
		FundingRate:     fr,
		OpenInterest:    oi,
		NextFundingTime: time.Unix(nft/1000, 0),
		Timestamp:       time.Now(),
	}
	if bg.onMarketStat != nil {
		bg.onMarketStat(stat)
	}
	return nil
}

func (bg *BitgetConnector) handleLiquidation(data []byte) error {
	var liqs []struct {
		InstId string `json:"instId"`
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
			Exchange:   bg.name,
			Symbol:     bg.symbol,
			MarketType: bg.marketType,
			Side:       strings.ToLower(l.Side),
			Price:      price,
			Qty:        qty,
			QuoteQty:   price * qty,
			Timestamp:  time.Now(),
		}
		if bg.onLiquidation != nil {
			bg.onLiquidation(liq)
		}
	}
	return nil
}

func (bg *BitgetConnector) EmitOrderbookSnapshot(tickSize float64) {
	book := bg.orderbooks[bg.marketType]
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
	if bg.onOrderbookSnapshot != nil {
		bg.onOrderbookSnapshot(OrderbookSnapshot{
			Exchange:   bg.name,
			Symbol:     bg.symbol,
			MarketType: bg.marketType,
			TickSize:   tickSize,
			Levels:     levels,
			Timestamp:  time.Now(),
		})
	}
}
