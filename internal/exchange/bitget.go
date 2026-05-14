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

// BitgetConnector — spot + perp
type BitgetConnector struct {
	ConnectorRuntime
	name       string
	spotWSURL  string
	perpWSURL  string
	markets    []config.MarketConfig
	symbol     string
	marketType string
	instType   string

	ws      *websocket.Conn
	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	orderbooks  map[string]*bitgetOrderbook
	marketByArg map[string]config.MarketConfig

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
		name:        "BITGET",
		spotWSURL:   "wss://ws.bitget.com/v2/ws/public",
		perpWSURL:   "wss://ws.bitget.com/v2/ws/public",
		orderbooks:  make(map[string]*bitgetOrderbook),
		marketByArg: make(map[string]config.MarketConfig),
	}
}

func (bg *BitgetConnector) Name() string          { return bg.name }
func (bg *BitgetConnector) MarketTypes() []string { return []string{"spot", "perp"} }

func (bg *BitgetConnector) OnTrade(cb func(Trade)) { bg.onTrade = cb }
func (bg *BitgetConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) {
	bg.onOrderbookSnapshot = cb
}
func (bg *BitgetConnector) OnLiquidation(cb func(Liquidation)) { bg.onLiquidation = cb }
func (bg *BitgetConnector) OnMarketStat(cb func(MarketStat))   { bg.onMarketStat = cb }

func (bg *BitgetConnector) Connect(markets []config.MarketConfig) error {
	if len(markets) == 0 {
		return fmt.Errorf("no markets configured")
	}
	bg.markets = append([]config.MarketConfig(nil), markets...)
	first := markets[0]
	bg.symbol = strings.ToUpper(first.Pair)
	bg.marketType = string(first.Type)
	bg.marketByArg = make(map[string]config.MarketConfig, len(markets))
	for _, market := range markets {
		if market.Type != first.Type {
			continue
		}
		symbol := strings.ToUpper(market.Pair)
		bg.orderbooks[symbol] = &bitgetOrderbook{bids: make(map[string]float64), asks: make(map[string]float64)}
		instType := bitgetInstType(market)
		instID := bitgetInstID(market.Pair)
		bg.marketByArg[bitgetArgKey(instType, instID)] = config.MarketConfig{
			Exchange: market.Exchange,
			Pair:     symbol,
			Type:     market.Type,
		}
		if symbol == bg.symbol {
			bg.instType = instType
		}
	}
	if bg.instType == "" {
		bg.instType = bitgetInstType(first)
	}
	return nil
}

func (bg *BitgetConnector) Disconnect() {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	bg.running = false
	bg.MarkDisconnected("shutdown")
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
	bg.MarkConnecting("connecting")
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
			delay := bg.MarkReconnectScheduled("reconnect_scheduled")
			select {
			case <-bg.ctx.Done():
				return nil
			case <-time.After(delay):
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
		bg.MarkDisconnected("dial_error")
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	bg.ws = ws

	args := bg.buildSubscriptionArgs()

	if err := ws.WriteJSON(map[string]interface{}{"op": "subscribe", "args": args}); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	bg.MarkConnected("connected")
	go bg.pingLoop(ws)

	for {
		select {
		case <-bg.ctx.Done():
			return nil
		default:
		}
		ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			bg.MarkDisconnected("read_error")
			return fmt.Errorf("read: %w", err)
		}
		bg.MarkMessageReceived()
		if err := bg.handleMessage(msg); err != nil {
			slog.Warn("bitget handle message", "err", err)
		}
	}
}

func (bg *BitgetConnector) buildSubscriptionArgs() []map[string]interface{} {
	args := make([]map[string]interface{}, 0, len(bg.markets)*4)
	seen := make(map[string]struct{})
	for _, market := range bg.markets {
		if string(market.Type) != bg.marketType {
			continue
		}
		instType := bitgetInstType(market)
		instID := bitgetInstID(market.Pair)
		channels := []string{"trade", "books", "ticker"}
		if market.Type == config.MarketTypePerp {
			channels = append(channels, "liquidation-order")
		}
		for _, channel := range channels {
			key := bitgetArgKey(instType, instID) + ":" + channel
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			args = append(args, map[string]interface{}{
				"instType": instType,
				"channel":  channel,
				"instId":   instID,
			})
		}
	}
	if len(args) == 0 && bg.symbol != "" {
		instType := bg.instType
		instID := bitgetInstID(bg.symbol)
		args = append(args,
			map[string]interface{}{"instType": instType, "channel": "trade", "instId": instID},
			map[string]interface{}{"instType": instType, "channel": "books", "instId": instID},
			map[string]interface{}{"instType": instType, "channel": "ticker", "instId": instID},
		)
		if bg.marketType == "perp" {
			args = append(args, map[string]interface{}{"instType": instType, "channel": "liquidation-order", "instId": instID})
		}
	}
	return args
}

func (bg *BitgetConnector) pingLoop(ws *websocket.Conn) {
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-bg.ctx.Done():
			return
		case <-ticker.C:
			if err := ws.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
				return
			}
		}
	}
}

func bitgetInstType(market config.MarketConfig) string {
	pair := strings.ToUpper(market.Pair)
	if market.Type == config.MarketTypeSpot {
		return "SPOT"
	}
	switch {
	case strings.Contains(pair, "_DMCBL"):
		return "COIN-FUTURES"
	case strings.Contains(pair, "_CMCBL"):
		return "USDC-FUTURES"
	default:
		return "USDT-FUTURES"
	}
}

func bitgetInstID(pair string) string {
	upper := strings.ToUpper(pair)
	if idx := strings.IndexByte(upper, '_'); idx >= 0 {
		return upper[:idx]
	}
	return upper
}

func bitgetArgKey(instType, instID string) string {
	return strings.ToUpper(instType) + ":" + strings.ToUpper(instID)
}

func (bg *BitgetConnector) marketForArg(arg map[string]string) config.MarketConfig {
	key := bitgetArgKey(arg["instType"], arg["instId"])
	if market, ok := bg.marketByArg[key]; ok {
		return market
	}
	return config.MarketConfig{Pair: strings.ToUpper(arg["instId"]), Type: config.MarketType(bg.marketType)}
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
		return bg.handleTrade(wrapper.Arg, wrapper.Data)
	case "books":
		if wrapper.Action == "snapshot" {
			return bg.handleOrderbookSnapshot(wrapper.Arg, wrapper.Data)
		}
		return bg.handleOrderbookDelta(wrapper.Arg, wrapper.Data)
	case "ticker":
		return bg.handleTicker(wrapper.Arg, wrapper.Data)
	case "liquidation-order":
		return bg.handleLiquidation(wrapper.Arg, wrapper.Data)
	}
	return nil
}

func (bg *BitgetConnector) handleTrade(arg map[string]string, data []byte) error {
	var trades []struct {
		InstId  string `json:"instId"`
		TradeId string `json:"tradeId"`
		Px      string `json:"px"`
		Price   string `json:"price"`
		Sz      string `json:"sz"`
		Size    string `json:"size"`
		Side    string `json:"side"`
		Ts      string `json:"ts"`
	}
	if err := json.Unmarshal(data, &trades); err != nil {
		return err
	}
	market := bg.marketForArg(arg)
	for _, t := range trades {
		priceStr := t.Px
		if priceStr == "" {
			priceStr = t.Price
		}
		qtyStr := t.Sz
		if qtyStr == "" {
			qtyStr = t.Size
		}
		price, _ := strconv.ParseFloat(priceStr, 64)
		qty, _ := strconv.ParseFloat(qtyStr, 64)
		ts := bitgetTimestamp(t.Ts)
		side := strings.ToLower(t.Side)
		if price == 0 || qty == 0 || ts.IsZero() {
			continue
		}
		isMaker := side == "sell"
		trade := Trade{
			Exchange:     bg.name,
			Symbol:       market.Pair,
			MarketType:   string(market.Type),
			Price:        price,
			Qty:          qty,
			QuoteQty:     price * qty,
			Side:         side,
			IsBuyerMaker: isMaker,
			Timestamp:    ts,
			TradeID:      t.TradeId,
		}
		bg.MarkTrade(trade.Timestamp)
		if bg.onTrade != nil {
			bg.onTrade(trade)
		}
	}
	return nil
}

func bitgetTimestamp(ts string) time.Time {
	ms, _ := strconv.ParseInt(ts, 10, 64)
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func (bg *BitgetConnector) handleOrderbookSnapshot(arg map[string]string, data []byte) error {
	var obs []struct {
		Bids [][2]string `json:"bids"`
		Asks [][2]string `json:"asks"`
		Ts   string      `json:"ts"`
	}
	if err := json.Unmarshal(data, &obs); err != nil {
		return err
	}
	market := bg.marketForArg(arg)
	book := bg.orderbooks[market.Pair]
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

func (bg *BitgetConnector) handleOrderbookDelta(arg map[string]string, data []byte) error {
	var obs []struct {
		Bids [][2]string `json:"bids"`
		Asks [][2]string `json:"asks"`
		Ts   string      `json:"ts"`
	}
	if err := json.Unmarshal(data, &obs); err != nil {
		return err
	}
	market := bg.marketForArg(arg)
	book := bg.orderbooks[market.Pair]
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

func (bg *BitgetConnector) handleTicker(arg map[string]string, data []byte) error {
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
	market := bg.marketForArg(arg)
	mp, _ := strconv.ParseFloat(t.MarkPx, 64)
	ip, _ := strconv.ParseFloat(t.IdxPx, 64)
	fr, _ := strconv.ParseFloat(t.FundingRate, 64)
	oi, _ := strconv.ParseFloat(t.Oi, 64)
	nft, _ := strconv.ParseInt(t.NextFunding, 10, 64)
	stat := MarketStat{
		Exchange:        bg.name,
		Symbol:          market.Pair,
		MarketType:      string(market.Type),
		MarkPrice:       mp,
		IndexPrice:      ip,
		FundingRate:     fr,
		OpenInterest:    oi,
		NextFundingTime: time.UnixMilli(nft).UTC(),
		Timestamp:       time.Now(),
	}
	if bg.onMarketStat != nil {
		bg.onMarketStat(stat)
	}
	return nil
}

func (bg *BitgetConnector) handleLiquidation(arg map[string]string, data []byte) error {
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
	market := bg.marketForArg(arg)
	for _, l := range liqs {
		price, _ := strconv.ParseFloat(l.Px, 64)
		qty, _ := strconv.ParseFloat(l.Sz, 64)
		ts := bitgetTimestamp(l.Ts)
		if price == 0 || qty == 0 || ts.IsZero() {
			continue
		}
		liq := Liquidation{
			Exchange:   bg.name,
			Symbol:     market.Pair,
			MarketType: string(market.Type),
			Side:       strings.ToLower(l.Side),
			Price:      price,
			Qty:        qty,
			QuoteQty:   price * qty,
			Timestamp:  ts,
		}
		if bg.onLiquidation != nil {
			bg.onLiquidation(liq)
		}
	}
	return nil
}

func (bg *BitgetConnector) EmitOrderbookSnapshot(tickSize float64) {
	if bg.onOrderbookSnapshot == nil {
		return
	}
	for symbol, book := range bg.orderbooks {
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
		bg.onOrderbookSnapshot(OrderbookSnapshot{
			Exchange:   bg.name,
			Symbol:     symbol,
			MarketType: bg.marketType,
			TickSize:   tickSize,
			Levels:     levels,
			Timestamp:  time.Now(),
		})
	}
}
