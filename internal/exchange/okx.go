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

type OKXConnector struct {
	ConnectorRuntime
	name       string
	wsURL      string
	markets    []config.MarketConfig
	marketType string

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

func (o *OKXConnector) Name() string { return o.name }

func (o *OKXConnector) MarketTypes() []string { return []string{"spot", "perp"} }

func (o *OKXConnector) OnTrade(cb func(Trade)) { o.onTrade = cb }

func (o *OKXConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { o.onOrderbookSnapshot = cb }

func (o *OKXConnector) OnLiquidation(cb func(Liquidation)) { o.onLiquidation = cb }

func (o *OKXConnector) OnMarketStat(cb func(MarketStat)) { o.onMarketStat = cb }

func (o *OKXConnector) Connect(markets []config.MarketConfig) error {
	if len(markets) == 0 {
		return fmt.Errorf("no markets configured")
	}

	o.markets = append([]config.MarketConfig(nil), markets...)
	o.marketType = string(markets[0].Type)
	o.orderbooks = make(map[string]*okxOrderbook, len(markets))
	for _, market := range markets {
		instID := okxNormalizePair(market.Pair, market.Type)
		o.orderbooks[instID] = &okxOrderbook{bids: make(map[string]float64), asks: make(map[string]float64)}
	}
	return nil
}

func (o *OKXConnector) Disconnect() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.running = false
	o.MarkDisconnected("shutdown")
	if o.cancel != nil {
		o.cancel()
	}
	if o.ws != nil {
		_ = o.ws.Close()
	}
}

func (o *OKXConnector) Run(ctx context.Context) error {
	o.ctx, o.cancel = context.WithCancel(ctx)
	defer o.cancel()
	o.MarkConnecting("connecting")

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
			slog.Error("okx stream error", "exchange", o.name, "market", o.marketType, "err", err)
			delay := o.MarkReconnectScheduled("reconnect_scheduled")
			select {
			case <-o.ctx.Done():
				return nil
			case <-time.After(delay):
			}
		}
	}
}

func (o *OKXConnector) connectAndStream() error {
	ws, _, err := websocket.DefaultDialer.Dial(o.wsURL, nil)
	if err != nil {
		o.MarkDisconnected("dial_error")
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	o.ws = ws

	args := make([]map[string]string, 0, len(o.markets)*4)
	for _, market := range o.markets {
		instID := okxNormalizePair(market.Pair, market.Type)
		args = append(args,
			map[string]string{"channel": "trades", "instId": instID},
			map[string]string{"channel": "books", "instId": instID},
			map[string]string{"channel": "tickers", "instId": instID},
		)
		if market.Type == config.MarketTypePerp {
			args = append(args, map[string]string{"channel": "liquidation-orders", "instType": "SWAP", "instId": instID})
		}
	}

	if err := ws.WriteJSON(map[string]any{"op": "subscribe", "args": args}); err != nil {
		o.MarkDisconnected("subscribe_error")
		return fmt.Errorf("subscribe: %w", err)
	}

	o.MarkConnected("connected")
	go o.pingLoop(ws)

	for {
		select {
		case <-o.ctx.Done():
			return nil
		default:
		}

		_ = ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			if o.ctx.Err() != nil {
				return nil
			}
			o.MarkDisconnected("read_error")
			return fmt.Errorf("read: %w", err)
		}
		o.MarkMessageReceived()

		if err := o.handleMessage(msg); err != nil {
			slog.Warn("okx handle message", "err", err)
		}
	}
}

func (o *OKXConnector) handleMessage(msg []byte) error {
	if string(msg) == "pong" {
		return nil
	}

	var wrapper struct {
		Event  string            `json:"event"`
		Arg    map[string]string `json:"arg"`
		Data   json.RawMessage   `json:"data"`
		Action string            `json:"action"`
		Code   string            `json:"code"`
		Msg    string            `json:"msg"`
	}
	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return err
	}
	if wrapper.Event != "" {
		return nil
	}

	instID := strings.ToUpper(wrapper.Arg["instId"])
	switch wrapper.Arg["channel"] {
	case "trades":
		return o.handleTrade(wrapper.Data)
	case "books":
		if wrapper.Action == "snapshot" {
			return o.handleOrderbookSnapshot(instID, wrapper.Data)
		}
		return o.handleOrderbookDelta(instID, wrapper.Data)
	case "tickers":
		return o.handleTicker(instID, wrapper.Data)
	case "liquidation-orders":
		return o.handleLiquidation(wrapper.Data)
	}
	return nil
}

func (o *OKXConnector) handleTrade(data []byte) error {
	var trades []struct {
		InstID  string `json:"instId"`
		TradeID string `json:"tradeId"`
		Px      string `json:"px"`
		Sz      string `json:"sz"`
		Side    string `json:"side"`
		Ts      string `json:"ts"`
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

		instID := strings.ToUpper(t.InstID)
		trade := Trade{
			Exchange:     o.name,
			Symbol:       instID,
			MarketType:   okxMarketType(instID),
			Price:        price,
			Qty:          qty,
			QuoteQty:     okxQuoteQty(instID, price, qty),
			Side:         strings.ToLower(t.Side),
			IsBuyerMaker: strings.EqualFold(t.Side, "sell"),
			Timestamp:    okxTimestamp(t.Ts),
			TradeID:      t.TradeID,
		}
		o.MarkTrade(trade.Timestamp)
		if o.onTrade != nil {
			o.onTrade(trade)
		}
	}

	return nil
}

func okxTimestamp(ts string) time.Time {
	ms, _ := strconv.ParseInt(ts, 10, 64)
	return time.UnixMilli(ms).UTC()
}

func okxMarketType(instID string) string {
	if strings.HasSuffix(strings.ToUpper(instID), "-SWAP") {
		return "perp"
	}
	return "spot"
}

func okxQuoteQty(instID string, price, qty float64) float64 {
	upper := strings.ToUpper(instID)
	if strings.HasSuffix(upper, "-USD-SWAP") {
		return qty * 100
	}
	if strings.HasSuffix(upper, "-SWAP") {
		return qty * 0.01 * price
	}
	return price * qty
}

func okxNormalizePair(pair string, marketType config.MarketType) string {
	upper := strings.ToUpper(pair)
	if marketType == config.MarketTypeSpot {
		switch upper {
		case "BTCUSDT":
			return "BTC-USDT"
		case "BTCUSDC":
			return "BTC-USDC"
		case "BTCUSD":
			return "BTC-USD"
		}
	}
	if marketType == config.MarketTypePerp && !strings.Contains(upper, "-") {
		switch upper {
		case "BTCUSDT":
			return "BTC-USDT-SWAP"
		case "BTCUSD":
			return "BTC-USD-SWAP"
		case "BTCUSDC":
			return "BTC-USDC-SWAP"
		}
		return upper + "-SWAP"
	}
	return upper
}

func (o *OKXConnector) handleOrderbookSnapshot(instID string, data []byte) error {
	var obs []struct {
		Bids [][2]string `json:"bids"`
		Asks [][2]string `json:"asks"`
	}
	if err := json.Unmarshal(data, &obs); err != nil {
		return err
	}
	book := o.orderbooks[instID]
	if book == nil || len(obs) == 0 {
		return nil
	}

	book.mu.Lock()
	book.bids = make(map[string]float64)
	book.asks = make(map[string]float64)
	for _, bid := range obs[0].Bids {
		qty, _ := strconv.ParseFloat(bid[1], 64)
		if qty == 0 {
			continue
		}
		book.bids[bid[0]] = qty
	}
	for _, ask := range obs[0].Asks {
		qty, _ := strconv.ParseFloat(ask[1], 64)
		if qty == 0 {
			continue
		}
		book.asks[ask[0]] = qty
	}
	book.mu.Unlock()
	return nil
}

func (o *OKXConnector) handleOrderbookDelta(instID string, data []byte) error {
	var obs []struct {
		Bids [][2]string `json:"bids"`
		Asks [][2]string `json:"asks"`
	}
	if err := json.Unmarshal(data, &obs); err != nil {
		return err
	}
	book := o.orderbooks[instID]
	if book == nil || len(obs) == 0 {
		return nil
	}

	book.mu.Lock()
	for _, bid := range obs[0].Bids {
		qty, _ := strconv.ParseFloat(bid[1], 64)
		if qty == 0 {
			delete(book.bids, bid[0])
		} else {
			book.bids[bid[0]] = qty
		}
	}
	for _, ask := range obs[0].Asks {
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

func (o *OKXConnector) handleTicker(instID string, data []byte) error {
	var tickers []struct {
		InstID          string `json:"instId"`
		MarkPx          string `json:"markPx"`
		IdxPx           string `json:"idxPx"`
		FundingRate     string `json:"fundingRate"`
		NextFundingTime string `json:"nextFundingTime"`
		Oi              string `json:"oi"`
	}
	if err := json.Unmarshal(data, &tickers); err != nil {
		return err
	}
	if len(tickers) == 0 {
		return nil
	}

	t := tickers[0]
	if t.InstID != "" {
		instID = strings.ToUpper(t.InstID)
	}
	mp, _ := strconv.ParseFloat(t.MarkPx, 64)
	ip, _ := strconv.ParseFloat(t.IdxPx, 64)
	fr, _ := strconv.ParseFloat(t.FundingRate, 64)
	oi, _ := strconv.ParseFloat(t.Oi, 64)
	nft, _ := strconv.ParseInt(t.NextFundingTime, 10, 64)

	stat := MarketStat{
		Exchange:        o.name,
		Symbol:          instID,
		MarketType:      okxMarketType(instID),
		MarkPrice:       mp,
		IndexPrice:      ip,
		FundingRate:     fr,
		OpenInterest:    oi,
		NextFundingTime: time.UnixMilli(nft).UTC(),
		Timestamp:       time.Now().UTC(),
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
		instID := strings.ToUpper(l.InstID)
		liq := Liquidation{
			Exchange:   o.name,
			Symbol:     instID,
			MarketType: okxMarketType(instID),
			Side:       strings.ToLower(l.Side),
			Price:      price,
			Qty:        qty,
			QuoteQty:   okxQuoteQty(instID, price, qty),
			Timestamp:  okxTimestamp(l.Ts),
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
			if err := ws.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
				return
			}
		}
	}
}

func (o *OKXConnector) EmitOrderbookSnapshot(tickSize float64) {
	for instID, book := range o.orderbooks {
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

		if o.onOrderbookSnapshot != nil {
			o.onOrderbookSnapshot(OrderbookSnapshot{
				Exchange:   o.name,
				Symbol:     instID,
				MarketType: okxMarketType(instID),
				TickSize:   tickSize,
				Levels:     levels,
				Timestamp:  time.Now().UTC(),
			})
		}
	}
}
