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

// BitfinexConnector — spot + perp
type BitfinexConnector struct {
	ConnectorRuntime
	name       string
	wsURL      string
	markets    []config.MarketConfig
	symbol     string
	marketType string
	symbols    []string

	ws      *websocket.Conn
	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	orderbooks   map[string]*bitfinexOrderbook
	tradeChanMap map[int]string
	bookChanMap  map[int]string

	onTrade             func(Trade)
	onOrderbookSnapshot func(OrderbookSnapshot)
}

type bitfinexOrderbook struct {
	bids map[string]float64
	asks map[string]float64
	mu   sync.RWMutex
}

func NewBitfinexConnector() *BitfinexConnector {
	return &BitfinexConnector{
		name:         "BITFINEX",
		wsURL:        "wss://api-pub.bitfinex.com/ws/2",
		orderbooks:   make(map[string]*bitfinexOrderbook),
		tradeChanMap: make(map[int]string),
		bookChanMap:  make(map[int]string),
	}
}

func (bf *BitfinexConnector) Name() string          { return bf.name }
func (bf *BitfinexConnector) MarketTypes() []string { return []string{"spot"} }

func (bf *BitfinexConnector) OnTrade(cb func(Trade)) { bf.onTrade = cb }
func (bf *BitfinexConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) {
	bf.onOrderbookSnapshot = cb
}
func (bf *BitfinexConnector) OnLiquidation(cb func(Liquidation)) {}
func (bf *BitfinexConnector) OnMarketStat(cb func(MarketStat))   {}

func (bf *BitfinexConnector) Connect(markets []config.MarketConfig) error {
	if len(markets) == 0 {
		return fmt.Errorf("no markets configured")
	}
	bf.markets = append([]config.MarketConfig(nil), markets...)
	first := markets[0]
	bf.symbol = strings.ToUpper(first.Pair)
	bf.marketType = string(first.Type)
	bf.symbols = bf.symbols[:0]
	bf.tradeChanMap = make(map[int]string)
	bf.bookChanMap = make(map[int]string)
	for _, market := range markets {
		if market.Type != first.Type {
			continue
		}
		symbol := bitfinexNormalizePair(market.Pair)
		bf.symbols = append(bf.symbols, symbol)
		if _, ok := bf.orderbooks[symbol]; !ok {
			bf.orderbooks[symbol] = &bitfinexOrderbook{bids: make(map[string]float64), asks: make(map[string]float64)}
		}
	}
	if len(bf.symbols) == 0 {
		fallback := bitfinexNormalizePair(bf.symbol)
		bf.symbols = append(bf.symbols, fallback)
		bf.orderbooks[fallback] = &bitfinexOrderbook{bids: make(map[string]float64), asks: make(map[string]float64)}
	}
	return nil
}

func (bf *BitfinexConnector) Disconnect() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	bf.running = false
	bf.MarkDisconnected("shutdown")
	if bf.cancel != nil {
		bf.cancel()
	}
	if bf.ws != nil {
		bf.ws.Close()
	}
}

func (bf *BitfinexConnector) Run(ctx context.Context) error {
	bf.ctx, bf.cancel = context.WithCancel(ctx)
	defer bf.cancel()
	bf.MarkConnecting("connecting")
	bf.mu.Lock()
	bf.running = true
	bf.mu.Unlock()
	for {
		select {
		case <-bf.ctx.Done():
			return nil
		default:
		}
		if err := bf.connectAndStream(); err != nil {
			slog.Error("bitfinex stream error", "err", err)
			delay := bf.MarkReconnectScheduled("reconnect_scheduled")
			select {
			case <-bf.ctx.Done():
				return nil
			case <-time.After(delay):
				continue
			}
		}
	}
}

func (bf *BitfinexConnector) connectAndStream() error {
	ws, _, err := websocket.DefaultDialer.Dial(bf.wsURL, nil)
	if err != nil {
		bf.MarkDisconnected("dial_error")
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	bf.ws = ws

	for _, symbol := range bf.symbols {
		bookSub := map[string]interface{}{
			"event":   "subscribe",
			"channel": "book",
			"symbol":  bitfinexWSSymbol(symbol),
			"prec":    "P0",
			"freq":    "F0",
			"len":     100,
		}
		if err := ws.WriteJSON(bookSub); err != nil {
			return fmt.Errorf("subscribe book %s: %w", symbol, err)
		}

		tradesSub := map[string]interface{}{
			"event":   "subscribe",
			"channel": "trades",
			"symbol":  bitfinexWSSymbol(symbol),
		}
		if err := ws.WriteJSON(tradesSub); err != nil {
			return fmt.Errorf("subscribe trades %s: %w", symbol, err)
		}
	}
	bf.MarkConnected("connected")

	for {
		select {
		case <-bf.ctx.Done():
			return nil
		default:
		}
		ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			bf.MarkDisconnected("read_error")
			return fmt.Errorf("read: %w", err)
		}
		bf.MarkMessageReceived()
		if err := bf.handleMessage(msg); err != nil {
			slog.Warn("bitfinex handle message", "err", err)
		}
	}
}

func (bf *BitfinexConnector) handleMessage(msg []byte) error {
	var event map[string]interface{}
	if err := json.Unmarshal(msg, &event); err == nil {
		if event["event"] != nil {
			switch event["event"].(string) {
			case "subscribed":
				if cid, ok := event["chanId"].(float64); ok {
					symbol := bitfinexSymbolFromEvent(event)
					switch event["channel"] {
					case "trades":
						bf.tradeChanMap[int(cid)] = symbol
					case "book":
						bf.bookChanMap[int(cid)] = symbol
					}
				}
				return nil
			case "info", "pong":
				return nil
			}
		}
		return nil
	}

	var arr []interface{}
	if err := json.Unmarshal(msg, &arr); err != nil {
		return err
	}
	if len(arr) < 2 {
		return nil
	}
	channelID, ok := arr[0].(float64)
	if !ok {
		return nil
	}
	cid := int(channelID)

	payload := arr[1]
	switch payload.(type) {
	case string:
		eventType := payload.(string)
		if eventType == "tu" {
			if len(arr) >= 3 {
				return bf.handleTrade(cid, arr[2])
			}
		}
		if eventType == "hb" || eventType == "te" {
			return nil
		}
	case []interface{}:
		if _, ok := bf.bookChanMap[cid]; !ok {
			return nil
		}
		if len(payload.([]interface{})) == 0 {
			return nil
		}
		if _, ok := payload.([]interface{})[0].([]interface{}); ok {
			return bf.handleBookSnapshot(cid, payload.([]interface{}))
		}
		return bf.handleBookUpdate(cid, payload.([]interface{}))
	}

	_ = cid
	return nil
}

func (bf *BitfinexConnector) handleTrade(channelID int, data interface{}) error {
	arr, arrOK := data.([]interface{})
	symbol, symbolOK := bf.tradeChanMap[channelID]
	if !arrOK || !symbolOK || len(arr) < 4 {
		return nil
	}
	tradeID, _ := arr[0].(float64)
	timestamp, _ := arr[1].(float64)
	qty, _ := arr[2].(float64)
	price, _ := arr[3].(float64)
	if timestamp <= 0 || price == 0 || qty == 0 {
		return nil
	}

	side := "buy"
	if qty < 0 {
		side = "sell"
		qty = -qty
	}

	trade := Trade{
		Exchange:     bf.name,
		Symbol:       symbol,
		MarketType:   bf.marketType,
		Price:        price,
		Qty:          qty,
		QuoteQty:     price * qty,
		Side:         side,
		IsBuyerMaker: side == "sell",
		Timestamp:    time.UnixMilli(int64(timestamp)).UTC(),
		TradeID:      strconv.FormatInt(int64(tradeID), 10),
	}
	bf.MarkTrade(trade.Timestamp)
	if bf.onTrade != nil {
		bf.onTrade(trade)
	}
	return nil
}

func (bf *BitfinexConnector) handleBookSnapshot(channelID int, data []interface{}) error {
	symbol, ok := bf.bookChanMap[channelID]
	if !ok {
		return nil
	}
	book := bf.orderbooks[symbol]
	if book == nil {
		return nil
	}
	book.mu.Lock()
	book.bids = make(map[string]float64)
	book.asks = make(map[string]float64)
	for _, item := range data {
		arr, ok := item.([]interface{})
		if !ok || len(arr) < 3 {
			continue
		}
		price, _ := arr[0].(float64)
		count, _ := arr[1].(float64)
		qty, _ := arr[2].(float64)
		if count == 0 {
			delete(book.bids, fmt.Sprintf("%f", price))
			delete(book.asks, fmt.Sprintf("%f", price))
			continue
		}
		priceStr := fmt.Sprintf("%f", price)
		if qty > 0 {
			book.bids[priceStr] = qty
		} else {
			book.asks[priceStr] = -qty
		}
	}
	book.mu.Unlock()
	return nil
}

func (bf *BitfinexConnector) handleBookUpdate(channelID int, data []interface{}) error {
	symbol, ok := bf.bookChanMap[channelID]
	if !ok {
		return nil
	}
	book := bf.orderbooks[symbol]
	if book == nil {
		return nil
	}
	book.mu.Lock()
	defer book.mu.Unlock()
	if len(data) >= 3 {
		if _, ok := data[0].([]interface{}); !ok {
			data = []interface{}{data}
		}
	}
	for _, item := range data {
		arr, ok := item.([]interface{})
		if !ok || len(arr) < 3 {
			continue
		}
		price, _ := arr[0].(float64)
		count, _ := arr[1].(float64)
		qty, _ := arr[2].(float64)
		priceStr := fmt.Sprintf("%f", price)
		if count == 0 {
			delete(book.bids, priceStr)
			delete(book.asks, priceStr)
			continue
		}
		if qty > 0 {
			book.bids[priceStr] = qty
		} else {
			book.asks[priceStr] = -qty
		}
	}
	return nil
}

func (bf *BitfinexConnector) EmitOrderbookSnapshot(tickSize float64) {
	if bf.onOrderbookSnapshot == nil {
		return
	}
	for symbol, book := range bf.orderbooks {
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
		bf.onOrderbookSnapshot(OrderbookSnapshot{
			Exchange:   bf.name,
			Symbol:     symbol,
			MarketType: bf.marketType,
			TickSize:   tickSize,
			Levels:     levels,
			Timestamp:  time.Now(),
		})
	}
}

func bitfinexWSSymbol(symbol string) string {
	upper := strings.ToUpper(symbol)
	if strings.HasPrefix(upper, "T") {
		return "t" + strings.TrimPrefix(upper, "T")
	}
	return "t" + upper
}

func bitfinexNormalizePair(symbol string) string {
	upper := strings.ToUpper(symbol)
	switch upper {
	case "BTCUSDT", "BTCUSD":
		return "BTCUSD"
	case "BTCUSDC", "BTCUST":
		return "BTCUST"
	default:
		return upper
	}
}

func bitfinexSymbolFromEvent(event map[string]interface{}) string {
	if pair, ok := event["pair"].(string); ok && pair != "" {
		return strings.ToUpper(pair)
	}
	if symbol, ok := event["symbol"].(string); ok && symbol != "" {
		return strings.TrimPrefix(strings.ToUpper(symbol), "T")
	}
	return ""
}
