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

// BitfinexConnector — spot + perp
type BitfinexConnector struct {
	name       string
	wsURL      string
	symbol     string
	marketType string
	channelID  int

	ws      *websocket.Conn
	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	orderbook *bitfinexOrderbook

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
		name:      "BITFINEX",
		wsURL:     "wss://api-pub.bitfinex.com/ws/2",
		orderbook: &bitfinexOrderbook{bids: make(map[string]float64), asks: make(map[string]float64)},
	}
}

func (bf *BitfinexConnector) Name() string              { return bf.name }
func (bf *BitfinexConnector) MarketTypes() []string { return []string{"spot"} }

func (bf *BitfinexConnector) OnTrade(cb func(Trade))                        { bf.onTrade = cb }
func (bf *BitfinexConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { bf.onOrderbookSnapshot = cb }
func (bf *BitfinexConnector) OnLiquidation(cb func(Liquidation))            { }
func (bf *BitfinexConnector) OnMarketStat(cb func(MarketStat))               { }

func (bf *BitfinexConnector) Connect(symbol, marketType string) error {
	bf.symbol = strings.ToUpper(symbol)
	bf.marketType = marketType
	if bf.symbol == "BTCUSDT" {
		bf.symbol = "tBTCUSD"
	}
	return nil
}

func (bf *BitfinexConnector) Disconnect() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	bf.running = false
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
			select {
			case <-bf.ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}
}

func (bf *BitfinexConnector) connectAndStream() error {
	ws, _, err := websocket.DefaultDialer.Dial(bf.wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	bf.ws = ws

	sub := map[string]interface{}{
		"event":   "subscribe",
		"channel": "book",
		"symbol":  bf.symbol,
		"prec":    "P0",
		"freq":    "F0",
		"len":     100,
	}
	if err := ws.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe book: %w", err)
	}

	tradesSub := map[string]interface{}{
		"event":   "subscribe",
		"channel": "trades",
		"symbol":  bf.symbol,
	}
	if err := ws.WriteJSON(tradesSub); err != nil {
		return fmt.Errorf("subscribe trades: %w", err)
	}

	for {
		select {
		case <-bf.ctx.Done():
			return nil
		default:
		}
		ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
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
					bf.channelID = int(cid)
				}
				return nil
			case "info", "pong":
				return nil
			}
		}
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
		if payload.(string) == "te" || payload.(string) == "tu" {
			if len(arr) >= 3 {
				return bf.handleTrade(arr[2])
			}
		}
		if payload.(string) == "hb" {
			return nil
		}
	case []interface{}:
		if len(arr) >= 3 {
			snapType, ok := arr[2].(string)
			if ok && snapType == "1" {
				return bf.handleBookSnapshot(payload.([]interface{}))
			}
		}
		return bf.handleBookUpdate(payload.([]interface{}))
	}

	_ = cid
	return nil
}

func (bf *BitfinexConnector) handleTrade(data interface{}) error {
	arr, ok := data.([]interface{})
	if !ok || len(arr) < 5 {
		return nil
	}
	tradeID, _ := arr[0].(float64)
	timestamp, _ := arr[1].(float64)
	qty, _ := arr[2].(float64)
	price, _ := arr[3].(float64)

	side := "buy"
	if qty < 0 {
		side = "sell"
		qty = -qty
	}

	trade := Trade{
		Exchange:   bf.name,
		Symbol:       bf.symbol,
		MarketType:   bf.marketType,
		Price:        price,
		Qty:          qty,
		QuoteQty:     price * qty,
		Side:         side,
		IsBuyerMaker: side == "sell",
		Timestamp:    time.Unix(int64(timestamp)/1000, 0),
		TradeID:      strconv.FormatInt(int64(tradeID), 10),
	}
	if bf.onTrade != nil {
		bf.onTrade(trade)
	}
	return nil
}

func (bf *BitfinexConnector) handleBookSnapshot(data []interface{}) error {
	bf.orderbook.mu.Lock()
	bf.orderbook.bids = make(map[string]float64)
	bf.orderbook.asks = make(map[string]float64)
	for _, item := range data {
		arr, ok := item.([]interface{})
		if !ok || len(arr) < 3 {
			continue
		}
		price, _ := arr[0].(float64)
		count, _ := arr[1].(float64)
		qty, _ := arr[2].(float64)
		if count == 0 {
			delete(bf.orderbook.bids, fmt.Sprintf("%f", price))
			delete(bf.orderbook.asks, fmt.Sprintf("%f", price))
			continue
		}
		priceStr := fmt.Sprintf("%f", price)
		if qty > 0 {
			bf.orderbook.bids[priceStr] = qty
		} else {
			bf.orderbook.asks[priceStr] = -qty
		}
	}
	bf.orderbook.mu.Unlock()
	return nil
}

func (bf *BitfinexConnector) handleBookUpdate(data []interface{}) error {
	bf.orderbook.mu.Lock()
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
			delete(bf.orderbook.bids, priceStr)
			delete(bf.orderbook.asks, priceStr)
			continue
		}
		if qty > 0 {
			bf.orderbook.bids[priceStr] = qty
		} else {
			bf.orderbook.asks[priceStr] = -qty
		}
	}
	bf.orderbook.mu.Unlock()
	return nil
}

func (bf *BitfinexConnector) EmitOrderbookSnapshot(tickSize float64) {
	if bf.orderbook == nil {
		return
	}
	bf.orderbook.mu.RLock()
	allPrices := make(map[string]struct{})
	for p := range bf.orderbook.bids {
		allPrices[p] = struct{}{}
	}
	for p := range bf.orderbook.asks {
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
			BidQty: bf.orderbook.bids[p],
			AskQty: bf.orderbook.asks[p],
		})
	}
	bf.orderbook.mu.RUnlock()
	if bf.onOrderbookSnapshot != nil {
		bf.onOrderbookSnapshot(OrderbookSnapshot{
			Exchange:   bf.name,
			Symbol:     bf.symbol,
			MarketType: bf.marketType,
			TickSize:   tickSize,
			Levels:     levels,
			Timestamp:  time.Now(),
		})
	}
}
