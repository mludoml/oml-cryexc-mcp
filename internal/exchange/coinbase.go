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

// CoinbaseConnector — spot only (BTC-USD)
type CoinbaseConnector struct {
	name       string
	wsURL      string
	symbol     string
	productID  string

	ws      *websocket.Conn
	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	orderbook *coinbaseOrderbook

	onTrade             func(Trade)
	onOrderbookSnapshot func(OrderbookSnapshot)
}

type coinbaseOrderbook struct {
	bids map[string]float64
	asks map[string]float64
	mu   sync.RWMutex
}

func NewCoinbaseConnector() *CoinbaseConnector {
	return &CoinbaseConnector{
		name:      "COINBASE",
		wsURL:     "wss://ws-feed.exchange.coinbase.com",
		orderbook: &coinbaseOrderbook{bids: make(map[string]float64), asks: make(map[string]float64)},
	}
}

func (c *CoinbaseConnector) Name() string            { return c.name }
func (c *CoinbaseConnector) MarketTypes() []string { return []string{"spot"} }

func (c *CoinbaseConnector) OnTrade(cb func(Trade))                        { c.onTrade = cb }
func (c *CoinbaseConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { c.onOrderbookSnapshot = cb }
func (c *CoinbaseConnector) OnLiquidation(cb func(Liquidation))            { }
func (c *CoinbaseConnector) OnMarketStat(cb func(MarketStat))               { }

func (c *CoinbaseConnector) Connect(symbol, marketType string) error {
	c.symbol = strings.ToUpper(symbol)
	if c.symbol == "BTCUSDT" {
		c.productID = "BTC-USD"
	} else {
		c.productID = strings.Replace(c.symbol, "USDT", "-USD", 1)
	}
	return nil
}

func (c *CoinbaseConnector) Disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.running = false
	if c.cancel != nil {
		c.cancel()
	}
	if c.ws != nil {
		c.ws.Close()
	}
}

func (c *CoinbaseConnector) Run(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	defer c.cancel()
	c.mu.Lock()
	c.running = true
	c.mu.Unlock()
	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
		}
		if err := c.connectAndStream(); err != nil {
			slog.Error("coinbase stream error", "err", err)
			select {
			case <-c.ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}
}

func (c *CoinbaseConnector) connectAndStream() error {
	ws, _, err := websocket.DefaultDialer.Dial(c.wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	c.ws = ws

	sub := map[string]interface{}{
		"type":        "subscribe",
		"product_ids": []string{c.productID},
		"channels":    []string{"ticker", "matches", "level2"},
	}
	if err := ws.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
		}
		ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if err := c.handleMessage(msg); err != nil {
			slog.Warn("coinbase handle message", "err", err)
		}
	}
}

func (c *CoinbaseConnector) handleMessage(msg []byte) error {
	var wrapper map[string]interface{}
	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return err
	}
	msgType, _ := wrapper["type"].(string)
	switch msgType {
	case "match", "last_match":
		return c.handleMatch(msg)
	case "snapshot":
		return c.handleOrderbookSnapshot(msg)
	case "l2update":
		return c.handleOrderbookUpdate(msg)
	case "ticker":
		return nil
	case "subscriptions", "heartbeat":
		return nil
	}
	return nil
}

func (c *CoinbaseConnector) handleMatch(data []byte) error {
	var m struct {
		Price     string `json:"price"`
		Size      string `json:"size"`
		Side      string `json:"side"`
		Time      string `json:"time"`
		TradeID   int    `json:"trade_id"`
		MakerSide string `json:"maker_side"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	price, _ := strconv.ParseFloat(m.Price, 64)
	qty, _ := strconv.ParseFloat(m.Size, 64)
	if price == 0 || qty == 0 {
		return nil
	}
	ts, _ := time.Parse(time.RFC3339Nano, m.Time)
	if ts.IsZero() {
		ts = time.Now()
	}
	isMaker := strings.ToLower(m.Side) == "sell"
	trade := Trade{
		Exchange:     c.name,
		Symbol:       c.symbol,
		MarketType:   "spot",
		Price:        price,
		Qty:          qty,
		QuoteQty:     price * qty,
		Side:         strings.ToLower(m.Side),
		IsBuyerMaker: isMaker,
		Timestamp:    ts,
		TradeID:      strconv.Itoa(m.TradeID),
	}
	if c.onTrade != nil {
		c.onTrade(trade)
	}
	return nil
}

func (c *CoinbaseConnector) handleOrderbookSnapshot(data []byte) error {
	var snapshot struct {
		Bids [][2]string `json:"bids"`
		Asks [][2]string `json:"asks"`
	}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}
	c.orderbook.mu.Lock()
	c.orderbook.bids = make(map[string]float64)
	c.orderbook.asks = make(map[string]float64)
	for _, b := range snapshot.Bids {
		price := b[0]
		qty, _ := strconv.ParseFloat(b[1], 64)
		if qty == 0 {
			delete(c.orderbook.bids, price)
		} else {
			c.orderbook.bids[price] = qty
		}
	}
	for _, a := range snapshot.Asks {
		price := a[0]
		qty, _ := strconv.ParseFloat(a[1], 64)
		if qty == 0 {
			delete(c.orderbook.asks, price)
		} else {
			c.orderbook.asks[price] = qty
		}
	}
	c.orderbook.mu.Unlock()
	return nil
}

func (c *CoinbaseConnector) handleOrderbookUpdate(data []byte) error {
	var update struct {
		Changes [][3]string `json:"changes"`
	}
	if err := json.Unmarshal(data, &update); err != nil {
		return err
	}
	c.orderbook.mu.Lock()
	for _, ch := range update.Changes {
		if len(ch) < 3 {
			continue
		}
		side := ch[0]
		price := ch[1]
		qty, _ := strconv.ParseFloat(ch[2], 64)
		if side == "buy" {
			if qty == 0 {
				delete(c.orderbook.bids, price)
			} else {
				c.orderbook.bids[price] = qty
			}
		} else {
			if qty == 0 {
				delete(c.orderbook.asks, price)
			} else {
				c.orderbook.asks[price] = qty
			}
		}
	}
	c.orderbook.mu.Unlock()
	return nil
}

func (c *CoinbaseConnector) EmitOrderbookSnapshot(tickSize float64) {
	if c.orderbook == nil {
		return
	}
	c.orderbook.mu.RLock()
	allPrices := make(map[string]struct{})
	for p := range c.orderbook.bids {
		allPrices[p] = struct{}{}
	}
	for p := range c.orderbook.asks {
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
			BidQty: c.orderbook.bids[p],
			AskQty: c.orderbook.asks[p],
		})
	}
	c.orderbook.mu.RUnlock()
	if c.onOrderbookSnapshot != nil {
		c.onOrderbookSnapshot(OrderbookSnapshot{
			Exchange:   c.name,
			Symbol:     c.symbol,
			MarketType: "spot",
			TickSize:   tickSize,
			Levels:     levels,
			Timestamp:  time.Now(),
		})
	}
}
