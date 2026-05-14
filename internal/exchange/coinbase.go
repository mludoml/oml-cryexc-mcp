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

type CoinbaseConnector struct {
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

	orderbooks map[string]*coinbaseOrderbook

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
		name:       "COINBASE",
		wsURL:      "wss://advanced-trade-ws.coinbase.com",
		orderbooks: make(map[string]*coinbaseOrderbook),
	}
}

func (c *CoinbaseConnector) Name() string { return c.name }

func (c *CoinbaseConnector) MarketTypes() []string { return []string{"spot", "perp"} }

func (c *CoinbaseConnector) OnTrade(cb func(Trade)) { c.onTrade = cb }

func (c *CoinbaseConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) {
	c.onOrderbookSnapshot = cb
}

func (c *CoinbaseConnector) OnLiquidation(cb func(Liquidation)) {}

func (c *CoinbaseConnector) OnMarketStat(cb func(MarketStat)) {}

func (c *CoinbaseConnector) Connect(markets []config.MarketConfig) error {
	if len(markets) == 0 {
		return fmt.Errorf("no markets configured")
	}

	c.markets = append([]config.MarketConfig(nil), markets...)
	c.marketType = string(markets[0].Type)
	c.orderbooks = make(map[string]*coinbaseOrderbook, len(markets))
	for _, market := range markets {
		productID := coinbaseNormalizePair(market.Pair)
		c.orderbooks[productID] = &coinbaseOrderbook{bids: make(map[string]float64), asks: make(map[string]float64)}
	}
	return nil
}

func (c *CoinbaseConnector) Disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.running = false
	c.MarkDisconnected("shutdown")
	if c.cancel != nil {
		c.cancel()
	}
	if c.ws != nil {
		_ = c.ws.Close()
	}
}

func (c *CoinbaseConnector) Run(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	defer c.cancel()
	c.MarkConnecting("connecting")

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
			slog.Error("coinbase stream error", "market", c.marketType, "err", err)
			delay := c.MarkReconnectScheduled("reconnect_scheduled")
			select {
			case <-c.ctx.Done():
				return nil
			case <-time.After(delay):
			}
		}
	}
}

func (c *CoinbaseConnector) connectAndStream() error {
	ws, _, err := websocket.DefaultDialer.Dial(c.wsURL, nil)
	if err != nil {
		c.MarkDisconnected("dial_error")
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	c.ws = ws

	productIDs := make([]string, 0, len(c.markets))
	for _, market := range c.markets {
		productIDs = append(productIDs, coinbaseNormalizePair(market.Pair))
	}

	if err := ws.WriteJSON(map[string]any{
		"type":        "subscribe",
		"product_ids": productIDs,
		"channel":     "market_trades",
	}); err != nil {
		c.MarkDisconnected("subscribe_error")
		return fmt.Errorf("subscribe market_trades: %w", err)
	}
	if err := ws.WriteJSON(map[string]any{
		"type":        "subscribe",
		"product_ids": productIDs,
		"channel":     "heartbeats",
	}); err != nil {
		c.MarkDisconnected("subscribe_error")
		return fmt.Errorf("subscribe heartbeats: %w", err)
	}

	c.MarkConnected("connected")

	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
		}

		_ = ws.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			c.MarkDisconnected("read_error")
			return fmt.Errorf("read: %w", err)
		}
		c.MarkMessageReceived()

		if err := c.handleMessage(msg); err != nil {
			slog.Warn("coinbase handle message", "err", err)
		}
	}
}

func (c *CoinbaseConnector) handleMessage(msg []byte) error {
	var wrapper struct {
		Channel string          `json:"channel"`
		Events  json.RawMessage `json:"events"`
		Type    string          `json:"type"`
	}
	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return err
	}

	switch wrapper.Channel {
	case "market_trades":
		return c.handleMarketTrades(wrapper.Events)
	case "heartbeats":
		return nil
	}

	switch wrapper.Type {
	case "subscriptions", "heartbeat":
		return nil
	}

	return nil
}

func (c *CoinbaseConnector) handleMarketTrades(data []byte) error {
	var events []struct {
		Trades []struct {
			ProductID string `json:"product_id"`
			Price     string `json:"price"`
			Size      string `json:"size"`
			Side      string `json:"side"`
			Time      string `json:"time"`
			TradeID   string `json:"trade_id"`
		} `json:"trades"`
	}
	if err := json.Unmarshal(data, &events); err != nil {
		return err
	}

	for _, event := range events {
		for _, t := range event.Trades {
			price, _ := strconv.ParseFloat(t.Price, 64)
			qty, _ := strconv.ParseFloat(t.Size, 64)
			if price == 0 || qty == 0 {
				continue
			}

			ts, err := time.Parse(time.RFC3339Nano, t.Time)
			if err != nil {
				continue
			}

			productID := strings.ToUpper(t.ProductID)
			side := strings.ToLower(t.Side)
			if side != "buy" && side != "sell" {
				continue
			}

			trade := Trade{
				Exchange:   c.name,
				Symbol:     productID,
				MarketType: coinbaseMarketType(productID),
				Price:      price,
				Qty:        qty,
				QuoteQty:   price * qty,
				Side:       side,
				Timestamp:  ts.UTC(),
				TradeID:    t.TradeID,
			}
			c.MarkTrade(trade.Timestamp)
			if c.onTrade != nil {
				c.onTrade(trade)
			}
		}
	}

	return nil
}

func coinbaseMarketType(productID string) string {
	if strings.Contains(strings.ToUpper(productID), "-PERP") {
		return "perp"
	}
	return "spot"
}

func coinbaseNormalizePair(pair string) string {
	upper := strings.ToUpper(pair)
	switch upper {
	case "BTCUSDT":
		return "BTC-USD"
	case "BTCUSDC":
		return "BTC-USDC"
	case "BTCUSD":
		return "BTC-USD"
	default:
		return upper
	}
}

func (c *CoinbaseConnector) EmitOrderbookSnapshot(tickSize float64) {
	for productID, book := range c.orderbooks {
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

		if c.onOrderbookSnapshot != nil {
			c.onOrderbookSnapshot(OrderbookSnapshot{
				Exchange:   c.name,
				Symbol:     productID,
				MarketType: coinbaseMarketType(productID),
				TickSize:   tickSize,
				Levels:     levels,
				Timestamp:  time.Now().UTC(),
			})
		}
	}
}
