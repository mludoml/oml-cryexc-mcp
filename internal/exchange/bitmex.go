package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"oml-aggr-mcp/internal/config"
)

type BitmexConnector struct {
	ConnectorRuntime
	name    string
	wsURL   string
	markets []config.MarketConfig

	ws      *websocket.Conn
	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	onTrade             func(Trade)
	onOrderbookSnapshot func(OrderbookSnapshot)
	onLiquidation       func(Liquidation)
	onMarketStat        func(MarketStat)
}

func NewBitmexConnector() *BitmexConnector {
	return &BitmexConnector{
		name:  "BITMEX",
		wsURL: "wss://www.bitmex.com/realtime",
	}
}

func (b *BitmexConnector) Name() string { return b.name }

func (b *BitmexConnector) MarketTypes() []string { return []string{"perp"} }

func (b *BitmexConnector) OnTrade(cb func(Trade)) { b.onTrade = cb }

func (b *BitmexConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { b.onOrderbookSnapshot = cb }

func (b *BitmexConnector) OnLiquidation(cb func(Liquidation)) { b.onLiquidation = cb }

func (b *BitmexConnector) OnMarketStat(cb func(MarketStat)) { b.onMarketStat = cb }

func (b *BitmexConnector) Connect(markets []config.MarketConfig) error {
	if len(markets) == 0 {
		return fmt.Errorf("no markets configured")
	}
	b.markets = append([]config.MarketConfig(nil), markets...)
	return nil
}

func (b *BitmexConnector) Disconnect() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.running = false
	b.MarkDisconnected("shutdown")
	if b.cancel != nil {
		b.cancel()
	}
	if b.ws != nil {
		_ = b.ws.Close()
	}
}

func (b *BitmexConnector) Run(ctx context.Context) error {
	b.ctx, b.cancel = context.WithCancel(ctx)
	defer b.cancel()
	b.MarkConnecting("connecting")

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
			slog.Error("bitmex stream error", "err", err)
			delay := b.MarkReconnectScheduled("reconnect_scheduled")
			select {
			case <-b.ctx.Done():
				return nil
			case <-time.After(delay):
			}
		}
	}
}

func (b *BitmexConnector) connectAndStream() error {
	ws, _, err := websocket.DefaultDialer.Dial(b.wsURL, nil)
	if err != nil {
		b.MarkDisconnected("dial_error")
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	b.ws = ws

	ws.SetPingHandler(func(appData string) error {
		deadline := time.Now().Add(5 * time.Second)
		return ws.WriteControl(websocket.PongMessage, []byte(appData), deadline)
	})

	args := make([]string, 0, len(b.markets)*2)
	for _, market := range b.markets {
		symbol := strings.ToUpper(market.Pair)
		args = append(args, "trade:"+symbol, "liquidation:"+symbol)
	}
	if err := ws.WriteJSON(map[string]any{"op": "subscribe", "args": args}); err != nil {
		b.MarkDisconnected("subscribe_error")
		return fmt.Errorf("subscribe: %w", err)
	}

	b.MarkConnected("connected")

	for {
		select {
		case <-b.ctx.Done():
			return nil
		default:
		}

		_ = ws.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			if b.ctx.Err() != nil {
				return nil
			}
			b.MarkDisconnected("read_error")
			return fmt.Errorf("read: %w", err)
		}
		b.MarkMessageReceived()
		if err := b.handleMessage(msg); err != nil {
			slog.Warn("bitmex handle message", "err", err)
		}
	}
}

func (b *BitmexConnector) handleMessage(msg []byte) error {
	var wrapper struct {
		Table  string            `json:"table"`
		Action string            `json:"action"`
		Data   []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return err
	}
	if wrapper.Table != "trade" && wrapper.Table != "liquidation" {
		return nil
	}
	if wrapper.Action != "insert" {
		return nil
	}

	for _, raw := range wrapper.Data {
		var item struct {
			Symbol    string `json:"symbol"`
			Price     any    `json:"price"`
			Size      any    `json:"size"`
			LeavesQty any    `json:"leavesQty"`
			Timestamp string `json:"timestamp"`
			Side      string `json:"side"`
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			continue
		}

		price, err := parseFlexibleFloat(item.Price)
		if err != nil || price <= 0 {
			continue
		}
		sizeValue := item.Size
		if fmt.Sprint(sizeValue) == "" {
			sizeValue = item.LeavesQty
		}
		qty, err := parseFlexibleFloat(sizeValue)
		if err != nil || qty <= 0 {
			continue
		}
		ts, err := time.Parse(time.RFC3339Nano, item.Timestamp)
		if err != nil {
			continue
		}
		symbol := strings.ToUpper(item.Symbol)
		side := strings.ToLower(item.Side)
		if side != "buy" && side != "sell" {
			continue
		}

		trade := Trade{
			Exchange:     b.name,
			Symbol:       symbol,
			MarketType:   "perp",
			Price:        price,
			Qty:          qty,
			QuoteQty:     bitmexQuoteQty(symbol, price, qty),
			Side:         side,
			IsBuyerMaker: side == "sell",
			IsLiquidation: wrapper.Table == "liquidation",
			Timestamp:    ts.UTC(),
		}
		b.MarkTrade(trade.Timestamp)
		if b.onTrade != nil {
			b.onTrade(trade)
		}
		if wrapper.Table == "liquidation" && b.onLiquidation != nil {
			b.onLiquidation(Liquidation{
				Exchange:   b.name,
				Symbol:     symbol,
				MarketType: "perp",
				Side:       side,
				Price:      price,
				Qty:        qty,
				QuoteQty:   trade.QuoteQty,
				Timestamp:  trade.Timestamp,
			})
		}
	}

	return nil
}

func bitmexQuoteQty(symbol string, price, qty float64) float64 {
	switch strings.ToUpper(symbol) {
	case "XBTUSD":
		return qty
	case "XBTUSDT":
		return qty * price / 1_000_000
	default:
		return qty
	}
}

func parseFlexibleFloat(value any) (float64, error) {
	switch v := value.(type) {
	case string:
		return parseFloatString(v)
	case float64:
		return v, nil
	case json.Number:
		return v.Float64()
	default:
		return 0, fmt.Errorf("unsupported float type %T", value)
	}
}

func parseFloatString(value string) (float64, error) {
	if value == "" {
		return 0, fmt.Errorf("empty float string")
	}
	return strconv.ParseFloat(value, 64)
}

func parseInt64String(value string) (int64, error) {
	if value == "" {
		return 0, fmt.Errorf("empty int string")
	}
	return strconv.ParseInt(value, 10, 64)
}
