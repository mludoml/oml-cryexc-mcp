package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"oml-aggr-mcp/internal/config"
)

type BitstampConnector struct {
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

func NewBitstampConnector() *BitstampConnector {
	return &BitstampConnector{
		name:  "BITSTAMP",
		wsURL: "wss://ws.bitstamp.net",
	}
}

func (b *BitstampConnector) Name() string { return b.name }

func (b *BitstampConnector) MarketTypes() []string { return []string{"spot"} }

func (b *BitstampConnector) OnTrade(cb func(Trade)) { b.onTrade = cb }

func (b *BitstampConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { b.onOrderbookSnapshot = cb }

func (b *BitstampConnector) OnLiquidation(cb func(Liquidation)) { b.onLiquidation = cb }

func (b *BitstampConnector) OnMarketStat(cb func(MarketStat)) { b.onMarketStat = cb }

func (b *BitstampConnector) Connect(markets []config.MarketConfig) error {
	if len(markets) == 0 {
		return fmt.Errorf("no markets configured")
	}
	b.markets = append([]config.MarketConfig(nil), markets...)
	return nil
}

func (b *BitstampConnector) Disconnect() {
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

func (b *BitstampConnector) Run(ctx context.Context) error {
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
			slog.Error("bitstamp stream error", "err", err)
			delay := b.MarkReconnectScheduled("reconnect_scheduled")
			select {
			case <-b.ctx.Done():
				return nil
			case <-time.After(delay):
			}
		}
	}
}

func (b *BitstampConnector) connectAndStream() error {
	ws, _, err := websocket.DefaultDialer.Dial(b.wsURL, nil)
	if err != nil {
		b.MarkDisconnected("dial_error")
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	b.ws = ws

	for _, market := range b.markets {
		channel := "live_trades_" + strings.ToLower(market.Pair)
		if err := ws.WriteJSON(map[string]any{
			"event": "bts:subscribe",
			"data": map[string]string{"channel": channel},
		}); err != nil {
			b.MarkDisconnected("subscribe_error")
			return fmt.Errorf("subscribe %s: %w", channel, err)
		}
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
			slog.Warn("bitstamp handle message", "err", err)
		}
	}
}

func (b *BitstampConnector) handleMessage(msg []byte) error {
	var wrapper struct {
		Event   string `json:"event"`
		Channel string `json:"channel"`
		Data    struct {
			Price          any         `json:"price"`
			Amount         any         `json:"amount"`
			Type           any         `json:"type"`
			MicroTimestamp string      `json:"microtimestamp"`
			Timestamp      any         `json:"timestamp"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return err
	}

	if wrapper.Event != "trade" {
		return nil
	}

	price, err := parseFlexibleFloat(wrapper.Data.Price)
	if err != nil || price <= 0 {
		return nil
	}
	amount, err := parseFlexibleFloat(wrapper.Data.Amount)
	if err != nil || amount <= 0 {
		return nil
	}

	trade := Trade{
		Exchange:   b.name,
		Symbol:     strings.TrimPrefix(strings.ToLower(wrapper.Channel), "live_trades_"),
		MarketType: "spot",
		Price:      price,
		Qty:        amount,
		QuoteQty:   price * amount,
		Side:       bitstampSide(wrapper.Data.Type),
		Timestamp:  bitstampTimestamp(wrapper.Data.MicroTimestamp, wrapper.Data.Timestamp),
	}
	if trade.Symbol == "" || trade.Side == "" || trade.Timestamp.IsZero() {
		return nil
	}

	b.MarkTrade(trade.Timestamp)
	if b.onTrade != nil {
		b.onTrade(trade)
	}
	return nil
}

func bitstampSide(value any) string {
	switch v := value.(type) {
	case float64:
		if int(v) == 0 {
			return "buy"
		}
		return "sell"
	case string:
		if v == "0" || strings.EqualFold(v, "buy") {
			return "buy"
		}
		if v == "1" || strings.EqualFold(v, "sell") {
			return "sell"
		}
	}
	return ""
}

func bitstampTimestamp(micro string, fallback any) time.Time {
	if micro != "" {
		if micros, err := parseInt64String(micro); err == nil && micros > 0 {
			return time.UnixMicro(micros).UTC()
		}
	}
	switch v := fallback.(type) {
	case string:
		if secs, err := parseInt64String(v); err == nil && secs > 0 {
			return time.Unix(secs, 0).UTC()
		}
	case float64:
		if v > 0 {
			return time.Unix(int64(v), 0).UTC()
		}
	}
	return time.Time{}
}
