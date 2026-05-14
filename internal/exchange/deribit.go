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

type DeribitConnector struct {
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

func NewDeribitConnector() *DeribitConnector {
	return &DeribitConnector{
		name:  "DERIBIT",
		wsURL: "wss://www.deribit.com/ws/api/v2",
	}
}

func (d *DeribitConnector) Name() string { return d.name }

func (d *DeribitConnector) MarketTypes() []string { return []string{"perp"} }

func (d *DeribitConnector) OnTrade(cb func(Trade)) { d.onTrade = cb }

func (d *DeribitConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { d.onOrderbookSnapshot = cb }

func (d *DeribitConnector) OnLiquidation(cb func(Liquidation)) { d.onLiquidation = cb }

func (d *DeribitConnector) OnMarketStat(cb func(MarketStat)) { d.onMarketStat = cb }

func (d *DeribitConnector) Connect(markets []config.MarketConfig) error {
	if len(markets) == 0 {
		return fmt.Errorf("no markets configured")
	}
	d.markets = append([]config.MarketConfig(nil), markets...)
	return nil
}

func (d *DeribitConnector) Disconnect() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.running = false
	d.MarkDisconnected("shutdown")
	if d.cancel != nil {
		d.cancel()
	}
	if d.ws != nil {
		_ = d.ws.Close()
	}
}

func (d *DeribitConnector) Run(ctx context.Context) error {
	d.ctx, d.cancel = context.WithCancel(ctx)
	defer d.cancel()
	d.MarkConnecting("connecting")

	d.mu.Lock()
	d.running = true
	d.mu.Unlock()

	for {
		select {
		case <-d.ctx.Done():
			return nil
		default:
		}

		if err := d.connectAndStream(); err != nil {
			slog.Error("deribit stream error", "err", err)
			delay := d.MarkReconnectScheduled("reconnect_scheduled")
			select {
			case <-d.ctx.Done():
				return nil
			case <-time.After(delay):
			}
		}
	}
}

func (d *DeribitConnector) connectAndStream() error {
	ws, _, err := websocket.DefaultDialer.Dial(d.wsURL, nil)
	if err != nil {
		d.MarkDisconnected("dial_error")
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	d.ws = ws

	channels := make([]string, 0, len(d.markets))
	for _, market := range d.markets {
		channels = append(channels, fmt.Sprintf("trades.%s.100ms", strings.ToUpper(market.Pair)))
	}
	if err := ws.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "public/subscribe", "params": map[string]any{"channels": channels}}); err != nil {
		d.MarkDisconnected("subscribe_error")
		return fmt.Errorf("subscribe: %w", err)
	}
	d.MarkConnected("connected")
	go d.heartbeatLoop(ws)

	for {
		select {
		case <-d.ctx.Done():
			return nil
		default:
		}

		_ = ws.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			if d.ctx.Err() != nil {
				return nil
			}
			d.MarkDisconnected("read_error")
			return fmt.Errorf("read: %w", err)
		}
		d.MarkMessageReceived()
		if err := d.handleMessage(ws, msg); err != nil {
			slog.Warn("deribit handle message", "err", err)
		}
	}
}

func (d *DeribitConnector) heartbeatLoop(ws *websocket.Conn) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			if err := ws.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 9999, "method": "public/test", "params": map[string]any{}}); err != nil {
				return
			}
		}
	}
}

func (d *DeribitConnector) handleMessage(ws *websocket.Conn, msg []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(msg, &raw); err != nil {
		return err
	}
	method := fmt.Sprint(raw["method"])
	if method == "heartbeat" || method == "test_request" {
		return ws.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 9998, "method": "public/test", "params": map[string]any{}})
	}
	if method != "subscription" {
		return nil
	}
	params, _ := raw["params"].(map[string]any)
	channel := fmt.Sprint(params["channel"])
	items, _ := params["data"].([]any)
	parts := strings.Split(channel, ".")
	if len(parts) < 2 {
		return nil
	}
	pair := strings.ToUpper(parts[1])
	for _, item := range items {
		tradeMap, ok := item.(map[string]any)
		if !ok {
			continue
		}
		price, err := parseFloatString(fmt.Sprint(tradeMap["price"]))
		if err != nil || price <= 0 {
			continue
		}
		amount, err := parseFloatString(fmt.Sprint(tradeMap["amount"]))
		if err != nil || amount <= 0 {
			continue
		}
		ts, ok := tradeMap["timestamp"].(float64)
		if !ok || ts <= 0 {
			continue
		}
		side := strings.ToLower(fmt.Sprint(tradeMap["direction"]))
		if side != "buy" && side != "sell" {
			continue
		}
		liq := tradeMap["liquidation"] == true
		quoteQty := amount
		if strings.Contains(strings.ToUpper(pair), "USDC") || strings.Contains(strings.ToUpper(pair), "USDT") {
			quoteQty = amount * price
		}
		trade := Trade{Exchange: d.name, Symbol: pair, MarketType: "perp", Price: price, Qty: amount, QuoteQty: quoteQty, Side: side, IsLiquidation: liq, Timestamp: time.UnixMilli(int64(ts)).UTC()}
		d.MarkTrade(trade.Timestamp)
		if d.onTrade != nil {
			d.onTrade(trade)
		}
		if liq && d.onLiquidation != nil {
			d.onLiquidation(Liquidation{Exchange: d.name, Symbol: pair, MarketType: "perp", Side: side, Price: price, Qty: amount, QuoteQty: quoteQty, Timestamp: trade.Timestamp})
		}
	}
	return nil
}
