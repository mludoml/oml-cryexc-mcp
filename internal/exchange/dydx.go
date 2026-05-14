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

type DydxConnector struct {
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

func NewDydxConnector() *DydxConnector {
	return &DydxConnector{
		name:  "DYDX",
		wsURL: "wss://indexer.dydx.trade/v4/ws",
	}
}

func (d *DydxConnector) Name() string { return d.name }

func (d *DydxConnector) MarketTypes() []string { return []string{"perp"} }

func (d *DydxConnector) OnTrade(cb func(Trade)) { d.onTrade = cb }

func (d *DydxConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { d.onOrderbookSnapshot = cb }

func (d *DydxConnector) OnLiquidation(cb func(Liquidation)) { d.onLiquidation = cb }

func (d *DydxConnector) OnMarketStat(cb func(MarketStat)) { d.onMarketStat = cb }

func (d *DydxConnector) Connect(markets []config.MarketConfig) error {
	if len(markets) == 0 {
		return fmt.Errorf("no markets configured")
	}
	d.markets = append([]config.MarketConfig(nil), markets...)
	return nil
}

func (d *DydxConnector) Disconnect() {
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

func (d *DydxConnector) Run(ctx context.Context) error {
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
			slog.Error("dydx stream error", "err", err)
			delay := d.MarkReconnectScheduled("reconnect_scheduled")
			select {
			case <-d.ctx.Done():
				return nil
			case <-time.After(delay):
			}
		}
	}
}

func (d *DydxConnector) connectAndStream() error {
	ws, _, err := websocket.DefaultDialer.Dial(d.wsURL, nil)
	if err != nil {
		d.MarkDisconnected("dial_error")
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	d.ws = ws

	for _, market := range d.markets {
		if err := ws.WriteJSON(map[string]any{
			"type":    "subscribe",
			"channel": "v4_trades",
			"id":      strings.ToUpper(market.Pair),
		}); err != nil {
			d.MarkDisconnected("subscribe_error")
			return fmt.Errorf("subscribe %s: %w", market.Pair, err)
		}
	}
	d.MarkConnected("connected")

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
		if err := d.handleMessage(msg); err != nil {
			slog.Warn("dydx handle message", "err", err)
		}
	}
}

func (d *DydxConnector) handleMessage(msg []byte) error {
	var wrapper struct {
		Type     string `json:"type"`
		ID       string `json:"id"`
		Contents struct {
			Trades []struct {
				Price     string `json:"price"`
				Size      string `json:"size"`
				Side      string `json:"side"`
				CreatedAt string `json:"createdAt"`
			} `json:"trades"`
		} `json:"contents"`
	}
	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return err
	}
	if wrapper.Type != "channel_data" || len(wrapper.Contents.Trades) == 0 {
		return nil
	}
	for _, t := range wrapper.Contents.Trades {
		price, err := parseFloatString(t.Price)
		if err != nil || price <= 0 {
			continue
		}
		qty, err := parseFloatString(t.Size)
		if err != nil || qty <= 0 {
			continue
		}
		ts, err := time.Parse(time.RFC3339Nano, t.CreatedAt)
		if err != nil {
			continue
		}
		side := strings.ToLower(t.Side)
		if side != "buy" && side != "sell" {
			continue
		}
		trade := Trade{Exchange: d.name, Symbol: strings.ToUpper(wrapper.ID), MarketType: "perp", Price: price, Qty: qty, QuoteQty: price * qty, Side: side, Timestamp: ts.UTC()}
		d.MarkTrade(trade.Timestamp)
		if d.onTrade != nil {
			d.onTrade(trade)
		}
	}
	return nil
}
