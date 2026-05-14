package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"oml-aggr-mcp/internal/config"
)

type BinanceFuturesConnector struct {
	ConnectorRuntime
	name        string
	baseURL     string
	routePrefix string
	mode        string
	markets     []config.MarketConfig

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

func NewBinanceBtcusdtPerpConnector() *BinanceFuturesConnector {
	return &BinanceFuturesConnector{name: "BINANCE_BTCUSDT_PERP", baseURL: "wss://fstream.binance.com", routePrefix: "/market", mode: "linear"}
}

func NewBinanceBtcusdInverseConnector() *BinanceFuturesConnector {
	return &BinanceFuturesConnector{name: "BINANCE_BTCUSD_INVERSE", baseURL: "wss://dstream.binance.com", routePrefix: "", mode: "inverse"}
}

func (b *BinanceFuturesConnector) Name() string { return b.name }

func (b *BinanceFuturesConnector) MarketTypes() []string { return []string{"perp"} }

func (b *BinanceFuturesConnector) OnTrade(cb func(Trade)) { b.onTrade = cb }

func (b *BinanceFuturesConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { b.onOrderbookSnapshot = cb }

func (b *BinanceFuturesConnector) OnLiquidation(cb func(Liquidation)) { b.onLiquidation = cb }

func (b *BinanceFuturesConnector) OnMarketStat(cb func(MarketStat)) { b.onMarketStat = cb }

func (b *BinanceFuturesConnector) Connect(markets []config.MarketConfig) error {
	if len(markets) == 0 {
		return fmt.Errorf("no markets configured")
	}
	b.markets = append([]config.MarketConfig(nil), markets...)
	return nil
}

func (b *BinanceFuturesConnector) Disconnect() {
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

func (b *BinanceFuturesConnector) Run(ctx context.Context) error {
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
			slog.Error("binance futures stream error", "exchange", b.name, "err", err)
			delay := b.MarkReconnectScheduled("reconnect_scheduled")
			select {
			case <-b.ctx.Done():
				return nil
			case <-time.After(delay):
			}
		}
	}
}

func (b *BinanceFuturesConnector) connectAndStream() error {
	streams := make([]string, 0, len(b.markets)*2)
	for _, market := range b.markets {
		symbol := strings.ToLower(market.Pair)
		streams = append(streams, symbol+"@aggTrade", symbol+"@forceOrder")
	}
	u, _ := url.Parse(b.baseURL)
	u.Path = b.routePrefix + "/stream"
	q := u.Query()
	q.Set("streams", strings.Join(streams, "/"))
	u.RawQuery = q.Encode()

	ws, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
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
			slog.Warn("binance futures handle message", "exchange", b.name, "err", err)
		}
	}
}

func (b *BinanceFuturesConnector) handleMessage(msg []byte) error {
	var wrapper struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return err
	}
	d := wrapper.Data
	eventType := fmt.Sprint(d["e"])
	switch eventType {
	case "aggTrade":
		return b.handleTrade(d)
	case "forceOrder":
		return b.handleLiquidationMsg(d)
	default:
		return nil
	}
}

func (b *BinanceFuturesConnector) handleTrade(d map[string]any) error {
	symbol := strings.ToUpper(fmt.Sprint(d["s"]))
	price, err := parseFloatString(fmt.Sprint(d["p"]))
	if err != nil || price <= 0 {
		return nil
	}
	qty, err := parseFloatString(fmt.Sprint(d["q"]))
	if err != nil || qty <= 0 {
		return nil
	}
	ts, ok := d["T"].(float64)
	if !ok || ts <= 0 {
		return nil
	}
	side := "buy"
	if maker, ok := d["m"].(bool); ok && maker {
		side = "sell"
	}
	trade := Trade{Exchange: b.name, Symbol: symbol, MarketType: "perp", Price: price, Qty: qty, QuoteQty: b.quoteQty(price, qty), Side: side, Timestamp: time.UnixMilli(int64(ts)).UTC()}
	b.MarkTrade(trade.Timestamp)
	if b.onTrade != nil {
		b.onTrade(trade)
	}
	return nil
}

func (b *BinanceFuturesConnector) handleLiquidationMsg(d map[string]any) error {
	order, _ := d["o"].(map[string]any)
	if order == nil {
		return nil
	}
	symbol := strings.ToUpper(fmt.Sprint(order["s"]))
	price, err := parseFloatString(fmt.Sprint(order["ap"]))
	if err != nil || price <= 0 {
		price, err = parseFloatString(fmt.Sprint(order["p"]))
		if err != nil || price <= 0 {
			return nil
		}
	}
	qty, err := parseFloatString(fmt.Sprint(order["z"]))
	if err != nil || qty <= 0 {
		qty, err = parseFloatString(fmt.Sprint(order["q"]))
		if err != nil || qty <= 0 {
			return nil
		}
	}
	ts, ok := order["T"].(float64)
	if !ok || ts <= 0 {
		if evt, ok2 := d["E"].(float64); ok2 && evt > 0 {
			ts = evt
		} else {
			return nil
		}
	}
	side := strings.ToLower(fmt.Sprint(order["S"]))
	liq := Liquidation{Exchange: b.name, Symbol: symbol, MarketType: "perp", Price: price, Qty: qty, QuoteQty: b.quoteQty(price, qty), Side: side, Timestamp: time.UnixMilli(int64(ts)).UTC()}
	if b.onLiquidation != nil {
		b.onLiquidation(liq)
	}
	return nil
}

func (b *BinanceFuturesConnector) quoteQty(price, qty float64) float64 {
	if b.mode == "inverse" {
		return qty * 100
	}
	return price * qty
}
