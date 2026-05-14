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

type KrakenConnector struct {
	ConnectorRuntime
	name         string
	spotWSURL    string
	futuresWSURL string
	markets      []config.MarketConfig

	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc
	sockets map[string]*websocket.Conn

	groups []krakenSubscriptionGroup

	onTrade             func(Trade)
	onOrderbookSnapshot func(OrderbookSnapshot)
	onLiquidation       func(Liquidation)
	onMarketStat        func(MarketStat)
}

type krakenSubscriptionGroup struct {
	key     string
	wsURL   string
	pairs   []string
	feed    string
	isPerp  bool
}

func NewKrakenConnector() *KrakenConnector {
	return &KrakenConnector{
		name:         "KRAKEN",
		spotWSURL:    "wss://ws.kraken.com/v2",
		futuresWSURL: "wss://futures.kraken.com/ws/v1",
		sockets:      make(map[string]*websocket.Conn),
	}
}

func (k *KrakenConnector) Name() string { return k.name }

func (k *KrakenConnector) MarketTypes() []string { return []string{"spot", "perp"} }

func (k *KrakenConnector) OnTrade(cb func(Trade)) { k.onTrade = cb }

func (k *KrakenConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { k.onOrderbookSnapshot = cb }

func (k *KrakenConnector) OnLiquidation(cb func(Liquidation)) { k.onLiquidation = cb }

func (k *KrakenConnector) OnMarketStat(cb func(MarketStat)) { k.onMarketStat = cb }

func (k *KrakenConnector) Connect(markets []config.MarketConfig) error {
	if len(markets) == 0 {
		return fmt.Errorf("no markets configured")
	}
	k.markets = append([]config.MarketConfig(nil), markets...)
	k.groups = k.groups[:0]

	spotPairs := make([]string, 0)
	perpPairs := make([]string, 0)
	for _, market := range markets {
		pair := strings.ToUpper(market.Pair)
		if market.Type == config.MarketTypePerp || strings.HasPrefix(pair, "PI_") || strings.HasPrefix(pair, "PF_") {
			perpPairs = append(perpPairs, pair)
		} else {
			spotPairs = append(spotPairs, pair)
		}
	}
	if len(spotPairs) > 0 {
		k.groups = append(k.groups, krakenSubscriptionGroup{key: "spot", wsURL: k.spotWSURL, pairs: spotPairs, feed: "trade", isPerp: false})
	}
	if len(perpPairs) > 0 {
		k.groups = append(k.groups, krakenSubscriptionGroup{key: "perp", wsURL: k.futuresWSURL, pairs: perpPairs, feed: "trade", isPerp: true})
	}
	return nil
}

func (k *KrakenConnector) Disconnect() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.running = false
	k.MarkDisconnected("shutdown")
	if k.cancel != nil {
		k.cancel()
	}
	for _, ws := range k.sockets {
		_ = ws.Close()
	}
	k.sockets = make(map[string]*websocket.Conn)
}

func (k *KrakenConnector) Run(ctx context.Context) error {
	k.ctx, k.cancel = context.WithCancel(ctx)
	defer k.cancel()
	k.MarkConnecting("connecting")

	k.mu.Lock()
	k.running = true
	k.mu.Unlock()

	errCh := make(chan error, len(k.groups))
	var wg sync.WaitGroup
	for _, group := range k.groups {
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-k.ctx.Done():
					return
				default:
				}

				if err := k.connectAndStream(group); err != nil {
					slog.Error("kraken stream error", "group", group.key, "err", err)
					delay := k.MarkReconnectScheduled("reconnect_scheduled")
					select {
					case <-k.ctx.Done():
						return
					case <-time.After(delay):
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for {
		select {
		case <-k.ctx.Done():
			return nil
		case err, ok := <-errCh:
			if !ok {
				return nil
			}
			if err != nil {
				return err
			}
		}
	}
}

func (k *KrakenConnector) connectAndStream(group krakenSubscriptionGroup) error {
	ws, _, err := websocket.DefaultDialer.Dial(group.wsURL, nil)
	if err != nil {
		k.MarkDisconnected("dial_error")
		return fmt.Errorf("dial %s: %w", group.key, err)
	}
	defer ws.Close()
	k.mu.Lock()
	k.sockets[group.key] = ws
	k.mu.Unlock()

	ws.SetPingHandler(func(appData string) error {
		deadline := time.Now().Add(5 * time.Second)
		return ws.WriteControl(websocket.PongMessage, []byte(appData), deadline)
	})

	if err := k.subscribe(ws, group); err != nil {
		k.MarkDisconnected("subscribe_error")
		return err
	}
	k.MarkConnected("connected")

	for {
		select {
		case <-k.ctx.Done():
			return nil
		default:
		}

		_ = ws.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			if k.ctx.Err() != nil {
				return nil
			}
			k.MarkDisconnected("read_error")
			return fmt.Errorf("read %s: %w", group.key, err)
		}
		k.MarkMessageReceived()
		if err := k.handleMessage(group, msg); err != nil {
			slog.Warn("kraken handle message", "group", group.key, "err", err)
		}
	}
}

func (k *KrakenConnector) subscribe(ws *websocket.Conn, group krakenSubscriptionGroup) error {
	if group.isPerp {
		return ws.WriteJSON(map[string]any{
			"event":   "subscribe",
			"feed":    group.feed,
			"product_ids": group.pairs,
		})
	}
	normalizedPairs := make([]string, 0, len(group.pairs))
	for _, pair := range group.pairs {
		normalizedPairs = append(normalizedPairs, krakenNormalizeSpotPair(pair))
	}
	return ws.WriteJSON(map[string]any{
		"method": "subscribe",
		"params": map[string]any{
			"channel": "trade",
			"symbol":  normalizedPairs,
		},
	})
}

func (k *KrakenConnector) handleMessage(group krakenSubscriptionGroup, msg []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(msg, &raw); err != nil {
		return err
	}

	if !group.isPerp {
		if channel, _ := raw["channel"].(string); channel != "trade" {
			return nil
		}
		data, _ := raw["data"].([]any)
		for _, item := range data {
			d, ok := item.(map[string]any)
			if !ok {
				continue
			}
			pair := strings.ToUpper(fmt.Sprint(d["symbol"]))
			price, err := parseFloatString(fmt.Sprint(d["price"]))
			if err != nil || price <= 0 {
				continue
			}
			qty, err := parseFloatString(fmt.Sprint(d["qty"]))
			if err != nil || qty <= 0 {
				continue
			}
			ts, ok := krakenSpotTimestamp(d["timestamp"])
			if !ok {
				continue
			}
			side := strings.ToLower(fmt.Sprint(d["side"]))
			if side != "buy" && side != "sell" {
				continue
			}
			trade := Trade{Exchange: k.name, Symbol: pair, MarketType: "spot", Price: price, Qty: qty, QuoteQty: price * qty, Side: side, Timestamp: ts}
			k.MarkTrade(trade.Timestamp)
			if k.onTrade != nil {
				k.onTrade(trade)
			}
		}
		return nil
	}

	if feed, _ := raw["feed"].(string); feed != "trade" {
		return nil
	}
	pair := strings.ToUpper(fmt.Sprint(raw["product_id"]))
	price, err := parseFloatString(fmt.Sprint(raw["price"]))
	if err != nil || price <= 0 {
		return nil
	}
	qty, err := parseFloatString(fmt.Sprint(raw["qty"]))
	if err != nil || qty <= 0 {
		return nil
	}
	side := strings.ToLower(fmt.Sprint(raw["side"]))
	if side != "buy" && side != "sell" {
		return nil
	}
	trade := Trade{Exchange: k.name, Symbol: pair, MarketType: "perp", Price: price, Qty: qty, QuoteQty: krakenQuoteQty(pair, price, qty), Side: side, Timestamp: time.Now().UTC()}
	k.MarkTrade(trade.Timestamp)
	if k.onTrade != nil {
		k.onTrade(trade)
	}
	return nil
}

func krakenQuoteQty(pair string, price, qty float64) float64 {
	upper := strings.ToUpper(pair)
	if strings.HasPrefix(upper, "PI_") {
		return qty
	}
	return price * qty
}

func krakenSpotTimestamp(value any) (time.Time, bool) {
	switch v := value.(type) {
	case float64:
		if v <= 0 {
			return time.Time{}, false
		}
		return time.UnixMilli(int64(v * 1000)).UTC(), true
	case string:
		if ts, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return ts.UTC(), true
		}
		if f, err := parseFloatString(v); err == nil && f > 0 {
			return time.UnixMilli(int64(f * 1000)).UTC(), true
		}
	}
	return time.Time{}, false
}

func krakenNormalizeSpotPair(pair string) string {
	upper := strings.ToUpper(pair)
	switch upper {
	case "XBT/USD":
		return "BTC/USD"
	case "XBT/USDT":
		return "BTC/USDT"
	case "XBT/USDC":
		return "BTC/USDC"
	default:
		return upper
	}
}
