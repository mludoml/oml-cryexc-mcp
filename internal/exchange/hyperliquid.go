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

// HyperliquidConnector — perp only (BTC-PERP)
type HyperliquidConnector struct {
	name       string
	wsURL      string
	symbol     string
	coin       string

	ws      *websocket.Conn
	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	orderbook *hyperliquidOrderbook

	onTrade             func(Trade)
	onOrderbookSnapshot func(OrderbookSnapshot)
	onLiquidation       func(Liquidation)
	onMarketStat        func(MarketStat)
}

type hyperliquidOrderbook struct {
	bids map[string]float64
	asks map[string]float64
	mu   sync.RWMutex
}

func NewHyperliquidConnector() *HyperliquidConnector {
	return &HyperliquidConnector{
		name:      "HYPERLIQUID",
		wsURL:     "wss://api.hyperliquid.xyz/ws",
		orderbook: &hyperliquidOrderbook{bids: make(map[string]float64), asks: make(map[string]float64)},
	}
}

func (h *HyperliquidConnector) Name() string            { return h.name }
func (h *HyperliquidConnector) MarketTypes() []string { return []string{"perp"} }

func (h *HyperliquidConnector) OnTrade(cb func(Trade))                        { h.onTrade = cb }
func (h *HyperliquidConnector) OnOrderbookSnapshot(cb func(OrderbookSnapshot)) { h.onOrderbookSnapshot = cb }
func (h *HyperliquidConnector) OnLiquidation(cb func(Liquidation))            { h.onLiquidation = cb }
func (h *HyperliquidConnector) OnMarketStat(cb func(MarketStat))               { h.onMarketStat = cb }

func (h *HyperliquidConnector) Connect(symbol, marketType string) error {
	h.symbol = strings.ToUpper(symbol)
	if h.symbol == "BTCUSDT" {
		h.coin = "BTC"
	} else {
		h.coin = strings.Replace(h.symbol, "USDT", "", 1)
	}
	return nil
}

func (h *HyperliquidConnector) Disconnect() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.running = false
	if h.cancel != nil {
		h.cancel()
	}
	if h.ws != nil {
		h.ws.Close()
	}
}

func (h *HyperliquidConnector) Run(ctx context.Context) error {
	h.ctx, h.cancel = context.WithCancel(ctx)
	defer h.cancel()
	h.mu.Lock()
	h.running = true
	h.mu.Unlock()
	for {
		select {
		case <-h.ctx.Done():
			return nil
		default:
		}
		if err := h.connectAndStream(); err != nil {
			slog.Error("hyperliquid stream error", "err", err)
			select {
			case <-h.ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}
}

func (h *HyperliquidConnector) connectAndStream() error {
	ws, _, err := websocket.DefaultDialer.Dial(h.wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	h.ws = ws

	sub := map[string]interface{}{
		"method": "subscribe",
		"subscription": map[string]interface{}{
			"type": "allMids",
		},
	}
	if err := ws.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe allMids: %w", err)
	}

	tradesSub := map[string]interface{}{
		"method": "subscribe",
		"subscription": map[string]interface{}{
			"type":   "trades",
			"coin":   h.coin,
		},
	}
	if err := ws.WriteJSON(tradesSub); err != nil {
		return fmt.Errorf("subscribe trades: %w", err)
	}

	bookSub := map[string]interface{}{
		"method": "subscribe",
		"subscription": map[string]interface{}{
			"type": "l2Book",
			"coin": h.coin,
		},
	}
	if err := ws.WriteJSON(bookSub); err != nil {
		return fmt.Errorf("subscribe l2Book: %w", err)
	}

	for {
		select {
		case <-h.ctx.Done():
			return nil
		default:
		}
		ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if err := h.handleMessage(msg); err != nil {
			slog.Warn("hyperliquid handle message", "err", err)
		}
	}
}

func (h *HyperliquidConnector) handleMessage(msg []byte) error {
	var wrapper map[string]interface{}
	if err := json.Unmarshal(msg, &wrapper); err != nil {
		return err
	}
	channel, _ := wrapper["channel"].(string)
	switch channel {
	case "trades":
		return h.handleTrade(wrapper["data"])
	case "l2Book":
		return h.handleL2Book(wrapper["data"])
	case "allMids":
		return h.handleAllMids(wrapper["data"])
	}
	return nil
}

func (h *HyperliquidConnector) handleTrade(data interface{}) error {
	var trades []struct {
		Coin  string `json:"coin"`
		Side  string `json:"side"`
		Px    string `json:"px"`
		Sz    string `json:"sz"`
		Hash  string `json:"hash"`
		Time  int64  `json:"time"`
		Tid   int    `json:"tid"`
	}
	raw, _ := json.Marshal(data)
	if err := json.Unmarshal(raw, &trades); err != nil {
		return err
	}
	for _, t := range trades {
		if t.Coin != h.coin {
			continue
		}
		price, _ := strconv.ParseFloat(t.Px, 64)
		qty, _ := strconv.ParseFloat(t.Sz, 64)
		if price == 0 || qty == 0 {
			continue
		}
		trade := Trade{
			Exchange:   h.name,
			Symbol:     h.symbol,
			MarketType: "perp",
			Price:      price,
			Qty:        qty,
			QuoteQty:   price * qty,
			Side:       strings.ToLower(t.Side),
			Timestamp:  time.Unix(t.Time/1000, 0),
			TradeID:    t.Hash,
		}
		if h.onTrade != nil {
			h.onTrade(trade)
		}
	}
	return nil
}

func (h *HyperliquidConnector) handleL2Book(data interface{}) error {
	var book struct {
		Coin   string     `json:"coin"`
		Levels [][]string `json:"levels"`
	}
	raw, _ := json.Marshal(data)
	if err := json.Unmarshal(raw, &book); err != nil {
		return err
	}
	if book.Coin != h.coin {
		return nil
	}
	h.orderbook.mu.Lock()
	h.orderbook.bids = make(map[string]float64)
	h.orderbook.asks = make(map[string]float64)
	for _, lvl := range book.Levels {
		if len(lvl) < 3 {
			continue
		}
		price := lvl[0]
		qty, _ := strconv.ParseFloat(lvl[1], 64)
		side := strings.ToLower(lvl[2])
		if side == "b" {
			h.orderbook.bids[price] = qty
		} else {
			h.orderbook.asks[price] = qty
		}
	}
	h.orderbook.mu.Unlock()
	return nil
}

func (h *HyperliquidConnector) handleAllMids(data interface{}) error {
	var mids map[string]string
	raw, _ := json.Marshal(data)
	if err := json.Unmarshal(raw, &mids); err != nil {
		return err
	}
	midStr, ok := mids[h.coin]
	if !ok {
		return nil
	}
	mid, _ := strconv.ParseFloat(midStr, 64)
	stat := MarketStat{
		Exchange:   h.name,
		Symbol:     h.symbol,
		MarketType: "perp",
		MarkPrice:  mid,
		Timestamp:  time.Now(),
	}
	if h.onMarketStat != nil {
		h.onMarketStat(stat)
	}
	return nil
}

func (h *HyperliquidConnector) EmitOrderbookSnapshot(tickSize float64) {
	if h.orderbook == nil {
		return
	}
	h.orderbook.mu.RLock()
	allPrices := make(map[string]struct{})
	for p := range h.orderbook.bids {
		allPrices[p] = struct{}{}
	}
	for p := range h.orderbook.asks {
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
			BidQty: h.orderbook.bids[p],
			AskQty: h.orderbook.asks[p],
		})
	}
	h.orderbook.mu.RUnlock()
	if h.onOrderbookSnapshot != nil {
		h.onOrderbookSnapshot(OrderbookSnapshot{
			Exchange:   h.name,
			Symbol:     h.symbol,
			MarketType: "perp",
			TickSize:   tickSize,
			Levels:     levels,
			Timestamp:  time.Now(),
		})
	}
}
