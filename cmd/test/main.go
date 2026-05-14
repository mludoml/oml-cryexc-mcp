package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"oml-aggr-mcp/internal/config"
	"oml-aggr-mcp/internal/exchange"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))

	symbols := []string{"BTCUSDT"}
	if len(os.Args) > 1 {
		symbols = strings.Split(os.Args[1], ",")
	}

	fmt.Printf("=== Test runner — %s ===\n", time.Now().Format("15:04:05"))
	fmt.Printf("Symbols: %v\n\n", symbols)

	tests := []struct {
		name       string
		ex         exchange.Connector
		symbol     string
		marketType string
		wantTrades int // min trades to pass
	}{
		{"BINANCE spot", exchange.NewBinanceConnector(), "BTCUSDT", "spot", 50},
		{"BINANCE perp", exchange.NewBinanceBtcusdtPerpConnector(), "BTCUSDT", "perp", 50},
		{"BINANCE inverse", exchange.NewBinanceBtcusdInverseConnector(), "BTCUSD_PERP", "perp", 10},
		{"BYBIT spot", exchange.NewBybitConnector(), "BTCUSDT", "spot", 10},
		{"BYBIT perp", exchange.NewBybitConnector(), "BTCUSDT", "perp", 10},
		{"BITSTAMP spot", exchange.NewBitstampConnector(), "BTCUSD", "spot", 5},
		{"BITMEX perp", exchange.NewBitmexConnector(), "XBTUSD", "perp", 5},
		{"KRAKEN spot", exchange.NewKrakenConnector(), "XBT/USD", "spot", 5},
		{"KRAKEN perp", exchange.NewKrakenConnector(), "PF_XBTUSD", "perp", 5},
		{"DERIBIT perp", exchange.NewDeribitConnector(), "BTC-PERPETUAL", "perp", 5},
		{"DYDX perp", exchange.NewDydxConnector(), "BTC-USD", "perp", 5},
		{"OKX spot", exchange.NewOKXConnector(), "BTCUSDT", "spot", 10},
		{"OKX perp", exchange.NewOKXConnector(), "BTCUSDT", "perp", 10},
		{"COINBASE spot", exchange.NewCoinbaseConnector(), "BTCUSDT", "spot", 50},
		{"HYPERLIQUID perp", exchange.NewHyperliquidConnector(), "BTCUSDT", "perp", 10},
		{"BITGET spot", exchange.NewBitgetConnector(), "BTCUSDT", "spot", 10},
		{"BITGET perp", exchange.NewBitgetConnector(), "BTCUSDT", "perp", 10},
		{"BITFINEX spot", exchange.NewBitfinexConnector(), "BTCUSDT", "spot", 5},
		{"BITFINEX perp", exchange.NewBitfinexConnector(), "BTCUSDT", "perp", 5},
	}

	for _, tt := range tests {
		testExchange(tt.name, tt.ex, tt.symbol, tt.marketType, tt.wantTrades)
	}

	fmt.Println("\n=== Podsumowanie ===")
	fmt.Printf("Czas: %s\n", time.Now().Format("15:04:05"))
	fmt.Printf("Cel: retest w godzinach szczytu (NY Open 15:30 CEST)\n")
	fmt.Printf("Notka: 0 trades na perp może oznaczać cichy rynek; wymaga retestu w dzień\n")
}

func testExchange(name string, ex exchange.Connector, symbol, marketType string, want int) {
	var count atomic.Int32
	var lastPrice float64
	var lastTs time.Time

	ex.OnTrade(func(t exchange.Trade) {
		count.Add(1)
		lastPrice = t.Price
		lastTs = t.Timestamp
	})
	ex.OnOrderbookSnapshot(func(s exchange.OrderbookSnapshot) {
		if count.Load() == 0 {
			return
		}
	})

	if err := ex.Connect([]config.MarketConfig{{Pair: symbol, Type: config.MarketType(marketType)}}); err != nil {
		fmt.Printf("❌ %-20s connect: %v\n", name, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- ex.Run(ctx)
	}()

	<-done
	cancel()
	ex.Disconnect()

	cnt := int(count.Load())
	status := "✅"
	if cnt < want {
		status = "⚠️"
		if cnt == 0 {
			status = "❌"
		}
	}

	fmt.Printf("%s %-20s trades=%3d want=%3d price=%.2f last=%s\n",
		status, name, cnt, want, lastPrice, lastTs.Format("15:04:05.000"))
}
