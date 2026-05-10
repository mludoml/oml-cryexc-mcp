package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"oml-cryexc-mcp/internal/exchange"
)

type testResult struct {
	exchange   string
	marketType string
	trades     int
	errors    []string
	duration  time.Duration
}

func testConnector(name string, ctor func() exchange.Connector, symbol, marketType string, timeout time.Duration) testResult {
	result := testResult{exchange: name, marketType: marketType}
	start := time.Now()

	conn := ctor()
	if err := conn.Connect(symbol, marketType); err != nil {
		result.errors = append(result.errors, fmt.Sprintf("connect: %v", err))
		return result
	}

	tradeCount := 0
	var mu sync.Mutex
	conn.OnTrade(func(t exchange.Trade) {
		mu.Lock()
		tradeCount++
		mu.Unlock()
	})

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := conn.Run(ctx); err != nil {
		result.errors = append(result.errors, fmt.Sprintf("run: %v", err))
	}

	mu.Lock()
	result.trades = tradeCount
	mu.Unlock()
	result.duration = time.Since(start)
	return result
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	timeout := 15 * time.Second
	if t := os.Getenv("TEST_TIMEOUT"); t != "" {
		d, _ := time.ParseDuration(t)
		if d > 0 {
			timeout = d
		}
	}

	tests := []struct {
		name       string
		ctor       func() exchange.Connector
		symbol     string
		marketType string
	}{
		{"Binance", func() exchange.Connector { return exchange.NewBinanceConnector() }, "BTCUSDT", "spot"},
		{"Binance", func() exchange.Connector { return exchange.NewBinanceConnector() }, "BTCUSDT", "perp"},
		{"Bybit", func() exchange.Connector { return exchange.NewBybitConnector() }, "BTCUSDT", "spot"},
		{"Bybit", func() exchange.Connector { return exchange.NewBybitConnector() }, "BTCUSDT", "perp"},
		{"OKX", func() exchange.Connector { return exchange.NewOKXConnector() }, "BTCUSDT", "spot"},
		{"OKX", func() exchange.Connector { return exchange.NewOKXConnector() }, "BTCUSDT", "perp"},
		{"Coinbase", func() exchange.Connector { return exchange.NewCoinbaseConnector() }, "BTCUSDT", "spot"},
		{"Hyperliquid", func() exchange.Connector { return exchange.NewHyperliquidConnector() }, "BTCUSDT", "perp"},
		{"Bitget", func() exchange.Connector { return exchange.NewBitgetConnector() }, "BTCUSDT", "spot"},
		{"Bitget", func() exchange.Connector { return exchange.NewBitgetConnector() }, "BTCUSDT", "perp"},
		{"Bitfinex", func() exchange.Connector { return exchange.NewBitfinexConnector() }, "BTCUSDT", "spot"},
		{"Bitfinex", func() exchange.Connector { return exchange.NewBitfinexConnector() }, "BTCUSDT", "perp"},
	}

	fmt.Println("=== Test runner: oml-cryexc-mcp ===")
	fmt.Printf("Timeout: %v\n\n", timeout)

	var results []testResult
	for _, tt := range tests {
		fmt.Printf("Testing %s %s... ", tt.name, tt.marketType)
		result := testConnector(tt.name, tt.ctor, tt.symbol, tt.marketType, timeout)
		results = append(results, result)

		status := "✅"
		if result.trades == 0 {
			status = "⚠️"
		}
		if len(result.errors) > 0 {
			status = "❌"
		}
		fmt.Printf("%s trades=%d errors=%d\n", status, result.trades, len(result.errors))
	}

	fmt.Println("\n=== Summary ===")
	ok := 0
	warn := 0
	fail := 0
	for _, r := range results {
		if len(r.errors) > 0 {
			fail++
			fmt.Printf("❌ %s %s: %v\n", r.exchange, r.marketType, r.errors)
		} else if r.trades > 0 {
			ok++
			fmt.Printf("✅ %s %s: %d trades in %v\n", r.exchange, r.marketType, r.trades, r.duration)
		} else {
			warn++
			fmt.Printf("⚠️  %s %s: 0 trades (silent market or geo-block)\n", r.exchange, r.marketType)
		}
	}
	fmt.Printf("\nTotal: %d OK, %d silent, %d failed\n", ok, warn, fail)
}
