package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"oml-cryexc-mcp/internal/exchange"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))

	fmt.Printf("=== Validate Bybit Perp Fix — %s ===\n", time.Now().Format("15:04:05"))

	tests := []struct {
		name   string
		ex     exchange.Connector
		symbol string
		mtype  string
	}{
		{"BYBIT spot (check)", exchange.NewBybitConnector(), "BTCUSDT", "spot"},
		{"BYBIT perp (FIXED)", exchange.NewBybitConnector(), "BTCUSDT", "perp"},
	}

	for _, tt := range tests {
		validate(tt.name, tt.ex, tt.symbol, tt.mtype)
	}
	fmt.Println("\n=== Done ===")
}

func validate(name string, ex exchange.Connector, symbol, mtype string) {
	var count atomic.Int32

	ex.OnTrade(func(t exchange.Trade) {
		count.Add(1)
	})

	if err := ex.Connect(symbol, mtype); err != nil {
		fmt.Printf("❌ %-25s connect err: %v\n", name, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- ex.Run(ctx) }()

	runErr := <-done
	cancel()
	ex.Disconnect()

	cnt := int(count.Load())
	if cnt > 0 {
		fmt.Printf("✅ %-25s trades=%d\n", name, cnt)
	} else if runErr == nil {
		fmt.Printf("⚠️ %-25s trades=0 (no errors)\n", name)
	} else {
		fmt.Printf("❌ %-25s trades=0 (ERR: %v)\n", name, runErr)
	}
}
