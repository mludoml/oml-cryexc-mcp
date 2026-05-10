package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"oml-cryexc-mcp/internal/exchange"
	"oml-cryexc-mcp/internal/hub"
	"oml-cryexc-mcp/internal/mcp"
	"oml-cryexc-mcp/internal/store"
)

func main() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	level := slog.LevelInfo
	if logLevel == "debug" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		dbURL = "postgres://cryexec:cryexec_secret@localhost:5432/cryexec?sslmode=disable"
	}

	symbolsStr := os.Getenv("SYMBOLS")
	if symbolsStr == "" {
		symbolsStr = "BTCUSDT"
	}
	symbols := strings.Split(symbolsStr, ",")

	s, err := store.New(dbURL)
	if err != nil {
		slog.Error("failed to create store", "err", err)
		os.Exit(1)
	}
	defer s.Close()

	h := hub.New(s, symbols)

	for _, sym := range symbols {
		sym = strings.TrimSpace(sym)

		binanceSpot := exchange.NewBinanceConnector()
		if err := binanceSpot.Connect(sym, "spot"); err != nil {
			slog.Error("binance spot connect", "err", err)
		} else {
			h.AddConnector(binanceSpot)
		}

		binancePerp := exchange.NewBinanceConnector()
		if err := binancePerp.Connect(sym, "perp"); err != nil {
			slog.Error("binance perp connect", "err", err)
		} else {
			h.AddConnector(binancePerp)
		}

		bybitSpot := exchange.NewBybitConnector()
		if err := bybitSpot.Connect(sym, "spot"); err != nil {
			slog.Error("bybit spot connect", "err", err)
		} else {
			h.AddConnector(bybitSpot)
		}

		bybitPerp := exchange.NewBybitConnector()
		if err := bybitPerp.Connect(sym, "perp"); err != nil {
			slog.Error("bybit perp connect", "err", err)
		} else {
			h.AddConnector(bybitPerp)
		}

		okxSpot := exchange.NewOKXConnector()
		if err := okxSpot.Connect(sym, "spot"); err != nil {
			slog.Error("okx spot connect", "err", err)
		} else {
			h.AddConnector(okxSpot)
		}

		okxPerp := exchange.NewOKXConnector()
		if err := okxPerp.Connect(sym, "perp"); err != nil {
			slog.Error("okx perp connect", "err", err)
		} else {
			h.AddConnector(okxPerp)
		}

		coinbase := exchange.NewCoinbaseConnector()
		if err := coinbase.Connect(sym, "spot"); err != nil {
			slog.Error("coinbase connect", "err", err)
		} else {
			h.AddConnector(coinbase)
		}

		hyperliquid := exchange.NewHyperliquidConnector()
		if err := hyperliquid.Connect(sym, "perp"); err != nil {
			slog.Error("hyperliquid connect", "err", err)
		} else {
			h.AddConnector(hyperliquid)
		}

		bitgetSpot := exchange.NewBitgetConnector()
		if err := bitgetSpot.Connect(sym, "spot"); err != nil {
			slog.Error("bitget spot connect", "err", err)
		} else {
			h.AddConnector(bitgetSpot)
		}

		bitgetPerp := exchange.NewBitgetConnector()
		if err := bitgetPerp.Connect(sym, "perp"); err != nil {
			slog.Error("bitget perp connect", "err", err)
		} else {
			h.AddConnector(bitgetPerp)
		}

		bitfinexSpot := exchange.NewBitfinexConnector()
		if err := bitfinexSpot.Connect(sym, "spot"); err != nil {
			slog.Error("bitfinex spot connect", "err", err)
		} else {
			h.AddConnector(bitfinexSpot)
		}

		bitfinexPerp := exchange.NewBitfinexConnector()
		if err := bitfinexPerp.Connect(sym, "perp"); err != nil {
			slog.Error("bitfinex perp connect", "err", err)
		} else {
			h.AddConnector(bitfinexPerp)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	if err := h.Start(ctx); err != nil {
		slog.Error("failed to start hub", "err", err)
		os.Exit(1)
	}

	mcpServer := mcp.NewServer(s)
	go func() {
		if err := mcpServer.Start(":8080"); err != nil {
			slog.Error("rest server error", "err", err)
		}
	}()

	mcpProtoServer := mcp.NewMCPServer(s)
	go func() {
		if err := mcpProtoServer.Start(":8081"); err != nil {
			slog.Error("mcp server error", "err", err)
		}
	}()

	slog.Info("oml-cryexc-mcp started", "rest", "http://localhost:8080", "mcp", "http://localhost:8081/mcp/sse")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("shutting down...")
	h.Stop()
	
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := mcpServer.Stop(shutdownCtx); err != nil {
		slog.Error("rest shutdown error", "err", err)
	}
	if err := mcpProtoServer.Stop(shutdownCtx); err != nil {
		slog.Error("mcp shutdown error", "err", err)
	}
}
