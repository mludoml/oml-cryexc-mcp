package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cryexec-mcp/internal/exchange"
	"cryexec-mcp/internal/hub"
	"cryexec-mcp/internal/mcp"
	"cryexec-mcp/internal/store"
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
			continue
		}
		h.AddConnector(binanceSpot)
		
		binancePerp := exchange.NewBinanceConnector()
		if err := binancePerp.Connect(sym, "perp"); err != nil {
			slog.Error("binance perp connect", "err", err)
			continue
		}
		h.AddConnector(binancePerp)
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
			slog.Error("mcp server error", "err", err)
		}
	}()

	slog.Info("cryexec-mcp started", "symbols", symbols, "mcp", "http://localhost:8080")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("shutting down...")
	h.Stop()
	
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := mcpServer.Stop(shutdownCtx); err != nil {
		slog.Error("mcp shutdown error", "err", err)
	}
}
