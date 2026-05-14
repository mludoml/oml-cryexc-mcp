package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"oml-aggr-mcp/internal/api"
	"oml-aggr-mcp/internal/config"
	"oml-aggr-mcp/internal/exchange"
	"oml-aggr-mcp/internal/hub"
	"oml-aggr-mcp/internal/mcp"
	"oml-aggr-mcp/internal/metrics"
	"oml-aggr-mcp/internal/orderbook"
	"oml-aggr-mcp/internal/store"
	"oml-aggr-mcp/internal/ws"
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
		dbURL = "postgres://aggr:aggr_secret@localhost:5432/aggr?sslmode=disable"
	}

	s, err := store.New(dbURL)
	if err != nil {
		slog.Error("failed to create store", "err", err)
		os.Exit(1)
	}
	defer s.Close()

	metricsRegistry := metrics.NewRegistry(60 * time.Second)
	wsHub := ws.NewHub(metricsRegistry)
	obRegistry := orderbook.NewRegistry(1*time.Second, 20)
	h := hub.New(s, nil, metricsRegistry, wsHub, obRegistry)
	marketsByExchange := config.MarketsByExchange()

	register := func(name string, conn exchange.Connector, exchangeID config.ExchangeID) {
		markets := marketsByExchange[exchangeID]
		if len(markets) == 0 {
			return
		}
		if err := conn.Connect(markets); err != nil {
			slog.Error("connector connect", "name", name, "err", err)
			return
		}
		h.AddConnector(conn)
	}

	registerByType := func(name string, factory func() exchange.Connector, exchangeID config.ExchangeID, marketType config.MarketType) {
		all := marketsByExchange[exchangeID]
		filtered := make([]config.MarketConfig, 0, len(all))
		for _, market := range all {
			if market.Type == marketType {
				filtered = append(filtered, market)
			}
		}
		if len(filtered) == 0 {
			return
		}
		conn := factory()
		if err := conn.Connect(filtered); err != nil {
			slog.Error("connector connect", "name", name, "type", marketType, "err", err)
			return
		}
		h.AddConnector(conn)
	}

	registerByType("binance", func() exchange.Connector { return exchange.NewBinanceConnector() }, config.ExchangeBinance, config.MarketTypeSpot)
	register("binance-btcusdt-perp", exchange.NewBinanceBtcusdtPerpConnector(), config.ExchangeBinanceBtcusdtPerp)
	register("binance-btcusd-inverse", exchange.NewBinanceBtcusdInverseConnector(), config.ExchangeBinanceBtcusdInverse)
	registerByType("bybit", func() exchange.Connector { return exchange.NewBybitConnector() }, config.ExchangeBybit, config.MarketTypeSpot)
	registerByType("bybit", func() exchange.Connector { return exchange.NewBybitConnector() }, config.ExchangeBybit, config.MarketTypePerp)
	register("bitstamp", exchange.NewBitstampConnector(), config.ExchangeBitstamp)
	register("bitmex", exchange.NewBitmexConnector(), config.ExchangeBitmex)
	register("kraken", exchange.NewKrakenConnector(), config.ExchangeKraken)
	register("deribit", exchange.NewDeribitConnector(), config.ExchangeDeribit)
	register("dydx", exchange.NewDydxConnector(), config.ExchangeDydx)
	registerByType("okx", func() exchange.Connector { return exchange.NewOKXConnector() }, config.ExchangeOKX, config.MarketTypeSpot)
	registerByType("okx", func() exchange.Connector { return exchange.NewOKXConnector() }, config.ExchangeOKX, config.MarketTypePerp)
	registerByType("coinbase", func() exchange.Connector { return exchange.NewCoinbaseConnector() }, config.ExchangeCoinbase, config.MarketTypeSpot)
	registerByType("coinbase", func() exchange.Connector { return exchange.NewCoinbaseConnector() }, config.ExchangeCoinbase, config.MarketTypePerp)
	register("hyperliquid", exchange.NewHyperliquidConnector(), config.ExchangeHyperliquid)
	registerByType("bitget", func() exchange.Connector { return exchange.NewBitgetConnector() }, config.ExchangeBitget, config.MarketTypeSpot)
	registerByType("bitget", func() exchange.Connector { return exchange.NewBitgetConnector() }, config.ExchangeBitget, config.MarketTypePerp)
	register("bitfinex", exchange.NewBitfinexConnector(), config.ExchangeBitfinex)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wsHub.Start(ctx)
	if err := h.Start(ctx); err != nil {
		slog.Error("failed to start hub", "err", err)
		os.Exit(1)
	}

	restServer := api.NewServer(h, s, wsHub, obRegistry)
	go func() {
		if err := restServer.Start(":3000"); err != nil {
			slog.Error("rest server error", "err", err)
		}
	}()

	mcpProtoServer := mcp.NewMCPServer(s)
	go func() {
		if err := mcpProtoServer.Start(":8081"); err != nil {
			slog.Error("mcp server error", "err", err)
		}
	}()

	slog.Info("oml-aggr-mcp started", "rest", "http://localhost:3000", "mcp", "http://localhost:8081/mcp/sse")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("shutting down...")
	h.Stop()
	wsHub.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := restServer.Stop(shutdownCtx); err != nil {
		slog.Error("rest server shutdown error", "err", err)
	}
	if err := mcpProtoServer.Stop(shutdownCtx); err != nil {
		slog.Error("mcp server shutdown error", "err", err)
	}

	slog.Info("shutdown complete")
}
