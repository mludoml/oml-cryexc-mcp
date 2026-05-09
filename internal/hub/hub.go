package hub

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"cryexec-mcp/internal/exchange"
	"cryexec-mcp/internal/store"
)

type Hub struct {
	connectors []exchange.Connector
	store      *store.Store
	symbols    []string
	
	tradeBuf       []exchange.Trade
	tradeBufMu     sync.Mutex
	liquidationBuf []exchange.Liquidation
	liqBufMu       sync.Mutex
	
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(s *store.Store, symbols []string) *Hub {
	return &Hub{
		store:   s,
		symbols: symbols,
	}
}

func (h *Hub) AddConnector(c exchange.Connector) {
	h.connectors = append(h.connectors, c)
}

func (h *Hub) Start(ctx context.Context) error {
	h.ctx, h.cancel = context.WithCancel(ctx)
	
	for _, c := range h.connectors {
		// Set callbacks
		c.OnTrade(h.handleTrade)
		c.OnOrderbookSnapshot(h.handleOrderbookSnapshot)
		c.OnLiquidation(h.handleLiquidation)
		c.OnMarketStat(h.handleMarketStat)
		
		// Start connector
		h.wg.Add(1)
		go func(conn exchange.Connector) {
			defer h.wg.Done()
			if err := conn.Run(h.ctx); err != nil {
				slog.Error("connector error", "name", conn.Name(), "err", err)
			}
		}(c)
	}
	
	// Start snapshot emitter
	h.wg.Add(1)
	go h.snapshotLoop()
	
	// Start batch flush
	h.wg.Add(1)
	go h.flushLoop()
	
	return nil
}

func (h *Hub) Stop() {
	h.cancel()
	h.wg.Wait()
}

func (h *Hub) handleTrade(t exchange.Trade) {
	h.tradeBufMu.Lock()
	h.tradeBuf = append(h.tradeBuf, t)
	h.tradeBufMu.Unlock()
}

func (h *Hub) handleOrderbookSnapshot(ob exchange.OrderbookSnapshot) {
	if err := h.store.InsertOrderbookSnapshot(h.ctx, ob); err != nil {
		slog.Warn("insert ob snapshot error", "err", err)
	}
}

func (h *Hub) handleLiquidation(l exchange.Liquidation) {
	h.liqBufMu.Lock()
	h.liquidationBuf = append(h.liquidationBuf, l)
	h.liqBufMu.Unlock()
}

func (h *Hub) handleMarketStat(ms exchange.MarketStat) {
	if err := h.store.InsertMarketStat(h.ctx, ms); err != nil {
		slog.Warn("insert market stat error", "err", err)
	}
}

func (h *Hub) snapshotLoop() {
	defer h.wg.Done()
	
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			for _, c := range h.connectors {
				// Try to emit snapshot if connector supports it
				if bc, ok := c.(*exchange.BinanceConnector); ok {
					bc.EmitOrderbookSnapshot(0.01) // 1 cent tick for BTC
				}
			}
		}
	}
}

func (h *Hub) flushLoop() {
	defer h.wg.Done()
	
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-h.ctx.Done():
			// Final flush
			h.flushAll()
			return
		case <-ticker.C:
			h.flushAll()
		}
	}
}

func (h *Hub) flushAll() {
	// Flush trades
	h.tradeBufMu.Lock()
	trades := h.tradeBuf
	h.tradeBuf = nil
	h.tradeBufMu.Unlock()
	
	if len(trades) > 0 {
		if err := h.store.InsertTradesBatch(h.ctx, trades); err != nil {
			slog.Error("flush trades error", "err", err)
		}
	}
	
	// Flush liquidations
	h.liqBufMu.Lock()
	liqs := h.liquidationBuf
	h.liquidationBuf = nil
	h.liqBufMu.Unlock()
	
	for _, l := range liqs {
		if err := h.store.InsertLiquidation(h.ctx, l); err != nil {
			slog.Warn("flush liquidation error", "err", err)
		}
	}
}
