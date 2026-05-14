package hub

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"oml-aggr-mcp/internal/buffer"
	"oml-aggr-mcp/internal/exchange"
	"oml-aggr-mcp/internal/monitoring"
	"oml-aggr-mcp/internal/store"
)

const defaultTradeRingCapacity = 500_000

type Hub struct {
	connectors []exchange.Connector
	store      *store.Store
	symbols    []string
	tradeRing  *buffer.RingBuffer[exchange.Trade]
	
	tradeBuf       []exchange.Trade
	tradeBufMu     sync.Mutex
	liquidationBuf []exchange.Liquidation
	liqBufMu       sync.Mutex
	
	monitoring *monitoring.Service
	
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(s *store.Store, symbols []string) *Hub {
	return &Hub{
		store:   s,
		symbols: symbols,
		tradeRing: buffer.New[exchange.Trade](defaultTradeRingCapacity),
	}
}

func (h *Hub) AddConnector(c exchange.Connector) {
	h.connectors = append(h.connectors, c)
}

func (h *Hub) Start(ctx context.Context) error {
	h.ctx, h.cancel = context.WithCancel(ctx)
	
	for _, c := range h.connectors {
		c.OnTrade(h.handleTrade)
		c.OnOrderbookSnapshot(h.handleOrderbookSnapshot)
		c.OnLiquidation(h.handleLiquidation)
		c.OnMarketStat(h.handleMarketStat)
		
		h.wg.Add(1)
		go func(conn exchange.Connector) {
			defer h.wg.Done()
			if err := conn.Run(h.ctx); err != nil {
				slog.Error("connector error", "name", conn.Name(), "err", err)
			}
		}(c)
	}
	
	// Start monitoring heartbeat
	h.monitoring = monitoring.NewService(h.store, h.connectors)
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.monitoring.Start(h.ctx)
	}()
	
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
	h.tradeRing.Push(t)
	h.tradeBufMu.Lock()
	h.tradeBuf = append(h.tradeBuf, t)
	h.tradeBufMu.Unlock()
}

func (h *Hub) RecentTrades(limit int) []exchange.Trade {
	return h.tradeRing.Recent(limit)
}

func (h *Hub) FilterTrades(predicate func(exchange.Trade) bool) []exchange.Trade {
	return h.tradeRing.Filter(predicate)
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
				switch conn := c.(type) {
				case interface{ EmitOrderbookSnapshot(float64) }:
					conn.EmitOrderbookSnapshot(0.01)
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
			h.requeueTrades(trades)
		}
	}
	
	// Flush liquidations
	h.liqBufMu.Lock()
	liqs := h.liquidationBuf
	h.liquidationBuf = nil
	h.liqBufMu.Unlock()
	
	for i, l := range liqs {
		if err := h.store.InsertLiquidation(h.ctx, l); err != nil {
			slog.Warn("flush liquidation error", "err", err)
			h.requeueLiquidations(liqs[i:])
			break
		}
	}
}

func (h *Hub) requeueTrades(trades []exchange.Trade) {
	if len(trades) == 0 {
		return
	}

	h.tradeBufMu.Lock()
	defer h.tradeBufMu.Unlock()

	requeued := make([]exchange.Trade, 0, len(trades)+len(h.tradeBuf))
	requeued = append(requeued, trades...)
	requeued = append(requeued, h.tradeBuf...)
	h.tradeBuf = requeued
}

func (h *Hub) requeueLiquidations(liqs []exchange.Liquidation) {
	if len(liqs) == 0 {
		return
	}

	h.liqBufMu.Lock()
	defer h.liqBufMu.Unlock()

	requeued := make([]exchange.Liquidation, 0, len(liqs)+len(h.liquidationBuf))
	requeued = append(requeued, liqs...)
	requeued = append(requeued, h.liquidationBuf...)
	h.liquidationBuf = requeued
}
