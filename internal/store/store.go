package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"oml-aggr-mcp/internal/exchange"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(dbURL string) (*Store, error) {
	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.ParseConfig: %w", err)
	}
	config.MaxConns = 20
	config.MinConns = 5
	config.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.NewWithConfig: %w", err)
	}

	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *Store) InsertTrade(ctx context.Context, t exchange.Trade) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO trades (time, exchange, pair, market_type, price, size, side, liquidation)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, t.Timestamp, t.Exchange, t.Symbol, t.MarketType, t.Price, t.QuoteQty, t.Side, t.IsLiquidation)
	return err
}

func (s *Store) InsertTradesBatch(ctx context.Context, trades []exchange.Trade) error {
	if len(trades) == 0 {
		return nil
	}
	_, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"trades"},
		[]string{"time", "exchange", "pair", "market_type", "price", "size", "side", "liquidation"},
		copyFromSlice(len(trades), func(i int) ([]any, error) {
			trade := trades[i]
			return []any{trade.Timestamp, trade.Exchange, trade.Symbol, trade.MarketType, trade.Price, trade.QuoteQty, trade.Side, trade.IsLiquidation}, nil
		}),
	)
	return err
}

func (s *Store) InsertOrderbookSnapshot(ctx context.Context, ob exchange.OrderbookSnapshot) error {
	if len(ob.Levels) == 0 {
		return nil
	}

	levelsTop, err := json.Marshal(ob.Levels)
	if err != nil {
		return fmt.Errorf("marshal orderbook levels: %w", err)
	}

	bestBid, bestAsk, bidDepth, askDepth := summarizeOrderbook(ob.Levels)
	spread := 0.0
	midPrice := 0.0
	imbalance := 0.0
	if bestBid > 0 && bestAsk > 0 {
		spread = bestAsk - bestBid
		midPrice = (bestAsk + bestBid) / 2
	}
	if askDepth > 0 {
		imbalance = bidDepth / askDepth
	}

	_, err = s.pool.CopyFrom(ctx,
		pgx.Identifier{"orderbook_snapshots"},
		[]string{"time", "exchange", "pair", "best_bid", "best_ask", "spread", "mid_price", "imbalance", "bid_depth", "ask_depth", "levels_top", "symbol", "market_type", "tick_size", "price", "bid_qty", "ask_qty"},
		copyFromSlice(len(ob.Levels), func(i int) ([]any, error) {
			level := ob.Levels[i]
			return []any{ob.Timestamp, ob.Exchange, ob.Symbol, bestBid, bestAsk, spread, midPrice, imbalance, bidDepth, askDepth, levelsTop, ob.Symbol, ob.MarketType, ob.TickSize, level.Price, level.BidQty, level.AskQty}, nil
		}),
	)
	return err
}

func (s *Store) InsertLiquidation(ctx context.Context, l exchange.Liquidation) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO liquidations (time, exchange, symbol, market_type, side, price, qty, quote_qty)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, l.Timestamp, l.Exchange, l.Symbol, l.MarketType, l.Side, l.Price, l.Qty, l.QuoteQty)
	return err
}

func copyFromSlice(length int, fn func(i int) ([]any, error)) pgx.CopyFromSource {
	return pgx.CopyFromSlice(length, fn)
}

func summarizeOrderbook(levels []exchange.OrderbookLevel) (bestBid, bestAsk, bidDepth, askDepth float64) {
	for _, level := range levels {
		if level.BidQty > 0 {
			if level.Price > bestBid {
				bestBid = level.Price
			}
			bidDepth += level.BidQty
		}
		if level.AskQty > 0 {
			if bestAsk == 0 || level.Price < bestAsk {
				bestAsk = level.Price
			}
			askDepth += level.AskQty
		}
	}
	return bestBid, bestAsk, bidDepth, askDepth
}

func (s *Store) InsertMarketStat(ctx context.Context, ms exchange.MarketStat) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO market_stats (time, exchange, symbol, market_type, mark_price, index_price, funding_rate, next_funding_time, open_interest, long_short_ratio, long_account_ratio, short_account_ratio)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, ms.Timestamp, ms.Exchange, ms.Symbol, ms.MarketType, ms.MarkPrice, ms.IndexPrice, ms.FundingRate, ms.NextFundingTime, ms.OpenInterest, ms.LongShortRatio, ms.LongAccountRatio, ms.ShortAccountRatio)
	return err
}

// --- Metrics ---

func (s *Store) GetMetrics(ctx context.Context, since time.Time) (*Metrics, error) {
	m := &Metrics{ExchangeLag: make(map[string]time.Duration)}

	// total trades in last minute
	_ = s.pool.QueryRow(ctx, `SELECT COALESCE(COUNT(*),0) FROM trades WHERE time > $1`, since).Scan(&m.TradesLastMin)

	// total liquidations in last minute
	_ = s.pool.QueryRow(ctx, `SELECT COALESCE(COUNT(*),0) FROM liquidations WHERE time > $1`, since).Scan(&m.LiquidationsLastMin)

	// lag per exchange (max lag of latest trade)
	rows, _ := s.pool.Query(ctx, `
		SELECT exchange, EXTRACT(EPOCH FROM (NOW() - MAX(time)))::float8
		FROM trades WHERE time > NOW() - INTERVAL '5 minutes'
		GROUP BY exchange
	`)
	defer rows.Close()
	for rows.Next() {
		var ex string
		var lag float64
		rows.Scan(&ex, &lag)
		m.ExchangeLag[ex] = time.Duration(lag) * time.Second
	}

	// DB size
	_ = s.pool.QueryRow(ctx, `
		SELECT pg_size_pretty(pg_total_relation_size('trades'))
	`).Scan(&m.DBSize)

	return m, nil
}

type Metrics struct {
	TradesLastMin     int64                      `json:"trades_last_minute"`
	LiquidationsLastMin int64                    `json:"liquidations_last_minute"`
	ExchangeLag       map[string]time.Duration `json:"exchange_lag_seconds"`
	DBSize            string                     `json:"db_size"`
}
