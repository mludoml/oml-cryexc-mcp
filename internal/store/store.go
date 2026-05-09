package store

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"oml-cryexc-mcp/internal/exchange"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(dbURL string) (*Store, error) {
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) InsertTrade(ctx context.Context, t exchange.Trade) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO trades (time, exchange, symbol, market_type, price, qty, quote_qty, side, is_buyer_maker, is_liquidation, trade_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, t.Timestamp, t.Exchange, t.Symbol, t.MarketType, t.Price, t.Qty, t.QuoteQty, t.Side, t.IsBuyerMaker, t.IsLiquidation, t.TradeID)
	return err
}

func (s *Store) InsertTradesBatch(ctx context.Context, trades []exchange.Trade) error {
	if len(trades) == 0 {
		return nil
	}
	
	batch := &pgx.Batch{}
	for _, t := range trades {
		batch.Queue(`
			INSERT INTO trades (time, exchange, symbol, market_type, price, qty, quote_qty, side, is_buyer_maker, is_liquidation, trade_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`, t.Timestamp, t.Exchange, t.Symbol, t.MarketType, t.Price, t.Qty, t.QuoteQty, t.Side, t.IsBuyerMaker, t.IsLiquidation, t.TradeID)
	}
	
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	
	for i := 0; i < len(trades); i++ {
		if _, err := br.Exec(); err != nil {
			slog.Warn("batch insert trade error", "index", i, "err", err)
		}
	}
	return br.Close()
}

func (s *Store) InsertOrderbookSnapshot(ctx context.Context, ob exchange.OrderbookSnapshot) error {
	batch := &pgx.Batch{}
	for _, lvl := range ob.Levels {
		batch.Queue(`
			INSERT INTO orderbook_snapshots (time, exchange, symbol, market_type, tick_size, price, bid_qty, ask_qty)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, ob.Timestamp, ob.Exchange, ob.Symbol, ob.MarketType, ob.TickSize, lvl.Price, lvl.BidQty, lvl.AskQty)
	}
	
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	
	for i := 0; i < len(ob.Levels); i++ {
		if _, err := br.Exec(); err != nil {
			slog.Warn("batch insert ob error", "index", i, "err", err)
		}
	}
	return br.Close()
}

func (s *Store) InsertLiquidation(ctx context.Context, l exchange.Liquidation) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO liquidations (time, exchange, symbol, market_type, side, price, qty, quote_qty)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, l.Timestamp, l.Exchange, l.Symbol, l.MarketType, l.Side, l.Price, l.Qty, l.QuoteQty)
	return err
}

func (s *Store) InsertMarketStat(ctx context.Context, ms exchange.MarketStat) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO market_stats (time, exchange, symbol, market_type, mark_price, index_price, funding_rate, next_funding_time, open_interest, long_short_ratio, long_account_ratio, short_account_ratio)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, ms.Timestamp, ms.Exchange, ms.Symbol, ms.MarketType, ms.MarkPrice, ms.IndexPrice, ms.FundingRate, ms.NextFundingTime, ms.OpenInterest, ms.LongShortRatio, ms.LongAccountRatio, ms.ShortAccountRatio)
	return err
}
