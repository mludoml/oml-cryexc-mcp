package store

import (
	"context"
	"fmt"
	"time"
)

type TradeRow struct {
	Time          time.Time `json:"time"`
	Exchange      string    `json:"exchange"`
	Pair          string    `json:"pair"`
	MarketType    string    `json:"market_type"`
	Price         float64   `json:"price"`
	Size          float64   `json:"size"`
	Side          string    `json:"side"`
	Liquidation   bool      `json:"liquidation"`
}

type LiquidationRow struct {
	Time       time.Time `json:"time"`
	Exchange   string    `json:"exchange"`
	Symbol     string    `json:"symbol"`
	MarketType string    `json:"market_type"`
	Side       string    `json:"side"`
	Price      float64   `json:"price"`
	Qty        float64   `json:"qty"`
	QuoteQty   float64   `json:"quote_qty"`
}

type MarketStatRow struct {
	Time              time.Time `json:"time"`
	Exchange          string    `json:"exchange"`
	Symbol            string    `json:"symbol"`
	MarketType        string    `json:"market_type"`
	MarkPrice         float64   `json:"mark_price"`
	IndexPrice        float64   `json:"index_price"`
	FundingRate       float64   `json:"funding_rate"`
	NextFundingTime   time.Time `json:"next_funding_time"`
	OpenInterest      float64   `json:"open_interest"`
	LongShortRatio    float64   `json:"long_short_ratio"`
	LongAccountRatio  float64   `json:"long_account_ratio"`
	ShortAccountRatio float64   `json:"short_account_ratio"`
}

type OrderbookRow struct {
	Time       time.Time `json:"time"`
	Exchange   string    `json:"exchange"`
	Symbol     string    `json:"symbol"`
	MarketType string    `json:"market_type"`
	TickSize   float64   `json:"tick_size"`
	Price      float64   `json:"price"`
	BidQty     float64   `json:"bid_qty"`
	AskQty     float64   `json:"ask_qty"`
}

func (s *Store) GetTrades(ctx context.Context, symbol, exchange, marketType string, since time.Time, limit int) ([]TradeRow, error) {
	var args []interface{}
	query := `
		SELECT time, exchange, pair, market_type, price, size, side, liquidation
		FROM trades
		WHERE pair = $1 AND time >= $2
	`
	args = append(args, symbol, since)
	argCount := 2
	
	if exchange != "" {
		argCount++
		query += fmt.Sprintf(" AND exchange = $%d", argCount)
		args = append(args, exchange)
	}
	if marketType != "" {
		argCount++
		query += fmt.Sprintf(" AND market_type = $%d", argCount)
		args = append(args, marketType)
	}
	
	query += fmt.Sprintf(" ORDER BY time DESC LIMIT $%d", argCount+1)
	args = append(args, limit)
	
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var trades []TradeRow
	for rows.Next() {
		var t TradeRow
		err := rows.Scan(
			&t.Time, &t.Exchange, &t.Pair, &t.MarketType,
			&t.Price, &t.Size, &t.Side, &t.Liquidation,
		)
		if err != nil {
			continue
		}
		trades = append(trades, t)
	}
	return trades, rows.Err()
}

func (s *Store) GetLiquidations(ctx context.Context, symbol, exchange, marketType string, since time.Time, limit int) ([]LiquidationRow, error) {
	var args []interface{}
	query := `
		SELECT time, exchange, symbol, market_type, side, price, qty, quote_qty
		FROM liquidations
		WHERE symbol = $1 AND time >= $2
	`
	args = append(args, symbol, since)
	argCount := 2
	
	if exchange != "" {
		argCount++
		query += fmt.Sprintf(" AND exchange = $%d", argCount)
		args = append(args, exchange)
	}
	if marketType != "" {
		argCount++
		query += fmt.Sprintf(" AND market_type = $%d", argCount)
		args = append(args, marketType)
	}
	
	query += fmt.Sprintf(" ORDER BY time DESC LIMIT $%d", argCount+1)
	args = append(args, limit)
	
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var liqs []LiquidationRow
	for rows.Next() {
		var l LiquidationRow
		err := rows.Scan(
			&l.Time, &l.Exchange, &l.Symbol, &l.MarketType,
			&l.Side, &l.Price, &l.Qty, &l.QuoteQty,
		)
		if err != nil {
			continue
		}
		liqs = append(liqs, l)
	}
	return liqs, rows.Err()
}

func (s *Store) GetLatestMarketStats(ctx context.Context, symbol, exchange string) ([]MarketStatRow, error) {
	var args []interface{}
	query := `
		SELECT DISTINCT ON (exchange) time, exchange, symbol, market_type, mark_price, index_price, funding_rate, next_funding_time, open_interest, long_short_ratio, long_account_ratio, short_account_ratio
		FROM market_stats
		WHERE symbol = $1
	`
	args = append(args, symbol)
	argCount := 1
	
	if exchange != "" {
		argCount++
		query += fmt.Sprintf(" AND exchange = $%d", argCount)
		args = append(args, exchange)
	}
	
	query += " ORDER BY exchange, time DESC"
	
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var stats []MarketStatRow
	for rows.Next() {
		var ms MarketStatRow
		err := rows.Scan(
			&ms.Time, &ms.Exchange, &ms.Symbol, &ms.MarketType,
			&ms.MarkPrice, &ms.IndexPrice, &ms.FundingRate, &ms.NextFundingTime,
			&ms.OpenInterest, &ms.LongShortRatio, &ms.LongAccountRatio, &ms.ShortAccountRatio,
		)
		if err != nil {
			continue
		}
		stats = append(stats, ms)
	}
	return stats, rows.Err()
}

func (s *Store) GetCVD(ctx context.Context, symbol, exchange, marketType, interval string, since time.Time) ([]map[string]interface{}, error) {
	var intervalMs int64
	switch interval {
	case "1m":
		intervalMs = 60 * 1000
	case "5m":
		intervalMs = 5 * 60 * 1000
	case "15m":
		intervalMs = 15 * 60 * 1000
	case "1h":
		intervalMs = 60 * 60 * 1000
	default:
		intervalMs = 60 * 1000
	}
	
	var args []interface{}
	query := `
		SELECT 
			time_bucket($1::interval, time) AS bucket,
			exchange,
		SUM(CASE WHEN side = 'buy' THEN size ELSE 0 END) AS buy_vol,
		SUM(CASE WHEN side = 'sell' THEN size ELSE 0 END) AS sell_vol,
		SUM(CASE WHEN side = 'buy' THEN size ELSE -size END) AS delta
		FROM trades
		WHERE pair = $2 AND time >= $3
	`
	
	intervalDuration := time.Duration(intervalMs) * time.Millisecond
	args = append(args, intervalDuration, symbol, since)
	argCount := 3
	
	if exchange != "" {
		argCount++
		query += fmt.Sprintf(" AND exchange = $%d", argCount)
		args = append(args, exchange)
	}
	if marketType != "" {
		argCount++
		query += fmt.Sprintf(" AND market_type = $%d", argCount)
		args = append(args, marketType)
	}
	
	query += fmt.Sprintf(`
		GROUP BY bucket, exchange
		ORDER BY bucket DESC
		LIMIT $%d
	`, argCount+1)
	args = append(args, 500)
	
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var result []map[string]interface{}
	var cumDelta float64
	for rows.Next() {
		var bucket time.Time
		var ex string
		var buyVol, sellVol, delta float64
		err := rows.Scan(&bucket, &ex, &buyVol, &sellVol, &delta)
		if err != nil {
			continue
		}
		cumDelta += delta
		result = append(result, map[string]interface{}{
			"time":      bucket,
			"exchange":  ex,
			"buy_vol":   buyVol,
			"sell_vol":  sellVol,
			"delta":     delta,
			"cum_delta": cumDelta,
		})
	}
	
	return result, rows.Err()
}

func (s *Store) GetLatestOrderbook(ctx context.Context, symbol, exchange, marketType string) ([]OrderbookRow, error) {
	var args []interface{}
	query := `
		SELECT time, exchange, symbol, market_type, tick_size, price, bid_qty, ask_qty
		FROM orderbook_snapshots
		WHERE symbol = $1 AND exchange = $2
	`
	args = append(args, symbol, exchange)
	argCount := 2
	
	if marketType != "" {
		argCount++
		query += fmt.Sprintf(" AND market_type = $%d", argCount)
		args = append(args, marketType)
	}
	
	query += fmt.Sprintf(`
		AND time = (
			SELECT MAX(time) FROM orderbook_snapshots
			WHERE symbol = $1 AND exchange = $2
		)
		ORDER BY price DESC
	`)
	
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var ob []OrderbookRow
	for rows.Next() {
		var o OrderbookRow
		err := rows.Scan(
			&o.Time, &o.Exchange, &o.Symbol, &o.MarketType,
			&o.TickSize, &o.Price, &o.BidQty, &o.AskQty,
		)
		if err != nil {
			continue
		}
		ob = append(ob, o)
	}
	return ob, rows.Err()
}
