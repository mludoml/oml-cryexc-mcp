package compute

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DOMLevel struct {
	Price      float64         `json:"price"`
	BidQty     float64         `json:"bid_qty"`
	AskQty     float64         `json:"ask_qty"`
	Trades     []DOMTrade      `json:"trades"`
}

type DOMTrade struct {
	Time   time.Time `json:"time"`
	Qty    float64   `json:"qty"`
	Side   string    `json:"side"`
	IsLiq  bool      `json:"is_liquidation"`
}

type DOMSnapshot struct {
	Time       time.Time  `json:"time"`
	Exchange   string     `json:"exchange"`
	Symbol     string     `json:"symbol"`
	Levels     []DOMLevel `json:"levels"`
	Spread     float64    `json:"spread"`
	MidPrice   float64    `json:"mid_price"`
}

func ComputeDOM(ctx context.Context, pool *pgxpool.Pool, symbol, exchange, marketType string, tickSize float64, since time.Time) (*DOMSnapshot, error) {
	obQuery := `
		SELECT time, price, bid_qty, ask_qty
		FROM orderbook_snapshots
		WHERE symbol = $1 AND exchange = $2 AND time >= $3
	`
	args := []interface{}{symbol, exchange, since}
	argCount := 3
	if marketType != "" {
		argCount++
		obQuery += fmt.Sprintf(" AND market_type = $%d", argCount)
		args = append(args, marketType)
	}
	obQuery += " ORDER BY time DESC LIMIT 1"

	var obTime time.Time
	var price, bidQty, askQty float64
	err := pool.QueryRow(ctx, obQuery, args...).Scan(&obTime, &price, &bidQty, &askQty)
	if err != nil {
		return nil, err
	}

	domSince := obTime.Add(-5 * time.Minute)
	tradeQuery := `
		SELECT time, price, qty, side, is_liquidation
		FROM trades
		WHERE symbol = $1 AND exchange = $2 AND time >= $3 AND time <= $4
	`
	tArgs := []interface{}{symbol, exchange, domSince, obTime}
	tCount := 4
	if marketType != "" {
		tCount++
		tradeQuery += fmt.Sprintf(" AND market_type = $%d", tCount)
		tArgs = append(tArgs, marketType)
	}
	tradeQuery += " ORDER BY time ASC"

	rows, err := pool.Query(ctx, tradeQuery, tArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tradeMap := make(map[float64][]DOMTrade)
	for rows.Next() {
		var t time.Time
		var p, q float64
		var side string
		var isLiq bool
		if err := rows.Scan(&t, &p, &q, &side, &isLiq); err != nil {
			continue
		}
		bucket := math.Round(p/tickSize) * tickSize
		tradeMap[bucket] = append(tradeMap[bucket], DOMTrade{
			Time:  t,
			Qty:   q,
			Side:  side,
			IsLiq: isLiq,
		})
	}

	levels := []DOMLevel{{
		Price:  math.Round(price/tickSize) * tickSize,
		BidQty: bidQty,
		AskQty: askQty,
		Trades: tradeMap[math.Round(price/tickSize)*tickSize],
	}}

	return &DOMSnapshot{
		Time:     obTime,
		Exchange: exchange,
		Symbol:   symbol,
		Levels:   levels,
		Spread:   0,
		MidPrice: price,
	}, rows.Err()
}

type HeatmapRow struct {
	Time   time.Time `json:"time"`
	Price  float64   `json:"price"`
	BidQty float64   `json:"bid_qty"`
	AskQty float64   `json:"ask_qty"`
}

func ComputeHeatmap(ctx context.Context, pool *pgxpool.Pool, symbol, exchange, marketType string, tickSize float64, since time.Time) ([]HeatmapRow, error) {
	query := `
		SELECT time, price, bid_qty, ask_qty
		FROM orderbook_snapshots
		WHERE symbol = $1 AND exchange = $2 AND time >= $3
	`
	args := []interface{}{symbol, exchange, since}
	argCount := 3
	if marketType != "" {
		argCount++
		query += fmt.Sprintf(" AND market_type = $%d", argCount)
		args = append(args, marketType)
	}
	query += " ORDER BY time DESC, price DESC LIMIT $4"
	args = append(args, 5000)

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []HeatmapRow
	for rows.Next() {
		var h HeatmapRow
		if err := rows.Scan(&h.Time, &h.Price, &h.BidQty, &h.AskQty); err != nil {
			continue
		}
		result = append(result, h)
	}
	return result, rows.Err()
}
