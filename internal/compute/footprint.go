package compute

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type FootprintCandle struct {
	Time     time.Time         `json:"time"`
	Exchange string            `json:"exchange"`
	Symbol   string            `json:"symbol"`
	Levels   []FootprintLevel  `json:"levels"`
}

type FootprintLevel struct {
	Price    float64 `json:"price"`
	BuyVol   float64 `json:"buy_vol"`
	SellVol  float64 `json:"sell_vol"`
	Delta    float64 `json:"delta"`
	TotalVol float64 `json:"total_vol"`
}

func ComputeFootprint(ctx context.Context, pool *pgxpool.Pool, symbol, exchange, marketType string, tickSize float64, interval time.Duration, since time.Time) ([]FootprintCandle, error) {
	query := `
		SELECT 
			time_bucket($1::interval, time) AS bucket,
			ROUND(price / $2) * $2 AS price_level,
			SUM(CASE WHEN side = 'buy' THEN qty ELSE 0 END) AS buy_vol,
			SUM(CASE WHEN side = 'sell' THEN qty ELSE 0 END) AS sell_vol
		FROM trades
		WHERE symbol = $3 AND time >= $4
	`
	args := []interface{}{interval, tickSize, symbol, since}
	argCount := 4

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
		GROUP BY bucket, price_level
		ORDER BY bucket DESC, price_level ASC
		LIMIT $%d
	`, argCount+1)
	args = append(args, 5000)

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	candleMap := make(map[time.Time]*FootprintCandle)
	for rows.Next() {
		var bucket time.Time
		var price float64
		var buyVol, sellVol float64
		if err := rows.Scan(&bucket, &price, &buyVol, &sellVol); err != nil {
			continue
		}
		candle, ok := candleMap[bucket]
		if !ok {
			candle = &FootprintCandle{
				Time:     bucket,
				Exchange: exchange,
				Symbol:   symbol,
			}
			candleMap[bucket] = candle
		}
		candle.Levels = append(candle.Levels, FootprintLevel{
			Price:    price,
			BuyVol:   buyVol,
			SellVol:  sellVol,
			Delta:    buyVol - sellVol,
			TotalVol: buyVol + sellVol,
		})
	}

	var candles []FootprintCandle
	for _, c := range candleMap {
		candles = append(candles, *c)
	}
	return candles, rows.Err()
}
