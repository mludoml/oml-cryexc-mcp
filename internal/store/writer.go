package store

import "time"

const (
	TradeBatchFlushInterval = 500 * time.Millisecond
	TradeBatchFlushSize     = 1000
)
