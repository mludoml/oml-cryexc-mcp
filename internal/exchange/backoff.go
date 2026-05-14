package exchange

import "time"

const (
	initialBackoff = 1 * time.Second
	maxBackoff     = 30 * time.Second
)

type Backoff struct {
	current time.Duration
}

func (b *Backoff) Next() time.Duration {
	if b.current == 0 {
		b.current = initialBackoff
		return b.current
	}
	b.current *= 2
	if b.current > maxBackoff {
		b.current = maxBackoff
	}
	return b.current
}

func (b *Backoff) Reset() {
	b.current = 0
}
