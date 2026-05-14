package buffer

import "sync"

type RingBuffer[T any] struct {
	mu       sync.RWMutex
	buf      []T
	head     int
	size     int
	capacity int
}

func New[T any](capacity int) *RingBuffer[T] {
	if capacity <= 0 {
		panic("ring buffer capacity must be positive")
	}

	return &RingBuffer[T]{
		buf:      make([]T, capacity),
		capacity: capacity,
	}
}

func (r *RingBuffer[T]) Push(item T) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.buf[r.head] = item
	r.head = (r.head + 1) % r.capacity
	if r.size < r.capacity {
		r.size++
	}
}

func (r *RingBuffer[T]) Recent(n int) []T {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.recentLocked(n)
}

func (r *RingBuffer[T]) Range(from, to int) []T {
	r.mu.RLock()
	defer r.mu.RUnlock()

	items := r.snapshotNewestFirstLocked()
	if from < 0 {
		from = 0
	}
	if to < from {
		to = from
	}
	if from > len(items) {
		from = len(items)
	}
	if to > len(items) {
		to = len(items)
	}

	return append([]T(nil), items[from:to]...)
}

func (r *RingBuffer[T]) Filter(predicate func(T) bool) []T {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]T, 0, r.size)
	for i := 0; i < r.size; i++ {
		item := r.buf[(r.head-1-i+r.capacity)%r.capacity]
		if predicate(item) {
			result = append(result, item)
		}
	}

	return result
}

func (r *RingBuffer[T]) RecentFiltered(n int, predicate func(T) bool) []T {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if n <= 0 || r.size == 0 {
		return nil
	}

	target := min(n, r.size)
	result := make([]T, 0, target)
	for i := 0; i < r.size && len(result) < target; i++ {
		item := r.buf[(r.head-1-i+r.capacity)%r.capacity]
		if predicate(item) {
			result = append(result, item)
		}
	}

	return result
}

func (r *RingBuffer[T]) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.size
}

func (r *RingBuffer[T]) recentLocked(n int) []T {
	if n <= 0 || r.size == 0 {
		return nil
	}

	count := min(n, r.size)
	result := make([]T, count)
	for i := 0; i < count; i++ {
		result[i] = r.buf[(r.head-1-i+r.capacity)%r.capacity]
	}

	return result
}

func (r *RingBuffer[T]) snapshotNewestFirstLocked() []T {
	items := make([]T, r.size)
	for i := 0; i < r.size; i++ {
		items[i] = r.buf[(r.head-1-i+r.capacity)%r.capacity]
	}
	return items
}
