package tb

import (
	"math"
	"sync/atomic"
	"time"
)

// Bucket defines a generic lock-free implementation of a Token Bucket.
type Bucket struct {
	inc      int64
	tokens   int64
	capacity int64
	closing  chan struct{}
}

// NewBucket returns a full Bucket with c capacity and starts a filling
// go-routine which ticks every freq. The number of tokens added on each tick
// is computed dynamically to be even across the duration of a second.
//
// If freq == -1 then it will be adjusted to 1/c seconds. Otherwise,
// If freq < 1/c seconds, the filling go-routine won't be started.
func NewBucket(c int64, freq time.Duration) *Bucket {
	b := &Bucket{tokens: c, capacity: c, closing: make(chan struct{})}

	if freq == -1 {
		freq = time.Duration(1e9 / c)
	} else if freq.Seconds() < 1/float64(c) {
		return b
	}

	go b.fill(freq)

	return b
}

// Take attempts to take n tokens out of the bucket.
// If tokens == 0, nothing will be taken.
// If n <= tokens, n tokens will be taken.
// If n > tokens, all tokens will be taken.
//
// This method is thread-safe.
func (b *Bucket) Take(n int64) (taken int64) {
	for {
		if tokens := atomic.LoadInt64(&b.tokens); tokens == 0 {
			return 0
		} else if n <= tokens {
			if !atomic.CompareAndSwapInt64(&b.tokens, tokens, tokens-n) {
				continue
			}
			return n
		} else if atomic.CompareAndSwapInt64(&b.tokens, tokens, 0) { // Spill
			return tokens
		}
	}
}

// Put attempts to add n tokens to the bucket.
// If tokens == capacity, nothing will be added.
// If n <= capacity - tokens, n tokens will be added.
// If n > capacity - tokens, capacity - tokens will be added.
//
// This method is thread-safe.
func (b *Bucket) Put(n int64) (added int64) {
	for {
		if tokens := atomic.LoadInt64(&b.tokens); tokens == b.capacity {
			return 0
		} else if left := b.capacity - tokens; n <= left {
			if !atomic.CompareAndSwapInt64(&b.tokens, tokens, tokens+n) {
				continue
			}
			return n
		} else if atomic.CompareAndSwapInt64(&b.tokens, tokens, b.capacity) {
			return left
		}
	}
}

// Close stops the filling go-routine given it was started.
func (b *Bucket) Close() error {
	close(b.closing)
	return nil
}

func (b *Bucket) fill(freq time.Duration) {
	ticker := time.NewTicker(freq)
	defer ticker.Stop()

	b.inc = int64(math.Floor(.5 + (float64(b.capacity) * freq.Seconds())))

	for _ = range ticker.C {
		select {
		case <-b.closing:
			return
		default:
			b.Put(b.inc)
		}
	}
}
