package breaker

import (
	"testing"
	"time"
)

func failrate(b Breaker, count int, pct float64) {
	chance := int(1 / pct)
	if chance <= 0 {
		chance = 1
	}

	for i := 0; i < count; i++ {
		if b.Allow() {
			if (i%count)%chance == 0 {
				b.Failure(0)
			} else {
				b.Success(0)
			}
		}
	}
}

func TestSimulateConcurrentBreakerHandlerWithPartialFailures(t *testing.T) {
	const requestsPerSecond = 100
	const seconds = 5

	now := time.Now()
	after := make(chan time.Time)

	b := newBreaker(breakerConfig{
		Window:          seconds * time.Second,
		MinObservations: requestsPerSecond / seconds,
		FailureRatio:    0.05,
		Now:             func() time.Time { return now },
		After:           func(time.Duration) <-chan time.Time { return after },
	})

	for i := 0; i < seconds; i++ {
		failrate(b, requestsPerSecond, 0.20)
		now = now.Add(time.Second)
	}

	if got, want := b.Allow(), false; got != want {
		t.Fatalf("expected to trip at a high failure rate")
	}

	after <- now

	if got, want := b.Allow(), true; got != want {
		t.Fatalf("expected to allow in half-open state after cooldown")
	}

	b.Success(time.Duration(0))

	if got, want := b.Allow(), true; got != want {
		t.Fatalf("expected to close after success from half-open")
	}

	for i := 0; i < seconds; i++ {
		failrate(b, requestsPerSecond, 0.02)
		now = now.Add(time.Second)
	}

	if got, want := b.Allow(), true; got != want {
		t.Fatalf("expected to stay closed after lower error rate")
	}

	for i := 0; i < seconds; i++ {
		failrate(b, requestsPerSecond, 0.06)
		now = now.Add(time.Second)
	}

	if got, want := b.Allow(), false; got != want {
		t.Fatalf("expected to open after high error rate again")
	}
}
