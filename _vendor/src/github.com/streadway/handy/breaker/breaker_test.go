package breaker

import (
	"testing"
	"time"
)

func TestNewBreakerAllows(t *testing.T) {
	c := NewBreaker(0)

	if !c.Allow() {
		t.Fatal("expected new breaker to be closed")
	}
}

func TestBreakerSuccessClosesOpenBreaker(t *testing.T) {
	b := NewBreaker(0)

	b.trip()

	if b.Allow() {
		t.Fatal("expected new breaker to be open after being tripped")
	}

	b.Success(0)

	if !b.Allow() {
		t.Fatal("expected new breaker to be closed after a success")
	}
}

func TestBreakerFailTripsBreakerWithASingleFailureAt0PrecentThreshold(t *testing.T) {
	b := NewBreaker(0)

	for i := 0; i < 100; i++ {
		b.Success(0)
	}

	b.Failure(0)

	if b.Allow() {
		t.Fatalf("expected failure to not trip circuit at 0%% threshold")
	}
}

func TestBreakerFailDoesNotTripBreakerAt1PrecentThreshold(t *testing.T) {
	const threshold = 0.01

	c := NewBreaker(threshold)

	for i := 0; i < 100-100*threshold; i++ {
		c.Success(0)
	}

	for i := 0; i < 100*threshold; i++ {
		c.Failure(0)
	}

	if !c.Allow() {
		t.Fatalf("expected failure to not trip circuit at 1%% threshold")
	}

	c.Failure(0)

	if c.Allow() {
		t.Fatal("expected failure to trip over the threshold")
	}
}

func TestBreakerAllowsASingleRequestAfterNapTime(t *testing.T) {
	after := make(chan time.Time)

	c := newBreaker(breakerConfig{
		Window: 5 * time.Second,
		After:  func(time.Duration) <-chan time.Time { return after },
	})

	c.trip()

	after <- time.Now()

	if !c.Allow() {
		t.Fatal("expected to allow once after nap time")
	}

	if c.Allow() {
		t.Fatal("expected to only allow once after nap time")
	}
}

func TestBreakerClosesAfterSuccessAfterNapTime(t *testing.T) {
	after := make(chan time.Time)

	b := newBreaker(breakerConfig{
		Window: 5 * time.Second,
		After:  func(time.Duration) <-chan time.Time { return after },
	})

	b.trip()

	after <- time.Now()

	if !b.Allow() {
		t.Fatal("expected to allow once after nap time")
	}

	b.Success(0)

	if !b.Allow() {
		t.Fatal("expected to close after first success")
	}

	if !b.Allow() {
		t.Fatal("expected to stay closed after first success")
	}
}

func TestBreakerReschedulesOnFailureInHalfOpen(t *testing.T) {
	afters := make(chan chan time.Time)

	b := newBreaker(breakerConfig{
		Window: 5 * time.Second,
		After: func(time.Duration) <-chan time.Time {
			after := make(chan time.Time)
			afters <- after
			return after
		},
	})

	b.trip()

	(<-afters) <- time.Now()

	b.Failure(0)

	select {
	case after := <-afters:
		after <- time.Now()
	case <-time.After(time.Millisecond):
		t.Fatal("expected to reschedule after failure in half-open, did not")
	}

	b.Success(0)

	if !b.Allow() {
		t.Fatal("expected to close after failure in half-open")
	}
}
