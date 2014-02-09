package breaker

import (
	"testing"
	"time"
)

func afterTest() {
	after = time.After
	now = time.Now
}

func TestNewCircuitBreakerAllows(t *testing.T) {
	defer afterTest()

	b := NewCircuitBreaker(0)

	if !b.Allow() {
		t.Fatal("expected new breaker to be closed")
	}
}

func TestBreakerSuccessClosesOpenCircuit(t *testing.T) {
	defer afterTest()

	b := NewCircuitBreaker(0)
	b.Trip()
	if b.Allow() {
		t.Fatal("expected new breaker to be open in test")
	}

	b.Success(time.Duration(0))

	if !b.Allow() {
		t.Fatal("expected new breaker to be closed after a success")
	}
}

func TestBreakerFailTripsCircuitWithASingleFailureAt0PrecentThreshold(t *testing.T) {
	defer afterTest()

	b := NewCircuitBreaker(0)

	for i := 0; i < 100; i++ {
		b.Success(0)
	}
	b.Failure(0)

	if b.Allow() {
		t.Fatalf("expected failure to not trip circuit at 0%% threshold")
	}
}

func TestBreakerFailDoesNotTripCircuitAt1PrecentThreshold(t *testing.T) {
	defer afterTest()

	const threshold = 0.01

	b := NewCircuitBreaker(threshold)

	for i := 0; i < 100-100*threshold; i++ {
		b.Success(0)
	}

	for i := 0; i < 100*threshold; i++ {
		b.Failure(0)
	}

	if !b.Allow() {
		t.Fatalf("expected failure to not trip circuit at 1%% threshold")
	}

	b.Failure(0)

	if b.Allow() {
		t.Fatal("expected failure to trip over the threshold")
	}
}

func TestBreakerAllowsASingleRequestAfterNapTime(t *testing.T) {
	defer afterTest()

	timer := make(chan time.Time, 1)
	after = func(time.Duration) <-chan time.Time { return timer }

	b := NewCircuitBreaker(0)
	b.Trip()

	timer <- now()

	for !b.Allow() {
		// wait until we've been allowed once because of non-deterministic select
	}

	if b.Allow() {
		t.Fatal("expected to only allow once after nap time")
	}
}
