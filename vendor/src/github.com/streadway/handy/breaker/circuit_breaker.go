package breaker

import (
	"time"
)

var (
	after = time.After
	now   = time.Now
)

type states int

const (
	reset states = iota
	tripped

	closed
	open
	halfopen
)

// CircuitBreaker implements a circuit breaker state machine.
type CircuitBreaker struct {
	force     chan states
	allow     chan bool
	success   chan time.Duration
	failure   chan time.Duration
	threshold float64
}

// NewCircuitBreaker constructs a new circuit breaker, initially closed.
func NewCircuitBreaker(failureThreshold float64) CircuitBreaker {
	if failureThreshold < 0.0 || failureThreshold > 1.0 {
		panic("failureThreshold must be between 0.0 and 1.0")
	}

	b := CircuitBreaker{
		force:     make(chan states),
		allow:     make(chan bool),
		success:   make(chan time.Duration),
		failure:   make(chan time.Duration),
		threshold: failureThreshold,
	}
	go b.run()
	return b
}

func shouldOpen(m *metric, threshold float64) bool {
	s := m.Summary()
	return s.total > 5 && s.rate > threshold
}

/*
sed -n 's/^dot//p' | dot -Tsvg -ostates.svg
dot digraph {
dot  reset -> closed    [label="stats and time reset"]
dot  closed -> tripped	[label="failed and failure rate exceeded"]
dot  closed -> closed		[label="succeed and update stats"]
dot  closed -> closed		[label="failed and update stats"]
dot
dot  tripped -> open		[label="timeout scheduled"]
dot  open -> reset      [label="succeed"]
dot  open -> halfopen   [label="timeout expired"]
dot  halfopen -> open   [label="failed"]
dot  halfopen -> open   [label="allowed one"]
dot }
*/
func (b CircuitBreaker) run() {
	var (
		state   states
		timeout <-chan time.Time
		metrics metric
	)

	for {
		//println(state, len(timeout))
		switch state {
		case reset:
			metrics = metric{}
			timeout = nil
			state = closed

		case closed:
			select {
			case b.allow <- true:
			case d := <-b.success:
				metrics.Success(d)
			case d := <-b.failure:
				metrics.Failure(d)
				if shouldOpen(&metrics, b.threshold) {
					state = tripped
				}
			case state = <-b.force:
			}

		case tripped:
			timeout = after(time.Second)
			state = open

		case open:
			select {
			case b.allow <- false:
			case <-b.success:
				state = reset
			case <-b.failure:
			case <-timeout:
				state = halfopen
			case state = <-b.force:
			}

		case halfopen:
			select {
			case b.allow <- true:
				state = tripped
			case <-b.success:
				state = reset
			case <-b.failure:
				state = open
			case state = <-b.force:
			}

		}
	}
}

// Allow returns true if a new request should be allowed to proceed to the
// underlying resource.
func (b CircuitBreaker) Allow() bool {
	return <-b.allow
}

// Trip manually opens the circuit.
func (b CircuitBreaker) Trip() {
	b.force <- tripped
}

// Reset manually closes the circuit.
func (b CircuitBreaker) Reset() {
	b.force <- reset
}

// Success informs the circuit that a request to the underlying resource has
// completed successfully. Every Allowed request should signal either Success
// or Failure.
func (b CircuitBreaker) Success(d time.Duration) {
	b.success <- d
}

// Failure informs the circuit that a request to the underlying resource has
// failed. Every Allowed request should signal either Success or Failure.
func (b CircuitBreaker) Failure(d time.Duration) {
	b.failure <- d
}
