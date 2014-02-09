/*
Package breaker implements an outer circuit breaker for upstream handlers, returning 503 when open.
*/
package breaker

import (
	"net/http"
)

type codeWriter struct {
	http.ResponseWriter
	code int
}

func (w *codeWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

type breakerHandler struct {
	breaker CircuitBreaker
	next    http.Handler
}

func (h *breakerHandler) serveClosed(w http.ResponseWriter, r *http.Request) {
	cw := &codeWriter{w, 200}
	begin := now()

	h.next.ServeHTTP(cw, r)

	duration := now().Sub(begin)
	if cw.code < 500 {
		h.breaker.Success(duration)
	} else {
		h.breaker.Failure(duration)
	}
}

func (h *breakerHandler) serveOpened(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusServiceUnavailable)
}

func (h *breakerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.breaker.Allow() {
		h.serveClosed(w, r)
	} else {
		h.serveOpened(w, r)
	}
}

// DefaultBreaker is an experimental implementation of a CircuitBreaker that
// returns 503 with an empty body after 5% failure rate over a sliding window
// of 5 seconds with a 1 second cooldown period before retrying with a single
// request.
func DefaultBreaker(next http.Handler) *breakerHandler {
	return &breakerHandler{
		breaker: NewCircuitBreaker(0.05),
		next:    next,
	}
}
