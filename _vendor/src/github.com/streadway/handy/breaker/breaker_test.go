package breaker

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type code int

func (h code) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(int(h))
}
func TestCircuitStaysClosedWithSingleError(t *testing.T) {
	h := DefaultBreaker(code(500))

	resp := httptest.NewRecorder()
	req := &http.Request{
		Method: "GET",
	}

	h.ServeHTTP(resp, req)

	if !h.breaker.Allow() {
		t.Fatal("expected breaker to be closed after one requests")
	}
}

func TestCircuitOpenWith5PercentError(t *testing.T) {
	lastResponse := 200
	code := 200
	backend := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(code)
	})

	h := DefaultBreaker(backend)

	for i := 1; i <= 100; i++ {
		resp := httptest.NewRecorder()
		req := &http.Request{
			Method: "GET",
		}

		if i >= 95 {
			code = 500
		}

		h.ServeHTTP(resp, req)
		lastResponse = resp.Code
	}

	if lastResponse != 503 {
		t.Fatalf("expected breaker to be open with 503 after 5%% error rate, got last response: %d", lastResponse)
	}
}
