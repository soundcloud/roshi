package breaker

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
)

func TestTransportCircuitStaysClosedWithSingleError(t *testing.T) {
	s := httptest.NewServer(code(500))
	defer s.Close()

	c := http.Client{
		Transport: Transport(NewBreaker(0.05), DefaultResponseValidator, http.DefaultTransport),
	}

	c.Get(s.URL)

	if !c.Transport.(*transport).breaker.Allow() {
		t.Fatal("expected circuit to be closed after one bad request")
	}
}

func TestTransportCircuitOpenWith5PercentError(t *testing.T) {
	pass := httptest.NewServer(code(200))
	defer pass.Close()

	fail := httptest.NewServer(code(500))
	defer fail.Close()

	c := http.Client{
		Transport: Transport(NewBreaker(0.05), DefaultResponseValidator, http.DefaultTransport),
	}

	var lastError error
	for i := 1; i <= 100; i++ {
		url := pass.URL

		if i >= 95 {
			url = fail.URL
		}

		_, lastError = c.Get(url)
	}

	if lastError == nil {
		t.Fatalf("expected %q after 5%% error rate, got no error", ErrCircuitOpen)
	}

	urlError, ok := lastError.(*url.Error)
	if !ok {
		t.Fatalf("expected url.Error after 5%% error rate, got %s", reflect.TypeOf(lastError))
	}

	if err := urlError.Err; err != ErrCircuitOpen {
		t.Fatalf("expected %q after 5%% error rate, got %q", ErrCircuitOpen, err)
	}
}
