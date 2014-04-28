package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

type count int

func (h *count) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	(*h)++
	w.WriteHeader(200)
}

func TestProxy(t *testing.T) {
	var c1 count
	s1 := httptest.NewServer(&c1)
	defer s1.Close()

	var c2 count
	s2 := httptest.NewServer(&c2)
	defer s2.Close()

	proxyClient := http.Client{
		Transport: Transport{
			Proxy: func(*http.Request) (*url.URL, error) {
				return url.Parse(s2.URL)
			},
		},
	}

	resp, err := proxyClient.Get(s1.URL)

	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if expected, got := 0, int(c1); expected != got {
		t.Errorf("first server: expected request count %d, got %d", expected, got)
	}
	if expected, got := 1, int(c2); expected != got {
		t.Errorf("second server: expected request count %d, got %d", expected, got)
	}
}

func TestNilValues(t *testing.T) {
	var c count
	s := httptest.NewServer(&c)
	defer s.Close()

	nilClient := http.Client{
		Transport: Transport{},
	}

	resp, err := nilClient.Get(s.URL)

	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if expected, got := 1, int(c); expected != got {
		t.Errorf("expected request count %d, got %d", expected, got)
	}
}
