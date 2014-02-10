package redirect

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

type code int

func (h code) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(int(h))
}

func TestHTTPSServesUsingXForwardedProtoHttps(t *testing.T) {
	h := HTTPS(code(204))

	resp := httptest.NewRecorder()
	req := &http.Request{
		Method: "GET",
		URL:    &url.URL{Scheme: "http", Path: "/"},
		Host:   "localhost",
		Header: map[string][]string{"X-Forwarded-Proto": {"https"}},
	}

	h.ServeHTTP(resp, req)

	if resp.Code != 204 {
		t.Fatalf("expected 204 but got %d", resp.Code)
	}
}

func TestHTTPSServesUsingHttps(t *testing.T) {
	h := HTTPS(code(204))

	resp := httptest.NewRecorder()
	req := &http.Request{
		Method: "GET",
		URL:    &url.URL{Scheme: "https", Path: "/index.html"},
		Host:   "localhost",
		Header: map[string][]string{},
	}

	h.ServeHTTP(resp, req)

	if resp.Code != 204 {
		t.Fatalf("expected 204 but got %d", resp.Code)
	}
}

func TestHTTPSRedirectsUsingHttp(t *testing.T) {
	h := HTTPS(code(204))

	resp := httptest.NewRecorder()
	req := &http.Request{
		Method: "GET",
		URL:    &url.URL{Scheme: "http", Path: "/index.html"},
		Host:   "localhost",
		Header: map[string][]string{},
	}

	h.ServeHTTP(resp, req)

	if resp.Code != 302 {
		t.Fatalf("expected 302 but got %d", resp.Code)
	}
	if got, want := resp.Header().Get("Location"), "https://localhost/index.html"; got != want {
		t.Fatalf("expected %q, got: %q", want, got)
	}
}

func TestHTTPSRedirectsUsingXForwardedProtoHttp(t *testing.T) {
	h := HTTPS(code(204))

	resp := httptest.NewRecorder()
	req := &http.Request{
		Method: "GET",
		URL:    &url.URL{Scheme: "http", Path: "/index.html"},
		Host:   "localhost",
		Header: map[string][]string{"X-Forwarded-Proto": {"http"}},
	}

	h.ServeHTTP(resp, req)

	if resp.Code != 302 {
		t.Fatalf("expected 302 but got %d", resp.Code)
	}
	if got, want := resp.Header().Get("Location"), "https://localhost/index.html"; got != want {
		t.Fatalf("expected %q, got: %q", want, got)
	}
}

func TestHTTPSRedirectsToSameHostWithEmptyParameter(t *testing.T) {
	h := HTTPS(code(204))

	resp := httptest.NewRecorder()
	req := &http.Request{
		Method: "GET",
		URL:    &url.URL{Scheme: "http", Path: "/index.html"},
		Host:   "example.com",
		Header: map[string][]string{},
	}

	h.ServeHTTP(resp, req)

	if resp.Code != 302 {
		t.Fatalf("expected 302 but got %d", resp.Code)
	}
	if got, want := resp.Header().Get("Location"), "https://example.com/index.html"; got != want {
		t.Fatalf("expected %q, got: %q", want, got)
	}
}
