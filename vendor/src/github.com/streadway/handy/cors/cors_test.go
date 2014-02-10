// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the README file.
// Source code and contact info at http://github.com/streadway/handy

package cors

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type code int

func (h code) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(int(h))
}

func TestGetPreflightGet(t *testing.T) {
	h := Get("*", code(404))

	resp := httptest.NewRecorder()
	req := &http.Request{}

	req.Method = "OPTIONS"
	req.Header = map[string][]string{
		"Access-Control-Request-Method": {"GET"},
		"Origin":                        {"localhost"},
	}

	h.ServeHTTP(resp, req)

	// OPTIONS should always return 200 if the request is ok.
	if res := resp.Code; res != 200 {
		t.Fatalf("expected 200 for OPTIONS, got: %d", res)
	}

	if hdr := resp.HeaderMap.Get("Access-Control-Allow-Origin"); hdr != "*" {
		t.Fatalf("expected Access-Control-Origin for OPTIONS, got: %s", hdr)
	}

	for _, hdr := range []string{"Origin", "Accept", "Content-Type"} {
		if hdrs := resp.HeaderMap.Get("Access-Control-Allow-Headers"); !strings.Contains(hdrs, hdr) {
			t.Fatalf("expected Access-Control-Allow-Headers to include Origin, got: %s", hdrs)
		}
	}
}

func TestGet(t *testing.T) {
	h := Get("*", code(404))

	resp := httptest.NewRecorder()
	req := &http.Request{}

	req.Method = "GET"
	req.Header = map[string][]string{
		"Origin": {"localhost"},
	}

	h.ServeHTTP(resp, req)

	if res := resp.Code; res != 404 {
		t.Fatalf("expected 404 for GET, got: %d", res)
	}

	if hdr := resp.HeaderMap.Get("Access-Control-Allow-Origin"); hdr != "*" {
		t.Fatalf("expected Access-Control-Origin for OPTIONS, got: %s", hdr)
	}

	for _, hdr := range []string{"Origin", "Accept", "Content-Type"} {
		if hdrs := resp.HeaderMap.Get("Access-Control-Allow-Headers"); !strings.Contains(hdrs, hdr) {
			t.Fatalf("expected Access-Control-Allow-Headers to include Origin, got: %s", hdrs)
		}
	}
}

func TestGetWithPOST(t *testing.T) {
	h := Get("*", code(404))

	resp := httptest.NewRecorder()
	req := &http.Request{}

	req.Method = "POST"
	req.Header = map[string][]string{
		"Origin": {"localhost"},
	}

	h.ServeHTTP(resp, req)

	if res := resp.Code; res != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for GET, got: %d", res)
	}
}
