// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the README file.
// Source code and contact info at http://github.com/streadway/handy

package report

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func get(h http.Handler, url string) {
	res := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", url, nil)
	h.ServeHTTP(res, req)
}

type sleeper time.Duration

func (h sleeper) ServeHTTP(http.ResponseWriter, *http.Request) {
	time.Sleep(time.Duration(h))
}

func TestMultipleJSONLogLines(t *testing.T) {
	const worktime = 10 * time.Millisecond
	const requests = 10

	var (
		r, w    = io.Pipe()
		handler = JSON(w, sleeper(worktime))
		logger  = json.NewDecoder(r)
	)

	for i := 0; i < requests; i++ {
		go get(handler, "http://example.org/foo")
	}

	for i := 0; i < requests; i++ {
		report := map[string]interface{}{}
		if err := logger.Decode(&report); err != nil {
			t.Fatalf("expected to decode json report, got: %q", err)
		}

		t.Log(report)

		for field, want := range map[string]interface{}{
			"status": float64(200),
			"method": "GET",
			"proto":  "HTTP/1.1",
			"time":   "",
			"size":   "",
			"ms":     "",
		} {
			if _, ok := report[field]; !ok {
				t.Fatalf("expected to report %q with any value, did not", field)
			}

			if want != "" {
				if got := report[field]; got != want {
					t.Fatalf("expected to report %q with %v, got %v", field, want, got)
				}
			}
		}

		if ms, ok := report["ms"].(float64); !ok {
			t.Fatalf("ms is not a number")
		} else {
			if want, got, delta := worktime, time.Duration(ms)*time.Millisecond, time.Millisecond; want+delta < got || want-delta > got {
				t.Fatalf("duration falls outside of %s±%s, got: %d", want, delta, got)
			}
		}
	}
}

func TestJSONShouldHaveMs(t *testing.T) {
	var (
		r, w    = io.Pipe()
		logger  = json.NewDecoder(r)
		handler = JSON(w, sleeper(0))
	)

	go get(handler, "http://example.org/foo")

	report := map[string]interface{}{}
	if err := logger.Decode(&report); err != nil {
		t.Fatalf("expected to decode json report, got: %q", err)
	}

	if _, haveMs := report["ms"]; !haveMs {
		t.Fatalf("expected report to include ms, got: %v", report)
	}
}
