// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the README file.
// Source code and contact info at http://github.com/streadway/handy

package report

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestJSON(t *testing.T) {
	res := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "http://example.com/foo", nil)
	out := &bytes.Buffer{}

	h := JSON(out, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	h.ServeHTTP(res, req)

	report := map[string]interface{}{}
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("expected to decode json report, got: %q", err)
	}

	t.Log(report)

	for field, want := range map[string]interface{}{
		"status": float64(200),
		"method": "GET",
		"proto":  "HTTP/1.1",
		"time":   "",
	} {
		if want == "" {
			if _, ok := report[field]; !ok {
				t.Fatalf("expected to report %q with any value, did not", field)
			}
		} else {
			if got := report[field]; got != want {
				t.Fatalf("expected to report %q with %v, got %v", field, want, got)
			}
		}
	}
}
