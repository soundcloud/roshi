// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the README file.
// Source code and contact info at http://github.com/streadway/handy

package statsd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type codeHandler int

func (h codeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(int(h))
}

func TestInstrumentCodes(t *testing.T) {
	out := new(bytes.Buffer)

	handler200 := Codes(out, "test", time.Millisecond, codeHandler(200))
	handler404 := Codes(out, "test", time.Millisecond, codeHandler(404))

	resp := httptest.NewRecorder()
	req := &http.Request{}

	handler200.ServeHTTP(resp, req)
	handler404.ServeHTTP(resp, req)

	time.Sleep(2 * time.Millisecond)

	got := string(out.Bytes())

	if !strings.Contains(got, "test.200:1|c") {
		t.Fatalf("expected statsd formatted count for 200, got: %q", got)
	}

	if !strings.Contains(got, "test.404:1|c") {
		t.Fatalf("expected statsd formatted count for 404, got: %q", got)
	}
}

func TestInstrumentDurations(t *testing.T) {
	out := new(bytes.Buffer)

	handler := Durations(out, "test", time.Millisecond, codeHandler(200))

	resp := httptest.NewRecorder()
	req := &http.Request{}

	handler.ServeHTTP(resp, req)
	handler.ServeHTTP(resp, req)

	time.Sleep(2 * time.Millisecond)

	res := string(out.Bytes())

	if res != "test:0|ms:0|ms" {
		t.Fatalf("expected key and two measurements at 0ms, got %q", res)
	}
}

type writeCounter struct {
	count int
}

func (w *writeCounter) Write(b []byte) (int, error) {
	w.count++
	return len(b), nil
}

func TestInstrumentDurationFlushesAtPacketSize(t *testing.T) {
	out := &writeCounter{}

	handler := Durations(out, "test", time.Minute, codeHandler(200))

	resp := httptest.NewRecorder()
	req := &http.Request{}

	for i := 0; i < maxPacketLen; i++ {
		handler.ServeHTTP(resp, req)
	}

	if out.count < 4 {
		t.Fatalf("expected early flush based on packet size, got %d writes", out.count)
	}
}
