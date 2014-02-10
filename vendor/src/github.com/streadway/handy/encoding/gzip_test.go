// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the README file.
// Source code and contact info at http://github.com/streadway/handy

package encoding

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type body string

func (h body) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(h))
}

func TestGzip(t *testing.T) {
	const msg = "the meaning of life, the universe and everything"

	h := Gzip(body(msg))

	resp := httptest.NewRecorder()
	req := &http.Request{
		Header: map[string][]string{"Accept-Encoding": {"gzip"}},
	}

	h.ServeHTTP(resp, req)

	if hdr := resp.HeaderMap.Get("Content-Encoding"); hdr != "gzip" {
		t.Fatalf("expected content encoding to be gzip, got: %q", hdr)
	}

	if hdr := resp.HeaderMap.Get("Vary"); hdr != "Accept-Encoding" {
		t.Fatalf("expected to vary on accept encoding, got: %q", hdr)
	}

	plain := &bytes.Buffer{}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("expected a gzip stream, got: %q", err)
	}

	io.Copy(plain, gz)

	if plain.String() != msg {
		t.Fatalf("expected to decompress message, got: %q", plain.String())
	}
}
