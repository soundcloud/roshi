// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the README file.
// Source code and contact info at http://github.com/streadway/handy

package statsd_test

import (
	"github.com/streadway/handy/statsd"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

func ExampleCodes() {
	var remote io.Writer

	remote, err := net.Dial("udp", "127.0.0.1:8126")
	if err != nil {
		// log error and continue
		remote = ioutil.Discard
	}

	http.ListenAndServe(":8080",
		statsd.Codes(remote, "doc.status", 10*time.Second,
			http.FileServer(http.Dir("/usr/share/doc"))))
}
