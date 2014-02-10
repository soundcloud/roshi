package rewrite

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

type capture struct {
	r http.Request
}

func (h *capture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.r = *r
}

func TestMethodExpectations(t *testing.T) {
	for _, test := range []struct {
		original, overridden, expected string
	}{
		{"GET", "DELETE", "GET"},
		{"GET", "POST", "GET"},
		{"POST", "DELETE", "DELETE"},
		{"POST", "PUT", "PUT"},
		{"POST", "POST", "POST"},
		{"PUT", "DELETE", "PUT"},
		{"DELETE", "PUT", "DELETE"},
	} {
		h := &capture{}

		Method(h).ServeHTTP(httptest.NewRecorder(), &http.Request{
			Method: test.original,
			Form: url.Values{
				"_method": {test.overridden}},
		})

		if h.r.Method != test.expected {
			t.Fatalf("expected http method %q with override %q but got %q",
				test.original, test.expected, h.r.Method)
		}
	}
}
