package farm

import (
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/soundcloud/roshi/instrumentation"
	"github.com/soundcloud/roshi/pool"
)

func TestStripWhitespace(t *testing.T) {
	for input, expected := range map[string]string{
		"":                   "",
		" ":                  "",
		"a":                  "a",
		" a":                 "a",
		" a ":                "a",
		"\ta":                "a",
		"a\n":                "a",
		"a b":                "ab",
		" a b":               "ab",
		"a b ":               "ab",
		" a b ":              "ab",
		" a  b ":             "ab",
		"  a  b ":            "ab",
		"  a  b  ":           "ab",
		"\ta\nb\n\t\r\n":     "ab",
		"\ta b\n\t\r\n":      "ab",
		"\ta \n\n b\n\t\r\n": "ab",
	} {
		if got := stripWhitespace(input); expected != got {
			t.Errorf("%q: expected %q, got %q", input, expected, got)
		}
	}
}

func TestParseFarmString(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	for farmString, expected := range map[string]struct {
		success     bool
		numClusters int
	}{
		"":                                                {false, 0}, // no entries
		";;;":                                             {false, 0}, // no entries
		"foo1:1234":                                       {true, 1},
		"foo1:1234;bar1:1234":                             {true, 2},
		"foo1:1234;;bar1:1234":                            {false, 0}, // empty middle cluster
		"foo1,writeonly":                                  {false, 0}, // writeonly is an invalid token now
		"a1:1234,a2:1234;b1:1234,b2:1234":                 {true, 2},
		"a1:1234,a2:1234; b1:1234,b2:1234 ":               {true, 2},
		"a1:1234,a2:1234; b1:1234,b2:1234; ":              {false, 0}, // empty last cluster
		"a1:1234,a2:1234;b1:1234,b2:1234,writeonly":       {false, 0}, // writeonly is an invalid token now
		"a1:1234,a2:1234,a3:1234;b1:1234,b2:1234,b3:1234": {true, 2},
		"a1:1234,a2:1234 ; b1:1234,b2:1234 ; c1:1234":     {true, 3},
		"a1:1234,a2:1234 ; a1:1234,b2:1234 ; c1:1234":     {false, 0}, // duplicates
	} {
		clusters, err := ParseFarmString(
			farmString,
			1*time.Second, 1*time.Second, 1*time.Second,
			1,
			pool.Murmur3,
			100,
			0*time.Millisecond,
			instrumentation.NopInstrumentation{},
		)
		if expected.success && err != nil {
			t.Errorf("%q: %s", farmString, err)
			continue
		}
		if !expected.success && err == nil {
			t.Errorf("%q: expected error, got none", farmString)
			continue
		}
		if expected, got := expected.numClusters, len(clusters); expected != got {
			t.Errorf("expected %d cluster(s), got %d", expected, got)
		}
	}
}
