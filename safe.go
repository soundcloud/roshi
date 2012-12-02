package g2s

import (
	"time"
)

type noStatsd struct{}

func (n noStatsd) Counter(float32, string, ...int)          {}
func (n noStatsd) Timing(float32, string, ...time.Duration) {}
func (n noStatsd) Gauge(float32, string, ...string)         {}

// SafeDial attempts to Dial the given proto and endpoint, just like Dial.
// If that Dial fails for any reason, SafeDial is different in that it returns
// a valid Statter whose methods will return without doing anything.
func SafeDial(proto, endpoint string) Statter {
	if yesStatsd, err := Dial(proto, endpoint); err == nil {
		return yesStatsd
	}
	return noStatsd{}
}
