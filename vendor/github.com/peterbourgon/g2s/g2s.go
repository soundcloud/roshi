package g2s

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"
)

const maxPacketSize = 65536 - 8 - 20 // 8-byte UDP header, 20-byte IP header

// Statter collects the ways clients can send observations to a StatsD server.
type Statter interface {
	Counter(sampleRate float32, bucket string, n ...int)
	Timing(sampleRate float32, bucket string, d ...time.Duration)
	Gauge(sampleRate float32, bucket string, value ...string)
}

// Statsd implements the Statter interface by writing StatsD formatted messages
// to the underlying writer.
type Statsd struct {
	w      io.Writer
	prefix string
}

// Dial takes the same parameters as net.Dial, ie. a transport protocol
// (typically "udp") and an endpoint. It returns a new Statsd structure,
// ready to use.
//
// Note that g2s currently performs no management on the connection it creates.
func Dial(proto, endpoint string) (*Statsd, error) {
	c, err := net.DialTimeout(proto, endpoint, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return New(c, "")
}

// Variant of Dial that accepts a prefix that will be prepended to the name of
// all buckets sent (so it can be used with services that require an API key)
func DialWithPrefix(proto, endpoint string, prefix string) (*Statsd, error) {
	c, err := net.DialTimeout(proto, endpoint, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return New(c, prefix)
}

// New constructs a Statsd structure which will write statsd-protocol messages
// into the given io.Writer. New is intended to be used by consumers who want
// nonstandard behavior: for example, they may pass an io.Writer which performs
// buffering and aggregation of statsd-protocol messages.
//
// Note that g2s provides no synchronization. If you pass an io.Writer which
// is not goroutine-safe, for example a bytes.Buffer, you must make sure you
// synchronize your calls to the Statter methods.
func New(w io.Writer, prefix string) (*Statsd, error) {
	if len(prefix) > 0 && !strings.HasSuffix(prefix, ".") {
		prefix = fmt.Sprintf("%s.", prefix)
	}
	return &Statsd{w: w, prefix: prefix}, nil
}

// bufferize folds the slice of sendables into a slice of byte-buffers,
// each of which shall be no larger than max bytes.
func bufferize(sendables []sendable, max int) [][]byte {
	bN := [][]byte{}
	b1, b1sz := []byte{}, 0

	for _, sendable := range sendables {
		buf := []byte(sendable.Message())
		if b1sz+len(buf) > max {
			bN = append(bN, b1)
			b1, b1sz = []byte{}, 0
		}
		b1 = append(b1, buf...)
		b1sz += len(buf)
	}

	if len(b1) > 0 {
		bN = append(bN, b1[0:len(b1)-1])
	}

	return bN
}

// publish folds the slice of sendables into one or more packets, each of which
// will be no larger than maxPacketSize. It then writes them, one by one,
// into the Statsd io.Writer.
func (s *Statsd) publish(msgs []sendable) {
	for _, buf := range bufferize(msgs, maxPacketSize) {
		// In the base case, when the Statsd struct is backed by a net.Conn,
		// "Multiple goroutines may invoke methods on a Conn simultaneously."
		//   -- http://golang.org/pkg/net/#Conn
		// Otherwise, Bring Your Own Synchronization™.
		n, err := s.w.Write(buf)
		if err != nil {
			log.Printf("g2s: publish: %s", err)
		} else if n != len(buf) {
			log.Printf("g2s: publish: short send: %d < %d", n, len(buf))
		}
	}
}

// maybeSample returns a sampling structure and true if a pseudorandom number
// in the range 0..1 is less than or equal to the passed rate.
//
// As a special case, if r >= 1.0, maybeSample will return an uninitialized
// sampling structure and true. The uninitialized sampling structure implies
// enabled == false, which tells statsd that the value is unsampled.
func maybeSample(r float32) (sampling, bool) {
	if r >= 1.0 {
		return sampling{}, true
	}

	if rand.Float32() > r {
		return sampling{}, false
	}

	return sampling{
		enabled: true,
		rate:    r,
	}, true
}

// Counter sends one or more counter statistics to StatsD.
//
// Application code should call it for every potential invocation of a
// statistic; it uses the sampleRate to determine whether or not to send or
// squelch the data, on an aggregate basis.
func (s *Statsd) Counter(sampleRate float32, bucket string, n ...int) {
	samp, ok := maybeSample(sampleRate)
	if !ok {
		return
	}

	msgs := make([]sendable, len(n))
	for i, ni := range n {
		msgs[i] = &counterUpdate{
			prefix:   s.prefix,
			bucket:   bucket,
			n:        ni,
			sampling: samp,
		}
	}

	s.publish(msgs)
}

// Timing sends one or more timing statistics to StatsD.
//
// Application code should call it for every potential invocation of a
// statistic; it uses the sampleRate to determine whether or not to send or
// squelch the data, on an aggregate basis.
func (s *Statsd) Timing(sampleRate float32, bucket string, d ...time.Duration) {
	samp, ok := maybeSample(sampleRate)
	if !ok {
		return
	}

	msgs := make([]sendable, len(d))
	for i, di := range d {
		msgs[i] = &timingUpdate{
			prefix:   s.prefix,
			bucket:   bucket,
			ms:       int(di.Nanoseconds() / 1e6),
			sampling: samp,
		}
	}

	s.publish(msgs)
}

// Gauge sends one or more gauge statistics to Statsd.
//
// Application code should call it for every potential invocation of a
// statistic; it uses the sampleRate to determine whether or not to send or
// squelch the data, on an aggregate basis.
func (s *Statsd) Gauge(sampleRate float32, bucket string, v ...string) {
	samp, ok := maybeSample(sampleRate)
	if !ok {
		return
	}

	msgs := make([]sendable, len(v))
	for i, vi := range v {
		msgs[i] = &gaugeUpdate{
			prefix:   s.prefix,
			bucket:   bucket,
			val:      vi,
			sampling: samp,
		}
	}

	s.publish(msgs)
}
