package g2s

import (
	"io"
	"log"
	"math/rand"
	"net"
	"time"
)

const (
	MAX_PACKET_SIZE = 65536
)

type Statter interface {
	Counter(sampleRate float32, bucket string, n ...int)
	Timing(sampleRate float32, bucket string, d ...time.Duration)
	Gauge(sampleRate float32, bucket string, value ...string)
}

type Statsd struct {
	w io.Writer
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
	return New(c)
}

// New constructs a Statsd structure which will write statsd-protocol messages
// into the given io.Writer. New is intended to be used by consumers who want
// nonstandard behavior: for example, they may pass an io.Writer which performs
// buffering and aggregation of statsd-protocol messages.
//
// Note that g2s provides no synchronization. If you pass an io.Writer which
// is not goroutine-safe, for example a bytes.Buffer, you must make sure you
// synchronize your calls to the Statter methods.
func New(w io.Writer) (*Statsd, error) {
	return &Statsd{
		w: w,
	}, nil
}

// bufferize folds the slice of sendables into a slice of byte-buffers,
// each of which shall be no larger than max bytes. Each byte buffer is
// guaranteed to end with '\n'.
func bufferize(sendables []sendable, max int) [][]byte {
	bN := [][]byte{}
	b1, b1sz := []byte{}, 0

	for _, sendable := range sendables {
		buf := []byte(sendable.Message())
		if b1sz+len(buf) > max {
			bN = append(bN, b1)
			b1 = buf
			b1sz = len(buf)
			continue
		}
		b1 = append(b1, buf...)
		b1sz += len(buf)
	}

	if len(b1) > 0 {
		bN = append(bN, b1)
	}

	return bN
}

func (s *Statsd) publish(msgs []sendable) {
	for _, buf := range bufferize(msgs, MAX_PACKET_SIZE) {
		// In the base case, "Multiple goroutines may invoke methods on a Conn
		// simultaneously." -- http://golang.org/pkg/net/#Conn
		//
		// ...otherwise, Bring Your Own Synchronization.
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

func (s *Statsd) Counter(sampleRate float32, bucket string, n ...int) {
	samp, ok := maybeSample(sampleRate)
	if !ok {
		return
	}

	msgs := make([]sendable, len(n))
	for i, ni := range n {
		msgs[i] = &counterUpdate{
			bucket:   bucket,
			n:        ni,
			sampling: samp,
		}
	}

	s.publish(msgs)
}

func (s *Statsd) Timing(sampleRate float32, bucket string, d ...time.Duration) {
	samp, ok := maybeSample(sampleRate)
	if !ok {
		return
	}

	msgs := make([]sendable, len(d))
	for i, di := range d {
		msgs[i] = &timingUpdate{
			bucket:   bucket,
			ms:       int(di.Nanoseconds() / 1e9),
			sampling: samp,
		}
	}

	s.publish(msgs)
}

func (s *Statsd) Gauge(sampleRate float32, bucket string, v ...string) {
	samp, ok := maybeSample(sampleRate)
	if !ok {
		return
	}

	msgs := make([]sendable, len(v))
	for i, vi := range v {
		msgs[i] = &gaugeUpdate{
			bucket:   bucket,
			val:      vi,
			sampling: samp,
		}
	}

	s.publish(msgs)
}
