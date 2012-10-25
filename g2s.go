package g2s

import (
	"log"
	"net"
	"time"
)

type Statsd struct {
	connection net.Conn
}

func NewStatsd(endpoint string) (*Statsd, error) {
	connection, err := net.DialTimeout("udp", endpoint, 3*time.Second)
	if err != nil {
		return nil, err
	}
	return &Statsd{
		connection: connection,
	}, nil
}

func (s *Statsd) publish(msg sendable) {
	// "Multiple goroutines may invoke methods on a Conn simultaneously."
	// http://golang.org/pkg/net/#Conn
	buf := []byte(msg.Message())
	n, err := s.connection.Write(buf)
	if err != nil {
		log.Printf(
			"%s: publish: %s",
			s.connection.RemoteAddr().String(),
			err,
		)
	} else if n != len(buf) {
		log.Printf(
			"%s: publish: short send: %d < %d",
			s.connection.RemoteAddr().String(),
			n,
			len(buf),
		)
	}
}

func (s *Statsd) IncrementCounter(bucket string, n int) {
	s.publish(&counterUpdate{
		bucket: bucket,
		n:      n,
	})
}

func (s *Statsd) IncrementSampledCounter(bucket string, n int, srate float32) {
	s.publish(&counterUpdate{
		bucket: bucket,
		n:      n,
		sampling: sampling{
			enabled: true,
			rate:    srate,
		},
	})
}

func (s *Statsd) SendTiming(bucket string, ms int) {
	s.publish(&timingUpdate{
		bucket: bucket,
		ms:     ms,
	})
}

func (s *Statsd) SendSampledTiming(bucket string, ms int, srate float32) {
	s.publish(&timingUpdate{
		bucket: bucket,
		ms:     ms,
		sampling: sampling{
			enabled: true,
			rate:    srate,
		},
	})
}

func (s *Statsd) UpdateGauge(bucket, val string) {
	s.publish(&gaugeUpdate{
		bucket: bucket,
		val:    val,
	})
}

func (s *Statsd) UpdateSampledGauge(bucket, val string, srate float32) {
	s.publish(&gaugeUpdate{
		bucket: bucket,
		val:    val,
		sampling: sampling{
			enabled: true,
			rate:    srate,
		},
	})
}
