package g2s

import (
	"log"
	"net"
	"strings"
	"time"
)

const (
	MessagesPerPacket = 100
)

type Statsd struct {
	connection net.Conn
	interval   time.Duration
	outgoing   []string

	messages chan sendable
	shutdown chan chan bool
}

func NewStatsd(endpoint string, interval time.Duration) (*Statsd, error) {
	connection, err := net.DialTimeout("udp", endpoint, 3*time.Second)
	if err != nil {
		return nil, err
	}
	s := &Statsd{
		connection: connection,
		interval:   interval,
		outgoing:   []string{},

		messages: make(chan sendable),
		shutdown: make(chan chan bool),
	}
	go s.loop()
	return s, nil
}

func (s *Statsd) IncrementCounter(bucket string, n int) {
	s.messages <- &counterUpdate{
		bucket: bucket,
		n:      n,
	}
}

func (s *Statsd) IncrementSampledCounter(bucket string, n int, srate float32) {
	s.messages <- &counterUpdate{
		bucket: bucket,
		n:      n,
		sampling: sampling{
			enabled: true,
			rate:    srate,
		},
	}
}

func (s *Statsd) SendTiming(bucket string, ms int) {
	s.messages <- &timingUpdate{
		bucket: bucket,
		ms:     ms,
	}
}

func (s *Statsd) SendSampledTiming(bucket string, ms int, srate float32) {
	s.messages <- &timingUpdate{
		bucket: bucket,
		ms:     ms,
		sampling: sampling{
			enabled: true,
			rate:    srate,
		},
	}
}

func (s *Statsd) UpdateGauge(bucket, val string) {
	s.messages <- &gaugeUpdate{
		bucket: bucket,
		val:    val,
	}
}
func (s *Statsd) UpdateSampledGauge(bucket, val string, srate float32) {
	s.messages <- &gaugeUpdate{
		bucket: bucket,
		val:    val,
		sampling: sampling{
			enabled: true,
			rate:    srate,
		},
	}
}

func (s *Statsd) Shutdown() {
	q := make(chan bool)
	s.shutdown <- q
	<-q
}

func (s *Statsd) loop() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case sendable := <-s.messages:
			s.outgoing = append(s.outgoing, sendable.Message())
		case <-ticker.C:
			s.publish()
		case q := <-s.shutdown:
			q <- true
			return
		}
	}
}

// publish attempts to send all the updates that have been collected since
// the last publish to the statsd server. It batches updates into packets of
// 100 messages. Failures are logged but not retried.
func (s *Statsd) publish() {
	// split outgoing messages into packets of N messages
	buffer, packets := []string{}, []string{}
	fold := func() {
		packets = append(packets, strings.Join(buffer, "\n")+"\n")
		buffer = []string{}
	}
	for _, msg := range s.outgoing {
		buffer = append(buffer, msg)
		if len(buffer) >= MessagesPerPacket {
			fold()
		}
	}
	if len(buffer) > 0 {
		fold()
	}

	// send each packet
	N := len(packets)
	for i, packet := range packets {
		n, err := s.connection.Write([]byte(packet))
		if err != nil {
			log.Printf(
				"%s: publish %d/%d: %s",
				s.connection.RemoteAddr().String(),
				i+1,
				N,
				err,
			)
		} else if n < len(packet) {
			log.Printf(
				"%s: publish %d/%d: short send: %d < %d",
				s.connection.RemoteAddr().String(),
				i+1,
				N,
				n,
				len(packet),
			)
		}
	}

	// statsd is best-effort stuff, so reset the outgoing mailbox
	s.outgoing = []string{}
}
