package g2s

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

//
//
//

type Statsd struct {
	connection net.Conn
	stats      map[string]*stat
	interval   time.Duration
	outgoing   []string

	registrations   chan registration
	samplingChanges chan samplingChange
	updates         chan update
	shutdown        chan chan bool
}

func NewStatsd(endpoint string, interval time.Duration) (*Statsd, error) {
	connection, err := net.DialTimeout("udp", endpoint, 3*time.Second)
	if err != nil {
		return nil, err
	}
	s := &Statsd{
		connection: connection,
		stats:      map[string]*stat{},
		interval:   interval,
		outgoing:   []string{},

		registrations:   make(chan registration),
		samplingChanges: make(chan samplingChange),
		updates:         make(chan update),
		shutdown:        make(chan chan bool),
	}
	go s.loop()
	return s, nil
}

func (s *Statsd) Register(bucket string, statType StatType) error {
	r := registration{
		bucket:   bucket,
		statType: statType,
		err:      make(chan error),
	}
	s.registrations <- r
	return <-r.err
}

func (s *Statsd) SetSamplingRate(bucket string, samplingRate float32) error {
	c := samplingChange{
		bucket:       bucket,
		samplingRate: samplingRate,
		err:          make(chan error),
	}
	s.samplingChanges <- c
	return <-c.err
}

func (s *Statsd) IncrementCounter(bucket string, n int) {
	s.updates <- &counterUpdate{
		bucket: bucket,
		n:      n,
	}
}

func (s *Statsd) SendTiming(bucket string, ms int) {
	s.updates <- &timingUpdate{
		bucket: bucket,
		ms:     ms,
	}
}

func (s *Statsd) UpdateGauge(bucket string, val string) {
	s.updates <- &gaugeUpdate{
		bucket: bucket,
		val:    val,
	}
}

func (s *Statsd) loop() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case r := <-s.registrations:
			r.err <- s.register(r.bucket, r.statType)
		case c := <-s.samplingChanges:
			c.err <- s.changeSampling(c.bucket, c.samplingRate)
		case u := <-s.updates:
			s.outgoing = append(s.outgoing, u.Message())
		case <-ticker.C:
			s.publish()
		case q := <-s.shutdown:
			q <- true
			return
		}
	}
}

func (s *Statsd) register(bucket string, statType StatType) error {
	if _, ok := s.stats[bucket]; ok {
		return fmt.Errorf("%s: already registered", bucket)
	}
	s.stats[bucket] = &stat{
		statType: statType,
	}
	return nil
}

func (s *Statsd) changeSampling(bucket string, samplingRate float32) error {
	st, ok := s.stats[bucket]
	if !ok {
		return fmt.Errorf("%s: not registered", bucket)
	}
	return st.setSamplingRate(samplingRate)
}

// publish attempts to send all the updates that have been collected since
// the last publish to the statsd server. It batches updates into packets of
// 100 messages. Failures are logged but not retried.
func (s *Statsd) publish() {
	// split outgoing messages into packets of 100 messages
	buffer, packets := []string{}, []string{}
	fold := func() {
		packets = append(packets, strings.Join(buffer, "\n")+"\n")
		buffer = []string{}
	}
	for _, msg := range s.outgoing {
		buffer = append(buffer, msg)
		if len(buffer) >= 100 {
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
