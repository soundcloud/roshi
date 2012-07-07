package g2s

import (
	"strings"
	"testing"
	"sync"
	"fmt"
	"net"
	"time"
	"math/rand"
)

func TestPublish1(t *testing.T) {
	d := 25 * time.Millisecond
	mock := NewMockStatsd(t, 12345)
	sd, err := NewStatsd("localhost:12345", d)
	if err != nil {
		t.Fatalf("%s", err)
	}

	sd.IncrementCounter("foo", 1)
	time.Sleep(2 * d)
	if lines := mock.Lines(); lines != 1 {
		t.Errorf("expected 1 line, got %d", lines)
	}
	if packets := mock.Packets(); packets != 1 {
		t.Errorf("expected 1 packet, got %d", packets)
	}

	mock.Shutdown()
}

func TestPublishManyLines(t *testing.T) {
	d := 25 * time.Millisecond
	mock := NewMockStatsd(t, 12345)
	sd, err := NewStatsd("localhost:12345", d)
	if err != nil {
		t.Fatalf("%s", err)
	}

	sd.IncrementCounter("foo", 1)
	sd.SendSampledTiming("bar", 201, 0.1)
	sd.UpdateGauge("baz", "green")
	time.Sleep(2 * d)
	if lines := mock.Lines(); lines != 3 {
		t.Errorf("expected 3 lines, got %d", lines)
	}
	if packets := mock.Packets(); packets != 1 {
		t.Errorf("expected 1 packet, got %d", packets)
	}

	mock.Shutdown()
}

func TestPublishManyPackets(t *testing.T) {
	d := 25 * time.Millisecond
	mock := NewMockStatsd(t, 12345)
	sd, err := NewStatsd("localhost:12345", d)
	if err != nil {
		t.Fatalf("%s", err)
	}

	sd.IncrementCounter("foo", 1)
	time.Sleep(2 * d)
	sd.SendSampledTiming("bar", 201, 0.1)
	sd.UpdateGauge("baz", "green")
	time.Sleep(2 * d)

	if lines := mock.Lines(); lines != 3 {
		t.Errorf("expected 3 lines, got %d", lines)
	}
	if packets := mock.Packets(); packets != 2 {
		t.Errorf("expected 2 packets, got %d", packets)
	}

	mock.Shutdown()
}

func TestLoad(t *testing.T) {
	d := 25 * time.Millisecond
	mock := NewMockStatsd(t, 12345)
	sd, err := NewStatsd("localhost:12345", d)
	if err != nil {
		t.Fatalf("%s", err)
	}

	sends := 1234 // careful: too high, and we take >d ms to send
	for i := 0; i < sends; i++ {
		bucket := fmt.Sprintf("bucket-%02d", i%23)
		n := rand.Intn(100)
		sd.SendTiming(bucket, n)
	}
	time.Sleep(2 * d)

	expectedLines := sends
	if lines := mock.Lines(); lines != expectedLines {
		t.Errorf("expected %d lines, got %d", expectedLines, lines)
	}
	expectedPackets := sends / MessagesPerPacket
	if sends%MessagesPerPacket != 0 {
		expectedPackets++
	}
	if packets := mock.Packets(); packets != expectedPackets {
		t.Errorf("expected %d packets, got %d", expectedPackets, packets)
	}

	mock.Shutdown()
}

//
//
//

type MockStatsd struct {
	t       *testing.T
	port    int
	packets int
	lines   int
	mtx     sync.Mutex
	ln      *net.UDPConn
	done    chan bool
}

func NewMockStatsd(t *testing.T, port int) *MockStatsd {
	m := &MockStatsd{
		t:    t,
		port: port,
		done: make(chan bool, 1),
	}
	go m.loop()
	return m
}

func (m *MockStatsd) Packets() int {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.packets
}
func (m *MockStatsd) Lines() int {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.lines
}

func (m *MockStatsd) Shutdown() {
	m.ln.Close()
	<-m.done
}

func (m *MockStatsd) loop() {
	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("localhost:%d", m.port))
	if err != nil {
		panic(err)
	}
	ln, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		panic(err)
	}
	m.ln = ln
	b := make([]byte, 1024*1024)
	for {
		n, _, err := m.ln.ReadFrom(b)
		if err != nil {
			m.t.Logf("Mock Statsd: read error: %s", err)
			m.done <- true
			return
		}
		s := strings.TrimSpace(string(b[:n]))
		m.t.Logf("Mock Statsd: read %dB: %s", n, s)
		m.mtx.Lock()
		m.packets++
		m.lines += len(strings.Split(s, "\n"))
		m.mtx.Unlock()
	}
}
