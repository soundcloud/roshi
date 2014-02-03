package shard_test

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/soundcloud/roshi/shard"
	"github.com/soundcloud/roshi/vendor/redigo/redis"
)

func TestRecovery(t *testing.T) {
	binary := "redis-server"
	absBinary, err := exec.LookPath(binary)
	if err != nil {
		t.Fatalf("%s: %s", binary, err)
	}

	// Build a cluster.
	port := "10001"
	maxConnectionsPerInstance := 2
	redisTimeout := 50 * time.Millisecond
	s := shard.New(
		[]string{"localhost:" + port},
		redisTimeout, redisTimeout, redisTimeout,
		maxConnectionsPerInstance,
		shard.Murmur3,
	)

	func() {
		// Start Redis
		cmd := exec.Command(absBinary, "--port", port)
		if err := cmd.Start(); err != nil {
			t.Fatalf("Starting %s: %s", binary, err)
		}
		defer cmd.Process.Kill()
		waitDuration, err := time.ParseDuration(os.Getenv("TEST_REDIS_WAIT_DURATION"))
		if err != nil {
			waitDuration = 10 * time.Millisecond
		}
		time.Sleep(waitDuration)

		// Try initial PING
		if err := s.With("irrelevant", func(conn redis.Conn) error {
			_, err := conn.Do("PING")
			return err
		}); err != nil {
			t.Fatalf("Initial PING failed: %s", err)
		}
		t.Logf("Initial PING OK")
	}()

	terminal := make(chan struct{})
	requests := maxConnectionsPerInstance * 2 // > maxConnectionsPerInstance
	go func() {
		// Redis is down. Make a bunch of requests. All should fail quickly.
		for i := 0; i < requests; i++ {
			if err := s.With("irrelevant", func(conn redis.Conn) error {
				_, err := conn.Do("PING")
				return err
			}); err == nil {
				t.Errorf("Terminal PING succeeded, but we expected failure.")
			} else {
				t.Logf("Terminal PING failed (%s), but that was expected", err)
			}
		}
		close(terminal)
	}()
	select {
	case <-terminal:
		t.Logf("Terminal PINGs completed in time.")
	case <-time.After(2 * time.Duration(requests) * redisTimeout):
		t.Fatalf("Terminal PINGs timed out. Deadlock in connection pool?")
	}

	func() {
		// Restart Redis
		cmd := exec.Command(absBinary, "--port", port)
		if err := cmd.Start(); err != nil {
			t.Fatalf("Starting %s: %s", binary, err)
		}
		defer cmd.Process.Kill()
		time.Sleep(10 * time.Millisecond)

		// Try second PING x1
		err := s.With("irrelevant", func(conn redis.Conn) error {
			_, err := conn.Do("PING")
			return err
		})
		t.Logf("Second PING x1 gave error %v (just FYI)", err)

		// Try second PING x2
		if err := s.With("irrelevant", func(conn redis.Conn) error {
			_, err := conn.Do("PING")
			return err
		}); err != nil {
			t.Errorf("Second PING x2 failed: %s", err)
		} else {
			t.Logf("Second PING x2 OK")
		}
	}()
}
