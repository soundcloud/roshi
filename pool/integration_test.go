package pool_test

import (
	"os/exec"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/soundcloud/roshi/pool"
)

func TestRecovery(t *testing.T) {
	binary := "redis-server"
	absBinary, err := exec.LookPath(binary)
	if err != nil {
		t.Fatalf("%s: %s", binary, err)
	}

	// Build a cluster.
	var (
		port                      = "10001"
		maxConnectionsPerInstance = 2
		redisTimeout              = 3 * time.Second
		redisGracePeriod          = 5 * time.Second
	)
	p := pool.New(
		[]string{"localhost:" + port},
		redisTimeout, redisTimeout, redisTimeout,
		maxConnectionsPerInstance,
		pool.Murmur3,
	)

	func() {
		// Start Redis
		cmd := exec.Command(absBinary, "--port", port)
		if err := cmd.Start(); err != nil {
			t.Fatalf("Starting %s: %s", binary, err)
		}
		defer cmd.Process.Kill()

		time.Sleep(redisGracePeriod)

		// Try initial PING
		if err := p.With("irrelevant", func(conn redis.Conn) error {
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
			if err := p.With("irrelevant", func(conn redis.Conn) error {
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

		time.Sleep(redisGracePeriod)

		// Try second PING x1
		err := p.With("irrelevant", func(conn redis.Conn) error {
			_, err := conn.Do("PING")
			return err
		})
		t.Logf("Second PING x1 gave error %v (just FYI)", err)
		time.Sleep(1 * time.Second) // attempt to scoot by a problem with Travis

		// Try second PING x2
		if err := p.With("irrelevant", func(conn redis.Conn) error {
			_, err := conn.Do("PING")
			return err
		}); err != nil {
			t.Errorf("Second PING x2 failed: %s", err)
		} else {
			t.Logf("Second PING x2 OK")
		}
	}()
}
