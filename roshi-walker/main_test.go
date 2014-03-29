package main

import (
	"testing"
	"time"
)

func TestThrottleFailure(t *testing.T) {
	maxKeysPerSec := int64(1000)
	batchSize := int64(100)
	batches := int64(30)
	expectedDuration := time.Duration((batches*batchSize)/maxKeysPerSec) * time.Second

	done := make(chan struct{})
	go func() {
		defer close(done)
		throttle := newThrottle(maxKeysPerSec)
		for batch := int64(0); batch < batches; batch++ {
			begin := time.Now()
			throttle.wait(batchSize)
			t.Logf("batch of %d in %s", batchSize, time.Since(begin))
		}
	}()

	select {
	case <-done:
	case <-time.After(expectedDuration + 100*time.Millisecond):
		t.Errorf("timeout")
	}
}
