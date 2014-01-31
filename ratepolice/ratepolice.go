// Package ratepolice helps to track a rate as a moving average and
// then inquire how many events can be added to not exceed a given
// target rate.
package ratepolice

import (
	"time"
)

const (
	// MaxInt is what you think it is. Unfortunately not provided
	// by the go math package.
	MaxInt = int(^uint(0) >> 1)
)

// Reporter is the interface to report events to the rate police.
type Reporter interface {
	// Report reports n events as 'just happened' to the rate
	// police.
	Report(n int)
}

// Requester is the interface to request the number of events
// permitted to not exceed a given target rate.
type Requester interface {
	// Request asks the rate police how many events may happen
	// 'right now' so that the running average of the rate will
	// not exceed targetRatePerSec. (The value can be negative if
	// the current moving average rate is already exceeding the
	// target rate.) However, to avoid spikes, the maximum return
	// value is capped at twice the value that would need to go
	// into each bucket to sustain the targetRatePerSec. Example:
	// If the window size for the moving average is 5s, 10 buckets
	// are used for the moving average, the target rate is 100
	// events/s, and no events have been reported over the last
	// 5s, Request would return 500 (since 500 events could go
	// into the current window to keep the moving average at 100
	// events/s). However, in a perfectly even distribution of
	// events, each bucket would receive only 50 events. To not
	// deviate too far from an even distribution, Request will
	// return at most 2*50=100. (See the New function for details
	// about the moving average window and buckets.)
	Request(targetRatePerSec int) int
}

// RatePolice combines the Reporter and Requester interfaces. Create
// instances with New or NewNop.
type RatePolice interface {
	Reporter
	Requester
}

// New creates an implementation of RatePolice. The moving average is
// calculated over a time window given by movingAverageWindow. The
// calculation is done using time buckets (numberOfBuckets buckets,
// each movingAverageWindow/numberOfBuckets long). The time window
// should be approximately as long as the time over which events can
// interact with each other at the expected target rate. (For example,
// you run a web service where queries may have a latency of up to 5s
// under load (let's say a timeout kicks in after 5s so that queries
// will not run for longer than that). Your running average window
// should be ~5s then, too, so that a spike of queries will continue
// to be charged against the tracked rate for about as long as these
// queries may linger and consume resources.) A higher number of
// buckets increases the precision of the calculation, but requires
// more computational work. ~10 buckets should usually be enough for
// practical purposes. The actual value depends on your expected rates
// in relation to the moving average window.
func New(movingAverageWindow time.Duration, numberOfBuckets int) RatePolice {
	rp := &ratePolice{
		buckets:  make([]int, numberOfBuckets),
		reports:  make(chan int),
		requests: make(chan request),
	}
	rp.currentBucketStartTime = time.Now()
	rp.movingAverageWindow = movingAverageWindow
	rp.bucketDuration = movingAverageWindow / time.Duration(numberOfBuckets)
	go rp.loop()
	return rp
}

// NewNop creates a rate police implementation that allows everything,
// i.e. its Report call is a no-op, and the Request call will always
// return MaxInt.
func NewNop() RatePolice {
	return &noPolice{}
}

type ratePolice struct {
	buckets                []int
	reports                chan int
	requests               chan request
	currentBucket          int
	currentBucketStartTime time.Time
	movingAverageWindow    time.Duration
	bucketDuration         time.Duration
	bucketSum              int
}

type request struct {
	targetRatePerSec int
	result           chan int
}

func (rp *ratePolice) Report(n int) {
	rp.reports <- n
}

func (rp *ratePolice) Request(targetRatePerSec int) int {
	result := make(chan int)
	rp.requests <- request{targetRatePerSec: targetRatePerSec, result: result}
	return <-result
}

func (rp *ratePolice) loop() {
	for {
		select {
		case reported := <-rp.reports:
			rp.updateBuckets()
			rp.buckets[rp.currentBucket] += reported
			rp.bucketSum += reported
		case requested := <-rp.requests:
			rp.updateBuckets()
			max := int(time.Duration(requested.targetRatePerSec) * rp.movingAverageWindow / time.Second)
			granted := max - rp.bucketSum
			cap := 2 * max / len(rp.buckets)
			if granted > cap {
				granted = cap
			}
			requested.result <- granted
		}
	}
}

func (rp *ratePolice) updateBuckets() {
	now := time.Now()
	bucketShift := int(now.Sub(rp.currentBucketStartTime) / rp.bucketDuration)
	if bucketShift <= 0 {
		return
	}
	rp.currentBucketStartTime = now
	if bucketShift >= len(rp.buckets) {
		// Shortcut. Just empty all buckets in this case.
		rp.bucketSum = 0
		rp.buckets = make([]int, len(rp.buckets)) // Actually faster than zero'ing in a loop.
		return
	}
	for ; bucketShift > 0; bucketShift-- {
		rp.currentBucket++
		if rp.currentBucket >= len(rp.buckets) {
			rp.currentBucket = 0
		}
		rp.bucketSum -= rp.buckets[rp.currentBucket]
		rp.buckets[rp.currentBucket] = 0
	}
}

type noPolice struct{}

func (rp *noPolice) Report(n int) {
	return
}

func (rp *noPolice) Request(targetRatePerSec int) int {
	return MaxInt
}
