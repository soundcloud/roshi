
package breaker

import (
	"time"
)

type counter struct {
	success int
	failure int
}

type summary struct {
	total  int
	errors int
	rate   float64
}

type metric struct {
	counters [5 + 1]counter
	last     *counter
}

func (m metric) bucket() int {
	return int(now().Unix()) % len(m.counters)
}

func (m *metric) clear(cur *counter) {
	if m.last == nil {
		m.last = cur
	} else if cur != m.last {
		*m.last = counter{}
		m.last = cur
	}
}

func (m *metric) Success(time.Duration) {
	cur := &m.counters[m.bucket()]
	m.clear(cur)
	cur.success++
}

func (m *metric) Failure(time.Duration) {
	cur := &m.counters[m.bucket()]
	m.clear(cur)
	cur.failure++
}

func (m metric) Summary() summary {
	var sum summary

	for _, c := range m.counters {
		sum.total += c.success + c.failure
		sum.errors += c.failure
	}

	if sum.total > 0 {
		sum.rate = float64(sum.errors) / float64(sum.total)
	}

	return sum
}

