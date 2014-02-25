package pool

import (
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type connectionPool struct {
	mu *sync.Mutex
	co *sync.Cond

	address string
	connect time.Duration
	read    time.Duration
	write   time.Duration

	available   []redis.Conn
	outstanding int
	max         int
}

func newConnectionPool(
	address string,
	connectTimeout, readTimeout, writeTimeout time.Duration,
	maxConnections int,
) *connectionPool {
	mu := &sync.Mutex{}
	co := sync.NewCond(mu)
	return &connectionPool{
		mu: mu,
		co: co,

		address: address,
		connect: connectTimeout,
		read:    readTimeout,
		write:   writeTimeout,

		available:   []redis.Conn{},
		outstanding: 0,
		max:         maxConnections,
	}
}

func (p *connectionPool) get() (redis.Conn, error) {
	p.mu.Lock()
	for {
		available := len(p.available)
		switch {
		case available <= 0 && p.outstanding >= p.max:
			// Worst case. No connection available, and we can't dial a new one.
			p.co.Wait() // TODO starvation is possible here

		case available <= 0 && p.outstanding < p.max:
			// No connection available, but we can dial a new one.
			//
			// We shouldn't wait for a connection to be successfully established
			// before incrementing our outstanding counter, because additional
			// goroutines may sneak in with a get() request while we're dialing,
			// and bump outstanding above p.max.
			//
			// So, clients of get() should always put() the resulting conn, even
			// if it is nil. put() must handle that circumstance.
			p.outstanding++
			p.mu.Unlock()
			return redis.DialTimeout("tcp", p.address, p.connect, p.read, p.write)

		case available > 0:
			// Best case. We can directly use an available connection.
			var conn redis.Conn
			conn, p.available = p.available[0], p.available[1:]
			if p.outstanding < p.max {
				p.outstanding++
			}
			p.mu.Unlock()
			return conn, nil
		}
	}
}

func (p *connectionPool) put(conn redis.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn == nil || conn.Err() != nil {
		// Failed to dial, closed, or some other problem
		if p.outstanding > 0 {
			p.outstanding--
		}
		p.co.Signal() // someone can try to dial
		return
	}

	if len(p.available) >= p.max {
		go conn.Close() // don't block
		return
	}

	p.available = append(p.available, conn)
	if p.outstanding > 0 {
		p.outstanding--
	}
	p.co.Signal()
}

func (p *connectionPool) closeAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range p.available {
		conn.Close()
	}
	p.available = []redis.Conn{}
	return nil
}
