// Package shard performs key-based sharding over multiple Redis instances.
package shard

import (
	"time"

	"github.com/soundcloud/roshi/vendor/redigo/redis"
)

// Shards maintains a connection pool for multiple Redis instances.
type Shards struct {
	pools []*connectionPool
	hash  func(string) uint32
}

// New creates and returns a new Shards object.
//
// Addresses are host:port strings for each underlying Redis instance. The
// number and order of the addresses determines the hash slots, so be careful
// to make that deterministic.
//
// Connect timeout is the timeout for establishing a connection to any
// underlying Redis instance. Read timeout is the timeout for reading a reply
// to a command via an established connection. Write timeout is the timeout
// for writing a command to an established connection.
//
// Max connections per instance is the size of the connection pool for each
// Redis instance. Hash defines the hash function used by the With methods.
// Any function that takes a string and returns a uint32 may be used. Package
// shard ships with several options, including Murmur3, FNV, and FNVa.
func New(
	addresses []string,
	connectTimeout, readTimeout, writeTimeout time.Duration,
	maxConnectionsPerInstance int,
	hash func(string) uint32,
) *Shards {
	pools := make([]*connectionPool, len(addresses))
	for i, address := range addresses {
		pools[i] = newConnectionPool(
			address,
			connectTimeout, readTimeout, writeTimeout,
			maxConnectionsPerInstance,
		)
	}
	return &Shards{
		pools: pools,
		hash:  hash,
	}
}

// Index returns a reference to the connection pool that will be used to
// satisfy any request for the given key. Pass that value to WithIndex.
func (s *Shards) Index(key string) int {
	return int(s.hash(key) % uint32(len(s.pools)))
}

// Size returns how many instances the shards sits over. Useful for ranging
// over with WithIndex.
func (s *Shards) Size() int {
	return len(s.pools)
}

// WithIndex selects a single Redis instance from the referenced connection
// pool, and then calls the given function with that connection. If the
// function returns a nil error, WithIndex returns the connection to the pool
// after it's used. Otherwise, WithIndex discards the connection.
//
// WithIndex will return an error if it wasn't able to successfully retrieve a
// connection from the referenced connection pool, and will forward any error
// returned by the `do` function.
func (s *Shards) WithIndex(index int, do func(redis.Conn) error) error {
	conn, err := s.pools[index].get() // blocking up to connectTimeout
	defer s.pools[index].put(conn)    // always put, even if it's nil
	if err != nil {
		return err
	}

	err = do(conn)
	if err != nil {
		conn.Close() // deferred `put` will detect this, and reject the conn
	}
	return err
}

// With is a convenience function that combines Index and WithIndex, for
// simple/single Redis requests on a single key.
func (s *Shards) With(key string, do func(redis.Conn) error) error {
	index := s.Index(key)
	return s.WithIndex(index, do)
}

// Close closes all available (idle) connections in the cluster.
// Close does not affect oustanding (in-use) connections.
func (s *Shards) Close() error {
	for _, pool := range s.pools {
		pool.closeAll()
	}
	return nil
}
