# shard

[API documentation](http://godoc.org/github.com/soundcloud/roshi/shard).

Package shard connects to multiple physical Redis instances, and emulates a
single logical Redis instance. Clients are expected (but not required) to use
their Redis keys as hash keys to select a Redis instance. The package
maintains a pool of connections to each instance.

## Constructor parameters

* **addresses** are host:port strings for each Redis instance. The number and
  order of addresses is significant: they determines the size of, and position
  of instances within, the hash ring.

* **connectTimeout** is the timeout for establishing a connection to each
  underlying Redis instance.

* **readTimeout** is the timeout for reading a reply to a command from an
  established connection.

* **writeTimeout** is the timeout for writing a command to an established
  connection.

* **maxConnectionsPerInstance** determines the size of the connection pool for
  each Redis instance.

* **hash** defines the hash function used by the With method. Any function that
  takes a `string` and returns a `uint32` may be used. Package shard ships with
  several options, including Murmur3 (recommended), FNV, and FNVa.

## Usage

Simple usage with a single key.

```go
s := shard.New(...)
defer s.Close()

key, value := "foo", "bar"
if err := s.With(key, func(c redis.Conn) error {
	_, err := c.Do("SET", key, value)
	return err
}); err != nil {
	log.Printf("Failure: %s", err)
}
```

Keys may be pre-hashed with the Index method, and connections used for
pipelining.

```go
m := map[int][]string{} // index: keys to INCR
for _, key := range keys {
	index := s.Index(key)
	m[index] = append(m[index], key)
}

wg := sync.WaitGroup{}
wg.Add(len(m))
for index, keys := range m {
	// Shards are safe for concurrent access.
	go s.WithIndex(index, func(c redis.Conn) error) {
		defer wg.Done()
		for _, key := range keys {
			if err := c.Send("INCR", key); err != nil {
				return err
			}
		}
		if err := c.Flush(); err != nil {
			return err
		}
		for _ = range keys {
			if _, err := c.Receive(); err != nil {
				return err
			}
		}
		return nil
	})
}
wg.Wait()
```
