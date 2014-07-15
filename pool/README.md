# pool

[![GoDoc](https://godoc.org/github.com/soundcloud/roshi/pool?status.png)](https://godoc.org/github.com/soundcloud/roshi/pool)

Package pool connects to multiple physical Redis instances, and emulates a
single logical Redis instance. Clients are expected (but not required) to use
their Redis keys as hash keys to select a Redis instance. The package
maintains a connection pool to each instance.

## Usage

Simple usage with a single key.

```go
p := pool.New(...)
defer p.Close()

key, value := "foo", "bar"
if err := p.With(key, func(c redis.Conn) error {
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
	index := p.Index(key)
	m[index] = append(m[index], key)
}

wg := sync.WaitGroup{}
wg.Add(len(m))
for index, keys := range m {
	// Pool is safe for concurrent access.
	go func(index int, keys []string) {
		p.WithIndex(index, func(c redis.Conn) error) {
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
	}(index, keys)
}
wg.Wait()
```
