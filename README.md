# g2s

Get to Statsd: forward simple statistics to a statsd server.

[![Build Status][1]][2]

[1]: https://secure.travis-ci.org/peterbourgon/g2s.png
[2]: http://www.travis-ci.org/peterbourgon/g2s

# Usage

g2s provides a Statsd object, which provides some convenience functions for
each of the supported statsd statistic-types. Just call the relevant function
on the Statsd object wherever it makes sense in your code.

Updates are accumulated and sent to the statsd server in batches according to
the interval parameter. A relatively low interval (1s) is recommended.

```go
sd, err := g2s.NewStatsd("statsd-server:8125", 1*time.Second)
if err != nil {
	// do something
}

sd.IncrementCounter("my.silly.counter", 1)
sd.SendTiming("my.silly.slow-process", 534)
sd.SendSampledTiming("my.silly.fast-process", 7, 0.1)
sd.UpdateGauge("my.silly.status", "green")
```

All 'update'-class functions are goroutine safe, and should return immediately.
