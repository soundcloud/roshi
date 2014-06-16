// roshi-walker walks the keyspace and performs repairing Selects.
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/peterbourgon/g2s"
	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/farm"
	"github.com/soundcloud/roshi/instrumentation"
	"github.com/soundcloud/roshi/instrumentation/prometheus"
	"github.com/soundcloud/roshi/instrumentation/statsd"
	"github.com/soundcloud/roshi/pool"
	"github.com/tsenart/tb"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	var (
		redisInstances      = flag.String("redis.instances", "", "Semicolon-separated list of comma-separated lists of Redis instances")
		redisConnectTimeout = flag.Duration("redis.connect.timeout", 3*time.Second, "Redis connect timeout")
		redisReadTimeout    = flag.Duration("redis.read.timeout", 3*time.Second, "Redis read timeout")
		redisWriteTimeout   = flag.Duration("redis.write.timeout", 3*time.Second, "Redis write timeout")
		redisMCPI           = flag.Int("redis.mcpi", 2, "Max connections per Redis instance")
		redisHash           = flag.String("redis.hash", "murmur3", "Redis hash function: murmur3, fnv, fnva")
		maxSize             = flag.Int("max.size", 10000, "Maximum number of events per key")
		batchSize           = flag.Int("batch.size", 100, "keys to select per request")
		maxKeysPerSecond    = flag.Int64("max.keys.per.second", 1000, "max keys per second to walk")
		scanLogInterval     = flag.Duration("scan.log.interval", 5*time.Second, "how often to report scan rates in log")
		once                = flag.Bool("once", false, "walk entire keyspace once and exit (default false, walk forever)")
		statsdAddress       = flag.String("statsd.address", "", "Statsd address (blank to disable)")
		statsdSampleRate    = flag.Float64("statsd.sample.rate", 0.1, "Statsd sample rate for normal metrics")
		statsdBucketPrefix  = flag.String("statsd.bucket.prefix", "myservice.", "Statsd bucket key prefix, including trailing period")
		httpAddress         = flag.String("http.address", ":6060", "HTTP listen address (profiling/metrics endpoints only)")
	)
	flag.Parse()
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Lmicroseconds)

	// Validate integer arguments.
	if *maxKeysPerSecond < int64(*batchSize) {
		log.Fatal("max keys per second should be bigger than batch size")
	}

	// Set up instrumentation.
	statter := g2s.Noop()
	if *statsdAddress != "" {
		var err error
		statter, err = g2s.Dial("udp", *statsdAddress)
		if err != nil {
			log.Fatal(err)
		}
	}
	prometheusInstr := prometheus.New("")
	prometheusInstr.Install("/metrics", http.DefaultServeMux)
	instr := instrumentation.NewMultiInstrumentation(
		statsd.New(statter, float32(*statsdSampleRate), *statsdBucketPrefix),
		prometheusInstr,
	)

	// Parse hash function.
	var hashFunc func(string) uint32
	switch strings.ToLower(*redisHash) {
	case "murmur3":
		hashFunc = pool.Murmur3
	case "fnv":
		hashFunc = pool.FNV
	case "fnva":
		hashFunc = pool.FNVa
	default:
		log.Fatalf("unknown hash '%s'", *redisHash)
	}

	// Set up the clusters.
	clusters, err := makeClusters(
		*redisInstances,
		*redisConnectTimeout, *redisReadTimeout, *redisWriteTimeout,
		*redisMCPI,
		hashFunc,
		*maxSize,
		instr,
	)
	if err != nil {
		log.Fatal(err)
	}

	// HTTP server for profiling
	go func() { log.Print(http.ListenAndServe(*httpAddress, nil)) }()

	// Set up our rate limiter. Remember: it's per-key, not per-request.
	freq := time.Duration(1/(*maxKeysPerSecond)) * time.Second
	bucket := tb.NewBucket(*maxKeysPerSecond, freq)

	// Build the farm
	readStrategy := farm.SendAllReadAll
	repairStrategy := farm.AllRepairs // blocking
	dst := farm.New(clusters, len(clusters), readStrategy, repairStrategy, instr)

	// Perform the walk
	begin := time.Now()
	for {
		src := scan(clusters, *batchSize, *scanLogInterval) // new key set
		walkOnce(dst, bucket, src, *maxSize, instr)
		if *once {
			break
		}
	}
	log.Printf("walk complete in %s", time.Since(begin))
}

func makeClusters(
	redisInstances string,
	connectTimeout, readTimeout, writeTimeout time.Duration,
	redisMCPI int,
	hashFunc func(string) uint32,
	maxSize int,
	instr instrumentation.Instrumentation,
) ([]cluster.Cluster, error) {
	clusters := []cluster.Cluster{}
	for i, clusterInstances := range strings.Split(redisInstances, ";") {
		addresses := stripBlank(strings.Split(clusterInstances, ","))
		if len(addresses) <= 0 {
			continue
		}
		clusters = append(clusters, cluster.New(
			pool.New(
				addresses,
				connectTimeout, readTimeout, writeTimeout,
				redisMCPI,
				hashFunc,
			),
			maxSize,
			instr,
		))
		log.Printf("Redis cluster %d: %d instance(s)", i+1, len(addresses))
	}
	if len(clusters) <= 0 {
		return []cluster.Cluster{}, fmt.Errorf("no cluster(s)")
	}
	return clusters, nil
}

func scan(clusters []cluster.Cluster, batchSize int, logInterval time.Duration) <-chan []string {
	c := make(chan []string)
	go func() {
		defer close(c)
		for i, index := range rand.Perm(len(clusters)) {
			log.Printf("walking the keyspace of cluster index %d (%d/%d)", index, i+1, len(clusters))
			for batch := range clusters[index].Keys(batchSize) {
				c <- batch
				// log.Printf(
				// 	"scan: %d/%d, cluster index %d: forwarded batch of %d",
				// 	i+1, len(clusters), index,
				// 	len(batch),
				// )
			}
		}
	}()
	return c
}

func walkOnce(
	dst farm.Selecter,
	wait waiter,
	src <-chan []string,
	maxSize int,
	instr instrumentation.WalkInstrumentation,
) {
	for batch := range src {
		log.Printf("walk: received batch of %d, requesting tokens", len(batch))
		wait.Wait(int64(len(batch)))
		log.Printf("walk: received tokens, performing Select")
		dst.Select(batch, 0, maxSize)
		instr.WalkKeys(len(batch))
		log.Printf("walk: performed Select, waiting for next batch")
	}
}

func stripBlank(src []string) []string {
	dst := []string{}
	for _, s := range src {
		if s == "" {
			continue
		}
		dst = append(dst, s)
	}
	return dst
}

type waiter interface {
	Wait(int64) time.Duration
}
