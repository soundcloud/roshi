// roshi-server provides a REST-y HTTP service to interact with a farm.
package main

import (
	"encoding/json"
	_ "expvar"
	"flag"
	"fmt"
	logpkg "log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/pat"
	"github.com/peterbourgon/g2s"
	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
	"github.com/soundcloud/roshi/farm"
	"github.com/soundcloud/roshi/instrumentation/statsd"
	"github.com/soundcloud/roshi/shard"
	"github.com/streadway/handy/breaker"
)

var (
	stats = g2s.Noop()
	log   = logpkg.New(os.Stdout, "", logpkg.Lmicroseconds)
)

func main() {
	var (
		redisInstances            = flag.String("redis.instances", "", "Semicolon-separated list of comma-separated lists of Redis instances")
		redisConnectTimeout       = flag.Duration("redis.connect.timeout", 3*time.Second, "Redis connect timeout")
		redisReadTimeout          = flag.Duration("redis.read.timeout", 3*time.Second, "Redis read timeout")
		redisWriteTimeout         = flag.Duration("redis.write.timeout", 3*time.Second, "Redis write timeout")
		redisMCPI                 = flag.Int("redis.mcpi", 10, "Max connections per Redis instance")
		redisHash                 = flag.String("redis.hash", "murmur3", "Redis hash function: murmur3, fnv, fnva")
		redisReadStrategy         = flag.String("redis.read.strategy", "SendAllReadAll", "Redis read strategy: SendAllReadAll, SendOneReadOne, SendAllReadFirstLinger, SendVarReadFirstLinger")
		redisReadThresholdRate    = flag.Int("redis.read.threshold.rate", 10, "Baseline SendAll reads per sec, additional reads are SendOne (SendVarReadFirstLinger strategy only)")
		redisReadThresholdLatency = flag.Duration("redis.read.threshold.latency", 50*time.Millisecond, "If a SendOne read has not returned anything after this latency, it's promoted to SendAll (SendVarReadFirstLinger strategy only)")
		redisRepairer             = flag.String("redis.repairer", "RateLimited", "Redis repairer: RateLimited, Nop")
		redisMaxRepairRate        = flag.Int("redis.repair.maxrate", 10, "Max repairs per second (RateLimited repairer only)")
		redisMaxRepairBacklog     = flag.Int("redis.repair.maxbacklog", 100000, "Max number of queued repairs (RateLimited repairer only)")
		maxSize                   = flag.Int("max.size", 10000, "Maximum number of events per key")
		statsdAddress             = flag.String("statsd.address", "", "Statsd address (blank to disable)")
		statsdSampleRate          = flag.Float64("statsd.sample.rate", 0.1, "Statsd sample rate for normal metrics")
		statsdBucketPrefix        = flag.String("statsd.bucket.prefix", "myservice.", "Statsd bucket key prefix, including trailing period")
		httpCircuitBreaker        = flag.Bool("http.circuit.breaker", true, "Enable HTTP server circuit breaker")
		httpAddress               = flag.String("http.address", ":6301", "HTTP listen address")
	)
	flag.Parse()
	log.Printf("GOMAXPROCS %d", runtime.GOMAXPROCS(-1))

	// Set up statsd instrumentation, if it's specified.
	if *statsdAddress != "" {
		var err error
		stats, err = g2s.Dial("udp", *statsdAddress)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Parse read strategy.
	var readStrategy farm.ReadStrategy
	switch strings.ToLower(*redisReadStrategy) {
	case "sendallreadall":
		readStrategy = farm.SendAllReadAll
	case "sendonereadone":
		readStrategy = farm.SendOneReadOne
	case "sendallreadfirstlinger":
		readStrategy = farm.SendAllReadFirstLinger
	case "sendvarreadfirstlinger":
		readStrategy = farm.SendVarReadFirstLinger(*redisReadThresholdRate, *redisReadThresholdLatency)
	default:
		log.Fatalf("unknown read strategy '%s'", *redisReadStrategy)
	}
	log.Printf("using %s read strategy", *redisReadStrategy)

	// Parse repairer.
	var repairer farm.Repairer
	switch strings.ToLower(*redisRepairer) {
	case "nop":
		repairer = farm.NopRepairer
	case "ratelimited":
		repairer = farm.RateLimitedRepairer(*redisMaxRepairRate, *redisMaxRepairBacklog)
	default:
		log.Fatalf("unknown repairer '%s'", *redisRepairer)
	}

	// Parse hash function.
	var hashFunc func(string) uint32
	switch strings.ToLower(*redisHash) {
	case "murmur3":
		hashFunc = shard.Murmur3
	case "fnv":
		hashFunc = shard.FNV
	case "fnva":
		hashFunc = shard.FNVa
	default:
		log.Fatalf("unknown hash '%s'", *redisHash)
	}

	// Build the farm.
	farm, err := newFarm(
		*redisInstances,
		*redisConnectTimeout, *redisReadTimeout, *redisWriteTimeout,
		*redisMCPI,
		hashFunc,
		readStrategy,
		repairer,
		*maxSize,
		*statsdSampleRate,
		*statsdBucketPrefix,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Build the HTTP server.
	r := pat.New()
	r.Add("GET", "/debug", http.DefaultServeMux)
	r.Get("/", handleSelect(farm))
	r.Post("/", handleInsert(farm))
	r.Delete("/", handleDelete(farm))
	h := http.Handler(r)
	if *httpCircuitBreaker {
		log.Printf("using HTTP circuit breaker")
		h = breaker.DefaultBreaker(h)
	}

	// Go for it.
	log.Printf("listening on %s", *httpAddress)
	log.Fatal(http.ListenAndServe(*httpAddress, h))
}

func newFarm(
	redisInstances string,
	connectTimeout, readTimeout, writeTimeout time.Duration,
	redisMCPI int,
	hash func(string) uint32,
	readStrategy farm.ReadStrategy,
	repairer farm.Repairer,
	maxSize int,
	statsdSampleRate float64,
	bucketPrefix string,
) (*farm.Farm, error) {
	instr := statsd.New(stats, float32(statsdSampleRate), bucketPrefix)

	clusters := []cluster.Cluster{}
	for i, clusterInstances := range strings.Split(redisInstances, ";") {
		addresses := stripBlank(strings.Split(clusterInstances, ","))
		if len(addresses) <= 0 {
			continue
		}
		clusters = append(clusters, cluster.New(
			shard.New(
				addresses,
				connectTimeout, readTimeout, writeTimeout,
				redisMCPI,
				hash,
			),
			maxSize,
			instr,
		))
		log.Printf("Redis cluster %d: %d instance(s)", i+1, len(addresses))
	}
	if len(clusters) <= 0 {
		return nil, fmt.Errorf("no cluster(s)")
	}

	return farm.New(
		clusters,
		readStrategy,
		repairer,
		instr,
	), nil
}

func handleSelect(selecter farm.Selecter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		began := time.Now()

		if err := r.ParseForm(); err != nil {
			respondError(w, r.Method, r.URL.String(), http.StatusInternalServerError, err)
			return
		}
		offset := parseInt(r.Form, "offset", 0)
		limit := parseInt(r.Form, "limit", 10)
		coalesce := parseBool(r.Form, "coalesce", false)

		var keys [][]byte
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&keys); err != nil {
			respondError(w, r.Method, r.URL.String(), http.StatusBadRequest, err)
			return
		}

		keyStrings := make([]string, len(keys))
		for i := range keys {
			keyStrings[i] = string(keys[i])
		}

		var records interface{}
		if coalesce {
			// We need to Select from 0 to offset+limit, flatten the map to a
			// single ordered slice, and then cut off the last limit elements.
			m, err := selecter.Select(keyStrings, 0, offset+limit)
			if err != nil {
				respondError(w, r.Method, r.URL.String(), http.StatusInternalServerError, err)
				return
			}
			records = flatten(m, offset, limit)
		} else {
			// We can directly Select using the given offset and limit.
			m, err := selecter.Select(keyStrings, offset, limit)
			if err != nil {
				respondError(w, r.Method, r.URL.String(), http.StatusInternalServerError, err)
				return
			}
			records = m
		}

		respondSelected(w, keys, offset, limit, records, time.Since(began))
	}
}

func handleInsert(inserter cluster.Inserter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		began := time.Now()

		var tuples []common.KeyScoreMember
		if err := json.NewDecoder(r.Body).Decode(&tuples); err != nil {
			respondError(w, r.Method, r.URL.String(), http.StatusBadRequest, err)
			return
		}

		if err := inserter.Insert(tuples); err != nil {
			respondError(w, r.Method, r.URL.String(), http.StatusInternalServerError, err)
			return
		}

		respondInserted(w, len(tuples), time.Since(began))
	}
}

func handleDelete(deleter cluster.Deleter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		began := time.Now()

		var tuples []common.KeyScoreMember
		if err := json.NewDecoder(r.Body).Decode(&tuples); err != nil {
			respondError(w, r.Method, r.URL.String(), http.StatusBadRequest, err)
			return
		}

		if err := deleter.Delete(tuples); err != nil {
			respondError(w, r.Method, r.URL.String(), http.StatusInternalServerError, err)
			return
		}

		respondDeleted(w, len(tuples), time.Since(began))
	}
}

func flatten(m map[string][]common.KeyScoreMember, offset, limit int) []common.KeyScoreMember {
	a := common.KeyScoreMembers{}
	for _, tuples := range m {
		a = append(a, tuples...)
	}

	sort.Sort(a)

	if len(a) < offset {
		return []common.KeyScoreMember{}
	}
	a = a[offset:]

	if len(a) > limit {
		a = a[:limit]
	}

	return a
}

func parseInt(values url.Values, key string, defaultValue int) int {
	value, err := strconv.ParseInt(values.Get(key), 10, 64)
	if err != nil {
		return defaultValue
	}
	return int(value)
}

func parseBool(values url.Values, key string, defaultValue bool) bool {
	value, err := strconv.ParseBool(values.Get(key))
	if err != nil {
		return defaultValue
	}
	return value
}

func respondInserted(w http.ResponseWriter, n int, duration time.Duration) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"inserted": n,
		"duration": duration.String(),
	})
}

func respondSelected(w http.ResponseWriter, keys [][]byte, offset, limit int, records interface{}, duration time.Duration) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"keys":     keys,
		"offset":   offset,
		"limit":    limit,
		"records":  records,
		"duration": duration.String(),
	})
}

func respondDeleted(w http.ResponseWriter, n int, duration time.Duration) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"deleted":  n,
		"duration": duration.String(),
	})
}

func respondError(w http.ResponseWriter, method, url string, code int, err error) {
	log.Printf("%s %s: HTTP %d: %s", method, url, code, err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":       err.Error(),
		"code":        code,
		"description": http.StatusText(code),
	})
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

type writer struct{ *logpkg.Logger }

func (w writer) Write(p []byte) (int, error) { w.Print(string(p)); return len(p), nil }
