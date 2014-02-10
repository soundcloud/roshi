// roshi-server provides a REST-y HTTP service to interact with a farm.
package main

import (
	"encoding/json"
	_ "expvar"
	"flag"
	"fmt"
	logpkg "log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
	"github.com/soundcloud/roshi/farm"
	"github.com/soundcloud/roshi/instrumentation/statsd"
	"github.com/soundcloud/roshi/shard"
	"github.com/peterbourgon/g2s"
	"github.com/streadway/handy/breaker"
	"github.com/gorilla/pat"
)

const (
	ratePoliceMovingAverageDuration = 5 * time.Second
	ratePoliceNumberOfBuckets       = 20
)

var (
	stats = g2s.Noop()
	log   = logpkg.New(os.Stdout, "", logpkg.Lmicroseconds)
)

func main() {
	var (
		redisInstances           = flag.String("redis.instances", "", "Semicolon-separated list of comma-separated lists of Redis instances")
		redisConnectTimeout      = flag.Duration("redis.connect.timeout", 3*time.Second, "Redis connect timeout")
		redisReadTimeout         = flag.Duration("redis.read.timeout", 3*time.Second, "Redis read timeout")
		redisWriteTimeout        = flag.Duration("redis.write.timeout", 3*time.Second, "Redis write timeout")
		redisMCPI                = flag.Int("redis.mcpi", 10, "Max connections per Redis instance")
		redisHash                = flag.String("redis.hash", "murmur3", "Redis hash function: murmur3, fnv, fnva")
		farmWriteQuorum          = flag.String("farm.write.quorum", "100%", "Write quorum, either number of clusters (2) or percentage of clusters (51%)")
		farmReadStrategy         = flag.String("farm.read.strategy", "SendAllReadAll", "Farm read strategy: SendAllReadAll, SendOneReadOne, SendAllReadFirstLinger, SendVarReadFirstLinger")
		farmReadThresholdRate    = flag.Int("farm.read.threshold.rate", 2000, "Baseline SendAll keys read per sec, additional keys are SendOne (SendVarReadFirstLinger strategy only)")
		farmReadThresholdLatency = flag.Duration("farm.read.threshold.latency", 50*time.Millisecond, "If a SendOne read has not returned anything after this latency, it's promoted to SendAll (SendVarReadFirstLinger strategy only)")
		farmRepairer             = flag.String("farm.repairer", "RateLimited", "Farm repairer: RateLimited, Nop")
		farmMaxRepairRate        = flag.Int("farm.repair.maxrate", 1000, "Max repairs per second (RateLimited repairer only)")
		farmMaxRepairBacklog     = flag.Int("farm.repair.maxbacklog", 1000000, "Max number of queued repairs (RateLimited repairer only)")
		maxSize                  = flag.Int("max.size", 10000, "Maximum number of events per key")
		statsdAddress            = flag.String("statsd.address", "", "Statsd address (blank to disable)")
		statsdSampleRate         = flag.Float64("statsd.sample.rate", 0.1, "Statsd sample rate for normal metrics")
		statsdBucketPrefix       = flag.String("statsd.bucket.prefix", "myservice.", "Statsd bucket key prefix, including trailing period")
		httpCircuitBreaker       = flag.Bool("http.circuit.breaker", true, "Enable HTTP server circuit breaker")
		httpAddress              = flag.String("http.address", ":6302", "HTTP listen address")
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
	switch strings.ToLower(*farmReadStrategy) {
	case "sendallreadall":
		readStrategy = farm.SendAllReadAll
	case "sendonereadone":
		readStrategy = farm.SendOneReadOne
	case "sendallreadfirstlinger":
		readStrategy = farm.SendAllReadFirstLinger
	case "sendvarreadfirstlinger":
		readStrategy = farm.SendVarReadFirstLinger(*farmReadThresholdRate, *farmReadThresholdLatency)
	default:
		log.Fatalf("unknown read strategy '%s'", *farmReadStrategy)
	}
	log.Printf("using %s read strategy", *farmReadStrategy)

	// Parse repairer.
	var repairer farm.Repairer
	switch strings.ToLower(*farmRepairer) {
	case "nop":
		repairer = farm.NopRepairer
	case "ratelimited":
		repairer = farm.RateLimitedRepairer(*farmMaxRepairRate, *farmMaxRepairBacklog)
	default:
		log.Fatalf("unknown repairer '%s'", *farmRepairer)
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
		*farmWriteQuorum,
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
	writeQuorumStr string,
	connectTimeout, readTimeout, writeTimeout time.Duration,
	redisMCPI int,
	hash func(string) uint32,
	readStrategy farm.ReadStrategy,
	repairer farm.Repairer,
	maxSize int,
	statsdSampleRate float64,
	bucketPrefix string,
) (*farm.Farm, error) {
	// Build instrumentation.
	instr := statsd.New(stats, float32(statsdSampleRate), bucketPrefix)

	// Parse out and build clusters.
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

	// Evaluate writeQuorum.
	writeQuorum, err := evaluateScalarPercentage(writeQuorumStr, len(clusters))
	if err != nil {
		return nil, err
	}

	// Build and return Farm.
	return farm.New(
		clusters,
		writeQuorum,
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

// evaluateScalarPercentage takes a string of the form "P%" (percent) or "S"
// (straight scalar value), and evaluates that against the passed total n.
// Percentages mean at least that percent; for example, "50%" of 3 evaluates
// to 2. It is an error if the passed string evaluates to less than 1 or more
// than n.
func evaluateScalarPercentage(s string, n int) (int, error) {
	if n <= 0 {
		return -1, fmt.Errorf("n must be at least 1")
	}

	s = strings.TrimSpace(s)
	var value int
	if strings.HasSuffix(s, "%") {
		percentInt, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
		if err != nil || percentInt <= 0 || percentInt > 100 {
			return -1, fmt.Errorf("bad percentage input '%s'", s)
		}
		value = int(math.Ceil((float64(percentInt) / 100.0) * float64(n)))
	} else {
		value64, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return -1, fmt.Errorf("bad scalar input '%s'", s)
		}
		value = int(value64)
	}
	if value <= 0 || value > n {
		return -1, fmt.Errorf("with n=%d, value=%d (from '%s') is invalid", n, value, s)
	}
	return value, nil
}
