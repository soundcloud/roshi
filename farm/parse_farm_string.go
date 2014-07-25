package farm

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/instrumentation"
	"github.com/soundcloud/roshi/pool"
)

// ParseFarmString parses a farm declaration string into a slice of clusters.
// A farm string is a semicolon-separated list of cluster strings. A cluster
// string is a comma-separated list of Redis instances. All whitespace is
// ignored.
//
// An example farm string is:
//
//  "foo1:6379, foo2:6379; bar1:6379, bar2:6379, bar3:6379, bar4:6379"
//
func ParseFarmString(
	farmString string,
	connectTimeout, readTimeout, writeTimeout time.Duration,
	redisMCPI int,
	hash func(string) uint32,
	maxSize int,
	selectGap time.Duration,
	instr instrumentation.Instrumentation,
) ([]cluster.Cluster, error) {
	var (
		seen     = map[string]int{}
		clusters = []cluster.Cluster{}
	)
	for i, clusterString := range strings.Split(stripWhitespace(farmString), ";") {
		hostPorts := []string{}
		for _, hostPort := range strings.Split(clusterString, ",") {
			if hostPort == "" {
				continue
			}
			toks := strings.Split(hostPort, ":")
			if len(toks) != 2 {
				return []cluster.Cluster{}, fmt.Errorf("invalid host-port %q", hostPort)
			}
			if _, err := strconv.ParseUint(toks[1], 10, 16); err != nil {
				return []cluster.Cluster{}, fmt.Errorf("invalid port %q in host-port %q (%s)", toks[1], hostPort, err)
			}
			seen[hostPort]++
			hostPorts = append(hostPorts, hostPort)
		}
		if len(hostPorts) <= 0 {
			return []cluster.Cluster{}, fmt.Errorf("empty cluster %d (%q)", i+1, clusterString)
		}
		clusters = append(clusters, cluster.New(
			pool.New(hostPorts, connectTimeout, readTimeout, writeTimeout, redisMCPI, hash),
			maxSize,
			selectGap,
			instr,
		))
		log.Printf("cluster %d: %d instance(s)", i+1, len(hostPorts))
	}

	if len(clusters) <= 0 {
		return []cluster.Cluster{}, fmt.Errorf("no clusters specified")
	}

	duplicates := []string{}
	for hostPort, count := range seen {
		if count > 1 {
			duplicates = append(duplicates, hostPort)
		}
	}
	if len(duplicates) > 0 {
		return []cluster.Cluster{}, fmt.Errorf("duplicate instances found: %s", strings.Join(duplicates, ", "))
	}

	return clusters, nil
}

func stripWhitespace(src string) string {
	var dst []rune
	for _, c := range src {
		switch c {
		case ' ', '\t', '\r', '\n':
			continue
		}
		dst = append(dst, c)
	}
	return string(dst)
}
