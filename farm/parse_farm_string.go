package farm

import (
	"fmt"
	"strings"
	"time"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/instrumentation"
	"github.com/soundcloud/roshi/pool"
)

// ParseFarmString parses a farm declaration string into slices of read- and
// write-clusters. A farm string is a semicolon-separated list of cluster
// strings. A cluster string is a comma-separated list of Redis instances.
// All whitespace is ignored.
//
// The special sentinel string "writeonly" (case insensitive) may occur in
// place of a Redis instance. If it's present, that cluster will receive
// writes and be counted toward the quorum, but not service any reads. This
// has an impact on data safety guarantees, and is intended only as a
// maintenance mode for changing the size of a farm.
//
// An example farm string is:
//
//  "foo1:6379,foo2:6379; bar1:6379,bar2:6379,bar3:6379,bar4:6379,WRITEONLY"
//
func ParseFarmString(
	farmString string,
	connectTimeout, readTimeout, writeTimeout time.Duration,
	redisMCPI int,
	hash func(string) uint32,
	maxSize int,
	selectGap time.Duration,
	instr instrumentation.Instrumentation,
) (writeClusters, readClusters []cluster.Cluster, err error) {
	seen := map[string]int{}

	for i, clusterString := range strings.Split(stripWhitespace(farmString), ";") {
		var (
			hostPorts = []string{}
			writeOnly = false
		)
		for _, hostPort := range strings.Split(clusterString, ",") {
			if hostPort == "" {
				continue
			}
			if strings.ToLower(hostPort) == "writeonly" {
				writeOnly = true
				continue
			}
			seen[hostPort]++
			hostPorts = append(hostPorts, hostPort)
		}
		if len(hostPorts) <= 0 {
			return []cluster.Cluster{}, []cluster.Cluster{}, fmt.Errorf("empty cluster %d (%q)", i+1, clusterString)
		}

		cluster := cluster.New(
			pool.New(hostPorts, connectTimeout, readTimeout, writeTimeout, redisMCPI, hash),
			maxSize,
			selectGap,
			instr,
		)

		writeClusters = append(writeClusters, cluster)
		if !writeOnly {
			readClusters = append(readClusters, cluster)
		}
	}

	duplicates := []string{}
	for hostPort, count := range seen {
		if count > 1 {
			duplicates = append(duplicates, hostPort)
		}
	}
	if len(duplicates) > 0 {
		return []cluster.Cluster{}, []cluster.Cluster{}, fmt.Errorf("duplicate instances found: %s", strings.Join(duplicates, ", "))
	}

	if len(writeClusters) <= 0 && len(readClusters) <= 0 {
		return []cluster.Cluster{}, []cluster.Cluster{}, fmt.Errorf("no clusters specified")
	}

	if len(writeClusters) < len(readClusters) {
		return []cluster.Cluster{}, []cluster.Cluster{}, fmt.Errorf(
			"fewer write clusters (%d) than read clusters (%d)",
			len(writeClusters),
			len(readClusters),
		)
	}

	return writeClusters, readClusters, nil
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
