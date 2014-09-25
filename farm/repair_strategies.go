package farm

import (
	"log"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
	"github.com/soundcloud/roshi/instrumentation"

	"github.com/tsenart/tb"
)

// RepairStrategy generates a core repair strategy for a specific set of
// Clusters.
type RepairStrategy func([]cluster.Cluster, instrumentation.RepairInstrumentation) coreRepairStrategy

// coreRepairStrategy encodes one way of performing repair requests.
type coreRepairStrategy func(kms []common.KeyMember)

// Nonblocking wraps a RepairStrategy with a buffer of the given size. Repair
// requests are queued until the buffer is full, and then dropped.
//
// Nonblocking keeps read strategies responsive, while bounding process memory
// usage.
func Nonblocking(bufferSize int, repairStrategy RepairStrategy) RepairStrategy {
	return func(clusters []cluster.Cluster, instr instrumentation.RepairInstrumentation) coreRepairStrategy {
		c := make(chan []common.KeyMember, bufferSize)
		go func() {
			for kms := range c {
				repairStrategy(clusters, instr)(kms)
			}
		}()

		return func(kms []common.KeyMember) {
			select {
			case c <- kms:
				break
			default:
				log.Printf("Nonblocking repairs: request buffer full; repair request discarded")
				go instr.RepairDiscarded(len(kms))
			}
		}
	}
}

// RateLimited wraps a repair strategy with rate limit. Repair requests that
// would cause the instantaneous number of elements (score-members) per second
// to exceed the passed limit are dropped.
//
// RateLimited keeps read strategies responsive, while bounding the load
// applied to your infrastructure.
func RateLimited(maxElementsPerSecond int, repairStrategy RepairStrategy) RepairStrategy {
	return func(clusters []cluster.Cluster, instr instrumentation.RepairInstrumentation) coreRepairStrategy {
		permits := permitter(allowAllPermitter{})
		if maxElementsPerSecond >= 0 {
			permits = tokenBucketPermitter{tb.NewBucket(int64(maxElementsPerSecond), -1)}
		}

		return func(kms []common.KeyMember) {
			if n := len(kms); !permits.canHas(int64(n)) {
				log.Printf("RateLimited repairs: element rate exceeded; repair request discarded")
				instr.RepairDiscarded(n)
				return
			}
			repairStrategy(clusters, instr)(kms)
		}
	}
}

// NoRepairs is a no-op repair strategy.
func NoRepairs([]cluster.Cluster, instrumentation.RepairInstrumentation) coreRepairStrategy {
	return func([]common.KeyMember) {}
}

// AllRepairs is repair strategy that does what you expect: actually issue
// repairs with 100% probability.
//
// You may want to wrap AllRepairs with Nonblocking and/or RateLimited to
// control memory pressure in your process and/or load against your
// infrastructure, respectively.
func AllRepairs(clusters []cluster.Cluster, instr instrumentation.RepairInstrumentation) coreRepairStrategy {
	return func(keyMembers []common.KeyMember) {
		go func() {
			instr.RepairCall()
			instr.RepairRequest(len(keyMembers))
		}()

		// Every KeyMember has a presence in every cluster. Even if the
		// cluster errors during Score, we keep a default (empty) presence.
		// That means we may re-issue unnecessary writes, but that's OK!
		presenceMap := map[common.KeyMember][]cluster.Presence{}
		for _, keyMember := range keyMembers {
			presenceMap[keyMember] = make([]cluster.Presence, len(clusters))
		}

		// Make Score requests sequentially. If a key is totally missing from
		// a cluster, like when a node comes online empty and needs to be
		// rebuilt, you'll end up asking about maxSize KeyMembers, which is
		// probably a lot.
		for index := range clusters {
			// Make single request for this cluster.
			scoreResponse, err := clusters[index].Score(keyMembers)
			if err != nil {
				log.Printf("AllRepairs: cluster %d: %s", index, err)
				continue
			}

			// Copy this cluster's presence information into our map.
			for keyMember, presence := range scoreResponse {
				presenceMap[keyMember][index] = presence
			}
		}

		// With the collected responses, determine the correct state, and
		// schedule write operations.
		inserts := map[int][]common.KeyScoreMember{}
		deletes := map[int][]common.KeyScoreMember{}
		for keyMember, presenceSlice := range presenceMap {
			// Walk once, to determine the correct state.
			var (
				found        = false
				highestScore = 0.
				wasInserted  = false
			)

			for _, presence := range presenceSlice {
				if presence.Present && presence.Score >= highestScore {
					found = true
					highestScore = presence.Score
					wasInserted = wasInserted || presence.Inserted // https://github.com/soundcloud/roshi/issues/24
				}
			}

			if !found {
				// This is indeed a strange situation, but it can arise if we
				// get errors from every cluster during Score requests, for
				// example. We don't want to confuse that with presence in the
				// remove set.
				log.Printf("AllRepairs: %v not found anywhere, skipping", keyMember)
				continue
			}

			// We now know the correct element.
			keyScoreMember := common.KeyScoreMember{
				Key:    keyMember.Key,
				Score:  highestScore,
				Member: keyMember.Member,
			}

			// Walk again, to schedule write operations.
			for index, presence := range presenceSlice {
				var (
					notThere = !presence.Present
					lowScore = presence.Score < highestScore
					wrongSet = presence.Inserted != wasInserted
				)

				if notThere || lowScore || wrongSet {
					if wasInserted {
						inserts[index] = append(inserts[index], keyScoreMember)
					} else {
						deletes[index] = append(deletes[index], keyScoreMember)
					}
				}
			}
		}

		// Make write operations.

		for index, keyScoreMembers := range inserts {
			if err := clusters[index].Insert(keyScoreMembers); err != nil {
				log.Printf("AllRepairs: cluster %d: during Insert: %s", index, err)
			}
		}

		for index, keyScoreMembers := range deletes {
			if err := clusters[index].Delete(keyScoreMembers); err != nil {
				log.Printf("AllRepairs: cluster %d: during Delete: %s", index, err)
			}
		}
	}
}

type permitter interface {
	canHas(n int64) bool
}

type tokenBucketPermitter struct{ *tb.Bucket }

func (p tokenBucketPermitter) canHas(n int64) bool {
	if got := p.Bucket.Take(n); got < n {
		p.Bucket.Put(got)
		return false
	}
	return true
}

type allowAllPermitter struct{}

func (p allowAllPermitter) canHas(n int64) bool { return true }
