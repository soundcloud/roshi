package farm

import (
	"sync"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
	"github.com/soundcloud/roshi/instrumentation"
	"github.com/tsenart/tb"
)

// repairStrategy denotes one method for processing repair requests. Repair
// strategies probably need to close over a slice of clusters and a repair
// instrumentation.  Different repair strategies can provide different e.g.
// load guarantees.
type repairStrategy func(kms []keyMember)

// keyMember is like a KeyScoreMember, just without score. It is used to pass
// "suspects" to the core repair strategy.
type keyMember struct {
	Key    string
	Member string
}

// AllRepairs is the repair strategy that does what you expect: actually
// issuing the repairs with 100% probability. You may want to use
// RateLimitedRepairs to cap the load to your clusters.
func AllRepairs(clusters []cluster.Cluster, instr instrumentation.RepairInstrumentation) repairStrategy {
	return func(kms []keyMember) {
		type scoreTuple struct {
			score       float64
			wasInserted bool
			err         error
		}

		type responseTuple struct {
			clusterIndex int
			keyMember
			scoreTuple
		}

		responses := make(chan responseTuple, len(kms)*len(clusters))

		// Scatter Score requests.
		for _, km := range kms {
			for i, c := range clusters {
				go func(km keyMember, i int, c cluster.Cluster) {
					score, wasInserted, err := c.Score(km.Key, km.Member)
					t := scoreTuple{score: score, wasInserted: wasInserted, err: err}
					responses <- responseTuple{clusterIndex: i, keyMember: km, scoreTuple: t}
				}(km, i, c)
			}
		}

		gathered := map[keyMember][]scoreTuple{}

		// Gather Score responses.
		for i := 0; i < cap(responses); i++ {
			resp := <-responses
			if _, ok := gathered[resp.keyMember]; !ok {
				gathered[resp.keyMember] = make([]scoreTuple, len(clusters))
			}
			gathered[resp.keyMember][resp.clusterIndex] = resp.scoreTuple
		}

		type repair struct {
			common.KeyScoreMember
		}

		inserts := map[int][]common.KeyScoreMember{} // cluster index: KeyScoreMembers to Insert
		deletes := map[int][]common.KeyScoreMember{} // cluster index: KeyScoreMembers to Delete

		// Determine which clusters require repairs.
		for km, scoreTuples := range gathered {
			// Walk once, to determine the correct data.
			highestScore, wasInserted := 0., false
			for _, scoreTuple := range scoreTuples {
				if scoreTuple.score > highestScore {
					highestScore = scoreTuple.score
					wasInserted = scoreTuple.wasInserted
				}
			}

			// Choose the correct target write operation for this keyMember.
			target := inserts
			if !wasInserted {
				target = deletes
			}

			// Walk again, to schedule any necessary repairs.
			for clusterIndex, scoreTuple := range scoreTuples {
				if scoreTuple.score < highestScore || scoreTuple.wasInserted != wasInserted {
					target[clusterIndex] = append(target[clusterIndex], common.KeyScoreMember{
						Key:    km.Key,
						Score:  highestScore,
						Member: km.Member,
					})
				}
			}
		}

		var wg sync.WaitGroup
		wg.Add(len(inserts) + len(deletes))
		do := func(write func([]common.KeyScoreMember) error, tuples []common.KeyScoreMember) {
			write(tuples)
			wg.Done()
		}

		// Launch repairs.
		for clusterIndex, tuples := range inserts {
			write := clusters[clusterIndex].Insert
			tuples := tuples
			go do(write, tuples)
		}
		for clusterIndex, tuples := range deletes {
			write := clusters[clusterIndex].Delete
			tuples := tuples
			go do(write, tuples)
		}

		wg.Wait()
	}
}

// NoRepairs is a no-op repair strategy.
func NoRepairs([]keyMember) {}

// RateLimitedRepairs is a repair strategy which limits the total number of
// repaired key-member tuples to maxKeysPerSecond. Pass a negative value to
// allow unlimited repair.
func RateLimitedRepairs(clusters []cluster.Cluster, instr instrumentation.RepairInstrumentation, maxKeysPerSecond int) repairStrategy {
	permits := permitter(allowAllPermitter{})
	if maxKeysPerSecond >= 0 {
		permits = permitter(tokenBucketPermitter{tb.NewBucket(int64(maxKeysPerSecond), 1)})
	}
	return func(kms []keyMember) {
		if n := len(kms); !permits.canHas(int64(n)) {
			instr.RepairDiscarded(n)
			return
		}
		AllRepairs(clusters, instr)(kms)
	}
}
