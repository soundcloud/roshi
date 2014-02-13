package farm

import (
	"sync"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
	"github.com/soundcloud/roshi/instrumentation"
	"github.com/tsenart/tb"
)

// RepairStrategy generates a core repair strategy for a specific set of
// Clusters.
type RepairStrategy func([]cluster.Cluster, instrumentation.RepairInstrumentation) coreRepairStrategy

// coreRepairStrategy encodes one way of performing repair requests.
type coreRepairStrategy func(kms []keyMember)

// keyMember is like a KeyScoreMember, just without score. It is used to pass
// "suspects" to the core repair strategy.
type keyMember struct {
	Key    string
	Member string
}

// AllRepairs is a RepairStrategy that does what you expect: actually issuing
// the repairs with 100% probability. You may want to use RateLimitedRepairs
// to cap the load to your clusters.
func AllRepairs(clusters []cluster.Cluster, instr instrumentation.RepairInstrumentation) coreRepairStrategy {
	return func(kms []keyMember) {
		go func() {
			instr.RepairCall()
			instr.RepairRequest(len(kms))
		}()

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
			if err := write(tuples); err == nil {
				go instr.RepairWriteSuccess(len(tuples))
			} else {
				go instr.RepairWriteFailure(len(tuples))
			}
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
func NoRepairs([]cluster.Cluster, instrumentation.RepairInstrumentation) coreRepairStrategy {
	return func([]keyMember) {}
}

// RateLimitedRepairs is a RepairStrategy generator, which limits the total
// number of repaired key-member tuples to maxKeysPerSecond. Pass a negative
// value to allow unlimited repair.
func RateLimitedRepairs(maxKeysPerSecond int) RepairStrategy {
	permits := permitter(allowAllPermitter{})
	if maxKeysPerSecond >= 0 {
		permits = permitter(tokenBucketPermitter{tb.NewBucket(int64(maxKeysPerSecond), -1)})
	}
	return func(clusters []cluster.Cluster, instr instrumentation.RepairInstrumentation) coreRepairStrategy {
		return func(kms []keyMember) {
			if n := len(kms); !permits.canHas(int64(n)) {
				instr.RepairDiscarded(n)
				return
			}
			AllRepairs(clusters, instr)(kms)
		}
	}
}
