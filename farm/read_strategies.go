package farm

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
	"github.com/soundcloud/roshi/ratepolice"
)

// ReadStrategy generates a ReadStrategy for a given Farm. Different core
// read strategies can provide different QoS guarantees.
type ReadStrategy func(*Farm) coreReadStrategy

// coreReadStrategy encodes one method of satisfying the Select behavior.
type coreReadStrategy func(keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error)

// SendOneReadOne is the simplest (or most naïve) read strategy, and has the
// least impact on the network and underlying clusters. It forwards a single
// read request to a single randomly-chosen cluster, and waits for the
// complete response. It has no way to compute union- or difference-sets, and
// therefore performs no read-repair. A complete cluster failure is returned
// to the client as an error; otherwise, partial results are returned.
func SendOneReadOne(farm *Farm) coreReadStrategy {
	return func(keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error) {
		began := time.Now()
		go farm.instrumentation.SelectCall()
		defer func() { go farm.instrumentation.SelectDuration(time.Since(began)) }()

		response, errors := map[string][]common.KeyScoreMember{}, []string{}
		var firstResponseDuration time.Duration
		blockingBegan := time.Now()
		c := farm.clusters[rand.Intn(len(farm.clusters))].Select(keys, offset, limit)
		for e := range c {
			if firstResponseDuration == 0 {
				firstResponseDuration = time.Since(blockingBegan)
			}
			if e.Error != nil {
				errors = append(errors, e.Error.Error())
			}
			response[e.Key] = e.KeyScoreMembers // partial response OK
		}
		blockingDuration := time.Since(blockingBegan)

		go func(d time.Duration) {
			farm.instrumentation.SelectFirstResponseDuration(firstResponseDuration)
			farm.instrumentation.SelectBlockingDuration(blockingDuration)
			farm.instrumentation.SelectOverheadDuration(d - blockingDuration)
		}(time.Since(began))

		if len(errors) >= len(keys) {
			err := fmt.Errorf("complete failure (%s)", strings.Join(errors, "; "))
			return map[string][]common.KeyScoreMember{}, err
		}
		return response, nil // partial results are preferred
	}
}

// SendAllReadAll is the safest read strategy. It forwards the read request to
// all underlying clusters, waits for all responses, computes union- and
// difference-sets for read repair, and finally returns the union-set.
func SendAllReadAll(farm *Farm) coreReadStrategy {
	return func(keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error) {
		began := time.Now()
		go farm.instrumentation.SelectCall()
		defer func() { go farm.instrumentation.SelectDuration(time.Since(began)) }()

		// We'll combine all response elements into a single channel. When all
		// clusters have finished sending elements there, close it, so we can
		// have nice range semantics in our gather phase.
		elements := make(chan cluster.Element)
		wg := sync.WaitGroup{}
		wg.Add(len(farm.clusters))
		go func() { wg.Wait(); close(elements) }()

		blockingBegan := time.Now()
		scatterSelects(farm.clusters, keys, offset, limit, &wg, elements)

		// Gather all elements. An error implies some problem with the Redis
		// instance or the underlying cluster, and shouldn't trigger read
		// repair, so we don't include those elements in our responses map.
		// (We should only trigger read repair when we have successful
		// responses with inconsistent data.)
		var firstResponseDuration time.Duration
		responses := map[string][]tupleSet{}
		for e := range elements {
			if e.Error != nil {
				go farm.instrumentation.SelectPartialError()
				continue
			}
			if firstResponseDuration == 0 {
				firstResponseDuration = time.Since(blockingBegan)
			}
			responses[e.Key] = append(responses[e.Key], makeSet(e.KeyScoreMembers))
		}
		blockingDuration := time.Since(blockingBegan)

		// Compute union and difference sets for each key.
		response := map[string][]common.KeyScoreMember{}
		repairs := keyMemberSet{}
		for key, tupleSets := range responses {
			union, difference := unionDifference(tupleSets)
			response[key] = union.orderedLimitedSlice(limit)
			repairs.addMany(difference)
		}

		// Issue read repairs on the difference set.
		if len(repairs) > 0 {
			farm.repairer.requestRepair(repairs.slice())
		}

		// Kapow!
		go func() {
			farm.instrumentation.SelectFirstResponseDuration(firstResponseDuration)
			farm.instrumentation.SelectBlockingDuration(blockingDuration)
			farm.instrumentation.SelectOverheadDuration(time.Since(began) - blockingDuration)
		}()
		return response, nil
	}
}

// SendAllReadFirstLinger broadcasts the select request to all clusters, waits
// for the first non-error response, and returns it directly to the client.
//
// Before returning, SendAllReadFirstLinger spawns a goroutine to linger and
// collect responses from all the clusters. When all responses have been
// collected, SendAllReadFirstLinger will determine which keys should be sent
// to the repairer.
func SendAllReadFirstLinger(farm *Farm) coreReadStrategy {
	return SendVarReadFirstLinger(-1, -1, nil)(farm)
}

// SendVarReadFirstLinger is a refined version of
// SendAllReadFirstLinger. It works in the same way but reduces the
// requests to all clusters under certain circumstances.  If you pass
// in a RatePolice, it will request permission from it with
// thresholdKeysReadPerSec as targe rate. Obviously, you should use
// the same RatePolice as the one the farm is reporting its reads
// to. If the RatePolice determines that the target rate is already
// reached, this read strategy will not perform a "SendAll" style read
// but a "SendOne" style.  "SendOne" has two issues, though: First, no
// repairs will ever result from a "SendOne" read. Second, if the one
// cluster the read request is sent to is unable (or slow) to reply,
// the whole read will fail (or be delayed). The first issue is
// implicitly solved by SendVarReadFirstLinger because the baseline
// amount of "SendAll" reads effectively provides a probabilistic
// repair scheme. To solve the second issue, SendVarReadFirstLinger
// promotes any "SendOne" read to a "SendAll" read if it was
// unsuccessful to return any results within a time set by
// thresholdLatency.
//
// To never do an initial "SendAll", set thresholdKeysReadPerSec to 0
// (in which case only the promotion to "SendAll" after
// thresholdLatency has passed will ever create "SendAll" reads - and
// only those can trigger repairs!).  For unlimited
// thresholdKeysReadPerSec, set it to a negative number. To never
// promote "SendOne" to "SendAll", set thresholdLatency to a negative
// duration.
func SendVarReadFirstLinger(thresholdKeysReadPerSec int, thresholdLatency time.Duration, rp ratepolice.RatePolice) func(*Farm) coreReadStrategy {
	if rp == nil || thresholdKeysReadPerSec <= 0 {
		rp = ratepolice.NewNop()
	}
	return func(farm *Farm) coreReadStrategy {
		return func(keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error) {
			began := time.Now()
			go farm.instrumentation.SelectCall()

			var maySendAll bool
			switch {
			case thresholdKeysReadPerSec > 0:
				// Our key reads are already reported
				// to the rate police at this point,
				// so a permitted number of 0 or more
				// is OK.
				if rp.Request(thresholdKeysReadPerSec) >= 0 {
					maySendAll = true
				} else {
					maySendAll = false
				}
			case thresholdKeysReadPerSec == 0:
				maySendAll = false
			case thresholdKeysReadPerSec < 0:
				maySendAll = true
			}

			// We'll combine all response elements into a single channel. When
			// all clusters have finished sending elements there, close it, so
			// we can have nice range semantics in our linger phase.
			elements := make(chan cluster.Element)
			wg := sync.WaitGroup{}
			wg.Add(len(farm.clusters))
			go func() {
				// Note that we need a wg.Done signal for every cluster, even
				// if we didn't actually send to it!
				wg.Wait()
				close(elements)
			}()

			// Depending on maySendAll, pick either one random cluster or all
			// of them.
			var clustersUsed, clustersNotUsed []cluster.Cluster
			if maySendAll {
				clustersUsed = farm.clusters
				clustersNotUsed = []cluster.Cluster{}
			} else {
				i := rand.Intn(len(farm.clusters))
				clustersUsed = farm.clusters[i : i+1]
				clustersNotUsed = make([]cluster.Cluster, 0, len(farm.clusters)-1)
				clustersNotUsed = append(clustersNotUsed, farm.clusters[:i]...)
				clustersNotUsed = append(clustersNotUsed, farm.clusters[i+1:]...)
			}

			blockingBegan := time.Now()
			scatterSelects(clustersUsed, keys, offset, limit, &wg, elements)

			// remainingKeys keeps track of all keys for which we haven't
			// received any non-error responses yet.
			remainingKeys := make(map[string]bool, len(keys))
			for _, key := range keys {
				remainingKeys[key] = true
			}

			// If we are not permitted to SendAll, we need a timeout (after
			// which we will SendAll nevertheless).
			var timeout <-chan time.Time
			if !maySendAll && thresholdLatency >= 0 {
				timeout = time.After(thresholdLatency)
			}

			responses := map[string][]tupleSet{}
			var firstResponseDuration time.Duration

		loop:
			for {
				select {
				case e, ok := <-elements:
					if !ok {
						break loop // elements already closed, all Selects done.
					}
					if e.Error != nil {
						go farm.instrumentation.SelectPartialError()
						continue
						// It might appear tempting to immediately send a
						// Select to the unusedClusters once we run into an
						// error. However, it's probably better to wait until
						// thresholdLatency has passed (which should be a
						// short duration anyway and might have already
						// happened...) to gather all the keys for which we
						// need a SendAll first and then do them all in one
						// big Select.
					}
					if firstResponseDuration == 0 {
						firstResponseDuration = time.Since(blockingBegan)
					}
					responses[e.Key] = append(responses[e.Key], makeSet(e.KeyScoreMembers))
					delete(remainingKeys, e.Key)

				case <-timeout:
					// Promote to SendAll for remaining keys.
					go farm.instrumentation.SelectSendAllPromotion()
					maySendAll = true
					remainingKeysSlice := make([]string, 0, len(remainingKeys))
					for k := range remainingKeys {
						remainingKeysSlice = append(remainingKeysSlice, k)
					}
					scatterSelects(clustersNotUsed, remainingKeysSlice, offset, limit, &wg, elements)
					clustersUsed = farm.clusters
					clustersNotUsed = []cluster.Cluster{}
				}

				if len(remainingKeys) == 0 {
					// We got enough results to return our results.
					break loop
				}
			}
			blockingDuration := time.Since(blockingBegan)
			defer func() {
				go func() {
					duration := time.Since(began)
					farm.instrumentation.SelectDuration(duration)
					farm.instrumentation.SelectFirstResponseDuration(firstResponseDuration)
					farm.instrumentation.SelectBlockingDuration(blockingDuration)
					farm.instrumentation.SelectOverheadDuration(duration - blockingDuration)
				}()
			}()

			// If we are here, we either got at least one result for each key,
			// or all Select calls have finished but we still did not get at
			// least one result for each key. In either case, it's time to
			// return results.
			if len(responses) == 0 && len(remainingKeys) > 0 {
				// All Selects returned an error.
				return map[string][]common.KeyScoreMember{}, fmt.Errorf("complete failure")
			}

			response := map[string][]common.KeyScoreMember{}
			repairs := keyMemberSet{}
			for key, tupleSets := range responses {
				union, difference := unionDifference(tupleSets)
				response[key] = union.orderedLimitedSlice(limit)
				repairs.addMany(difference)
			}

			sentAllButIncomplete := len(remainingKeys) > 0
			sentOneGotEverything := !maySendAll
			if sentAllButIncomplete {
				// We already got all results but they are incomplete because
				// of errors. Partial results are still better than nothing,
				// so issue repairs as needed and return the partial results.
				if len(repairs) > 0 {
					farm.repairer.requestRepair(repairs.slice())
				}
				return response, nil
			}
			if sentOneGotEverything {
				// The WaitGroup expects len(farm.clusters) Done signals, but
				// so far we've only given 1. Give the rest.
				for _ = range clustersNotUsed {
					wg.Done()
				}
				return response, nil
			}

			// If we are here, we *might* still have Selects running. So start
			// a goroutine to "linger" and collect the remaining responses for
			// repairs before returning the results we have so far.
			go func() {
				for e := range elements {
					if e.Error != nil {
						go farm.instrumentation.SelectPartialError()
						continue
					}
					responses[e.Key] = append(responses[e.Key], makeSet(e.KeyScoreMembers))
				}
				for _, tupleSets := range responses {
					_, difference := unionDifference(tupleSets)
					repairs.addMany(difference)
				}
				if len(repairs) > 0 {
					farm.repairer.requestRepair(repairs.slice())
				}
			}()
			return response, nil
		}
	}
}

type responseTuple struct {
	response map[string][]common.KeyScoreMember
	err      error
}

func scatterSelects(
	clusters []cluster.Cluster,
	keys []string,
	offset int,
	limit int,
	wg *sync.WaitGroup,
	elements chan cluster.Element,
) {
	for _, c := range clusters {
		go func(c cluster.Cluster) {
			defer wg.Done()
			for e := range c.Select(keys, offset, limit) {
				elements <- e
			}
		}(c)
	}
}

func multiply(d time.Duration, f float32) time.Duration {
	ms := float32(d.Nanoseconds() / 1e6)
	ms *= f
	return time.Duration(ms) * time.Millisecond
}

func min(d1, d2 time.Duration) time.Duration {
	min := d1
	if d2 < d1 {
		min = d2
	}
	if min < 1*time.Nanosecond {
		min = 1 * time.Nanosecond
	}
	return min
}

func ksms2kms(ksms []common.KeyScoreMember) []keyMember {
	kms := make([]keyMember, len(ksms))
	for i := range ksms {
		kms[i] = keyMember{
			Key:    ksms[i].Key,
			Member: ksms[i].Member,
		}
	}
	return kms
}
