package farm

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/tsenart/tb"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
)

// ReadStrategy is a function that yields a farm.Selecter with a specific
// behavior over a given Farm.
type ReadStrategy func(*Farm) Selecter

// SendOneReadOne is a ReadStrategy that chooses a random cluster, sends the
// read request exclusively there, and  returns whatever result comes back.
// It's the simplest read strategy, and has the least impact on the network,
// but isn't resilient to stale data.
func SendOneReadOne(farm *Farm) Selecter { return sendOneReadOne{farm} }

type sendOneReadOne struct{ *Farm }

// SelectOffset implements farm.Selecter.
func (s sendOneReadOne) SelectOffset(keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error) {
	return s.read(len(keys), func(c cluster.Cluster) <-chan cluster.Element {
		return c.SelectOffset(keys, offset, limit)
	})
}

// SelectRange implements farm.Selecter.
func (s sendOneReadOne) SelectRange(keys []string, start, stop common.Cursor, limit int) (map[string][]common.KeyScoreMember, error) {
	return s.read(len(keys), func(c cluster.Cluster) <-chan cluster.Element {
		return c.SelectRange(keys, start, stop, limit)
	})
}

func (s sendOneReadOne) read(numKeys int, fn func(cluster.Cluster) <-chan cluster.Element) (map[string][]common.KeyScoreMember, error) {
	began := time.Now()
	go func() {
		s.Farm.instrumentation.SelectCall()
		s.Farm.instrumentation.SelectKeys(numKeys)
		s.Farm.instrumentation.SelectSendTo(1)
	}()
	defer func() { go s.Farm.instrumentation.SelectDuration(time.Since(began)) }()

	var (
		firstResponseDuration time.Duration

		blockingBegan = time.Now()
		retrieved     = 0
		response      = map[string][]common.KeyScoreMember{}
		errors        = []string{}
	)
	for e := range fn(s.Farm.clusters[rand.Intn(len(s.Farm.clusters))]) {
		if firstResponseDuration == 0 {
			firstResponseDuration = time.Since(blockingBegan)
		}
		if e.Error != nil {
			errors = append(errors, e.Error.Error())
		}
		retrieved += len(e.KeyScoreMembers)
		response[e.Key] = e.KeyScoreMembers // partial response OK
	}
	blockingDuration := time.Since(blockingBegan)

	go func(d time.Duration) {
		s.Farm.instrumentation.SelectFirstResponseDuration(firstResponseDuration)
		s.Farm.instrumentation.SelectBlockingDuration(blockingDuration)
		s.Farm.instrumentation.SelectOverheadDuration(d - blockingDuration)
		s.Farm.instrumentation.SelectRetrieved(retrieved)
		s.Farm.instrumentation.SelectReturned(retrieved) // for this strategy, retrieved == returned
	}(time.Since(began))

	if len(errors) >= numKeys {
		return map[string][]common.KeyScoreMember{}, fmt.Errorf("complete failure (%s)", strings.Join(errors, "; "))
	}
	return response, nil // partial results are preferred
}

// SendAllReadAll is a ReadStrategy that broadcasts the read request to all
// clusters, waits for all responses, and performs set union/difference on the
// result sets. It's a simple read strategy, which has the greatest impact on
// the network, but is also the most resilient to stale data.
func SendAllReadAll(farm *Farm) Selecter { return sendAllReadAll{farm} }

type sendAllReadAll struct{ *Farm }

// SelectOffset implements farm.Selecter.
func (s sendAllReadAll) SelectOffset(keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error) {
	return s.read(len(keys), func(c cluster.Cluster) <-chan cluster.Element {
		return c.SelectOffset(keys, offset, limit)
	}, limit)
}

// SelectRange implements farm.Selecter.
func (s sendAllReadAll) SelectRange(keys []string, start, stop common.Cursor, limit int) (map[string][]common.KeyScoreMember, error) {
	return s.read(len(keys), func(c cluster.Cluster) <-chan cluster.Element {
		return c.SelectRange(keys, start, stop, limit)
	}, limit)
}

func (s sendAllReadAll) read(numKeys int, fn func(cluster.Cluster) <-chan cluster.Element, limit int) (map[string][]common.KeyScoreMember, error) {
	began := time.Now()
	go func() {
		s.Farm.instrumentation.SelectCall()
		s.Farm.instrumentation.SelectKeys(numKeys)
		s.Farm.instrumentation.SelectSendTo(len(s.Farm.clusters))
	}()
	defer func() { go s.Farm.instrumentation.SelectDuration(time.Since(began)) }()

	// We'll combine all response elements into a single channel. When all
	// clusters have finished sending elements there, close it, so we can
	// have nice range semantics in our gather phase.
	elements := make(chan cluster.Element)
	wg := sync.WaitGroup{}
	wg.Add(len(s.Farm.clusters))
	go func() { wg.Wait(); close(elements) }()

	blockingBegan := time.Now()
	scatterSelects(s.Farm.clusters, fn, &wg, elements)

	// Gather all elements. An error implies some problem with the Redis
	// instance or the underlying cluster, and shouldn't trigger read
	// repair, so we don't include those elements in our responses map.
	// (We should only trigger read repair when we have successful
	// responses with inconsistent data.)
	var (
		firstResponseDuration time.Duration
		responses             = map[string][]tupleSet{}
		retrieved             = 0
	)
	for e := range elements {
		if e.Error != nil {
			log.Printf("SendAllReadAll partial error: %s", e.Error)
			go s.Farm.instrumentation.SelectPartialError()
			continue
		}
		if firstResponseDuration == 0 {
			firstResponseDuration = time.Since(blockingBegan)
		}
		responses[e.Key] = append(responses[e.Key], makeSet(e.KeyScoreMembers))
		retrieved += len(e.KeyScoreMembers)
	}
	blockingDuration := time.Since(blockingBegan)

	// Compute union and difference sets for each key.
	var (
		response = map[string][]common.KeyScoreMember{}
		repairs  = keyMemberSet{}
		returned = 0
	)
	for key, tupleSets := range responses {
		union, difference := unionDifference(tupleSets)
		response[key] = union.orderedLimitedSlice(limit)
		returned += len(response[key])
		repairs.addMany(difference)
	}

	// Issue read repairs on the difference set. Note that repairs are by
	// default blocking. If you want nonblocking repairs, as you probably
	// do in a production server, be sure to wrap your RepairStrategy in
	// Nonblocking!
	if len(repairs) > 0 {
		s.Farm.instrumentation.SelectRepairNeeded(len(repairs))
		s.Farm.repairStrategy(repairs.slice())
	}

	// Kapow!
	go func() {
		s.Farm.instrumentation.SelectFirstResponseDuration(firstResponseDuration)
		s.Farm.instrumentation.SelectBlockingDuration(blockingDuration)
		s.Farm.instrumentation.SelectOverheadDuration(time.Since(began) - blockingDuration)
		s.Farm.instrumentation.SelectRetrieved(retrieved)
		s.Farm.instrumentation.SelectReturned(returned)
	}()
	return response, nil
}

// SendAllReadFirstLinger is a ReadStrategy that broadcasts the read request
// to all clusters, waits for the first non-error response, and returns it
// directly to the client.
//
// Before returning, SendAllReadFirstLinger spawns a goroutine to linger and
// collect responses from all the clusters. When all responses have been
// collected, SendAllReadFirstLinger will determine which keys should be sent
// to the repairer.
func SendAllReadFirstLinger(farm *Farm) Selecter { return SendVarReadFirstLinger(-1, -1)(farm) }

// SendVarReadFirstLinger is a refined version of SendAllReadFirstLinger. It
// works in the same way but reduces the requests to all clusters under
// certain circumstances. If maxKeysPerSecond is exceeded by this read
// strategy, it will stop performing SendAll and revert to SendOne. SendOne
// has two issues: no repairs will ever be made, and if the chosen cluster is
// slow or unusable, the read will be delayed or fail. The first issue can be
// ignored, because the baseline SendAll reads provide a basis for repairs. To
// solve the second issue, we promote any SendOne to a SendAll if  no results
// are returned by thresholdLatency.
//
// To never perform an initial SendAll, set maxKeysPerSecond to 0. To always
// perform an initial SendAll, set maxKeysPerSecond to a negative value.
func SendVarReadFirstLinger(maxKeysPerSecond int, thresholdLatency time.Duration) func(*Farm) Selecter {
	permitter := permitter(allowAllPermitter{})
	if maxKeysPerSecond >= 0 {
		permitter = tokenBucketPermitter{tb.NewBucket(int64(maxKeysPerSecond), -1)}
	}
	permitter.canHas(0)
	return func(farm *Farm) Selecter {
		return sendVarReadFirstLinger{
			Farm:             farm,
			permitter:        permitter,
			thresholdLatency: thresholdLatency,
		}
	}
}

type sendVarReadFirstLinger struct {
	*Farm
	permitter
	thresholdLatency time.Duration
}

// SelectOffset implements farm.Selecter.
func (s sendVarReadFirstLinger) SelectOffset(keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error) {
	return s.read(keys, func(c cluster.Cluster, keys []string) <-chan cluster.Element {
		return c.SelectOffset(keys, offset, limit)
	}, limit)
}

// SelectRange implements farm.Selecter.
func (s sendVarReadFirstLinger) SelectRange(keys []string, start, stop common.Cursor, limit int) (map[string][]common.KeyScoreMember, error) {
	return s.read(keys, func(c cluster.Cluster, keys []string) <-chan cluster.Element {
		return c.SelectRange(keys, start, stop, limit)
	}, limit)
}

func (s sendVarReadFirstLinger) read(keys []string, fn func(cluster.Cluster, []string) <-chan cluster.Element, limit int) (map[string][]common.KeyScoreMember, error) {
	began := time.Now()
	go func() {
		s.Farm.instrumentation.SelectCall()
		s.Farm.instrumentation.SelectKeys(len(keys))
	}()

	// We'll combine all response elements into a single channel. When all
	// clusters have finished sending elements there, close it, so we can have
	// nice range semantics in our linger phase.
	elements := make(chan cluster.Element)
	wg := sync.WaitGroup{}
	wg.Add(len(s.Farm.clusters))
	go func() {
		// Note that we need a wg.Done signal for every cluster, even if we
		// didn't actually send to it!
		wg.Wait()
		close(elements)
	}()

	// Depending on maySendAll, pick either one random cluster or all of them.
	var (
		clustersUsed    = []cluster.Cluster{}
		clustersNotUsed = []cluster.Cluster{}
		maySendAll      = s.permitter.canHas(int64(len(keys)))
	)
	if maySendAll {
		go s.Farm.instrumentation.SelectSendAllPermitGranted()
		clustersUsed = s.Farm.clusters
		clustersNotUsed = []cluster.Cluster{}
	} else {
		go s.Farm.instrumentation.SelectSendAllPermitRejected()
		i := rand.Intn(len(s.Farm.clusters))
		clustersUsed = s.Farm.clusters[i : i+1]
		clustersNotUsed = make([]cluster.Cluster, 0, len(s.Farm.clusters)-1)
		clustersNotUsed = append(clustersNotUsed, s.Farm.clusters[:i]...)
		clustersNotUsed = append(clustersNotUsed, s.Farm.clusters[i+1:]...)
	}

	blockingBegan := time.Now()
	go s.Farm.instrumentation.SelectSendTo(len(clustersUsed))
	scatterSelects(clustersUsed, func(c cluster.Cluster) <-chan cluster.Element { return fn(c, keys) }, &wg, elements)

	// remainingKeys keeps track of all keys for which we haven't received any
	// non-error responses yet.
	remainingKeys := make(map[string]bool, len(keys))
	for _, key := range keys {
		remainingKeys[key] = true
	}

	// If we are not permitted to SendAll, we need a timeout (after which we
	// will SendAll nevertheless).
	var timeout <-chan time.Time // initially nil
	if !maySendAll && s.thresholdLatency >= 0 {
		timeout = time.After(s.thresholdLatency)
	}

	var (
		firstResponseDuration time.Duration
		responses             = map[string][]tupleSet{}
		retrieved             = 0
	)

loop:
	for {
		select {
		case e, ok := <-elements:
			if !ok {
				break loop // elements already closed, all Selects done.
			}
			retrieved += len(e.KeyScoreMembers)
			if e.Error != nil {
				log.Printf("SendVarReadFirstLinger initial read partial error: %s", e.Error)
				go s.Farm.instrumentation.SelectPartialError()
				continue
				// It might appear tempting to immediately send a Select to
				// the unusedClusters once we run into an error. However, it's
				// probably better to wait until thresholdLatency has passed
				// (which should be a short duration anyway and might have
				// already happened...) to gather all the keys for which we
				// need a SendAll first and then do them all in one big
				// Select.
			}
			if firstResponseDuration == 0 {
				firstResponseDuration = time.Since(blockingBegan)
			}
			responses[e.Key] = append(responses[e.Key], makeSet(e.KeyScoreMembers))
			delete(remainingKeys, e.Key)

		case <-timeout:
			// Promote to SendAll for remaining keys.
			go s.Farm.instrumentation.SelectSendAllPromotion()
			maySendAll = true
			remainingKeysSlice := make([]string, 0, len(remainingKeys))
			for k := range remainingKeys {
				remainingKeysSlice = append(remainingKeysSlice, k)
			}
			go s.Farm.instrumentation.SelectSendTo(len(clustersNotUsed))
			scatterSelects(clustersNotUsed, func(c cluster.Cluster) <-chan cluster.Element { return fn(c, remainingKeysSlice) }, &wg, elements)
			clustersUsed = s.Farm.clusters
			clustersNotUsed = []cluster.Cluster{}
		}

		if len(remainingKeys) == 0 {
			// We got enough results to return our results.
			break loop
		}
	}

	var (
		blockingDuration = time.Since(blockingBegan)
		returned         = 0
	)
	defer func() {
		duration := time.Since(began)
		go func() {
			s.Farm.instrumentation.SelectDuration(duration)
			s.Farm.instrumentation.SelectFirstResponseDuration(firstResponseDuration)
			s.Farm.instrumentation.SelectBlockingDuration(blockingDuration)
			s.Farm.instrumentation.SelectOverheadDuration(duration - blockingDuration)
			s.Farm.instrumentation.SelectRetrieved(retrieved)
			s.Farm.instrumentation.SelectReturned(returned)
		}()
	}()

	// If we are here, we either got at least one result for each key, or all
	// Select calls have finished but we still did not get at least one result
	// for each key. In either case, it's time to return results.
	if len(responses) == 0 && len(remainingKeys) > 0 {
		// All Selects returned an error.
		return map[string][]common.KeyScoreMember{}, fmt.Errorf("complete failure")
	}

	var (
		response = map[string][]common.KeyScoreMember{}
		repairs  = keyMemberSet{}
	)
	for key, tupleSets := range responses {
		union, difference := unionDifference(tupleSets)
		a := union.orderedLimitedSlice(limit)
		response[key] = a
		returned += len(a)
		repairs.addMany(difference)
	}

	var (
		sentAllButIncomplete = len(remainingKeys) > 0
		sentOneGotEverything = !maySendAll
	)
	if sentAllButIncomplete {
		// We already got all results but they are incomplete because
		// of errors. Partial results are still better than nothing,
		// so issue repairs as needed and return the partial results.
		if len(repairs) > 0 {
			go s.Farm.repairStrategy(repairs.slice())
		}
		return response, nil
	}
	if sentOneGotEverything {
		// The WaitGroup expects len(s.Farm.clusters) Done signals,
		// but so far we've only given 1. Give the rest.
		for _ = range clustersNotUsed {
			wg.Done()
		}
		return response, nil
	}

	// If we are here, we *might* still have Selects running. So start
	// a goroutine to "linger" and collect the remaining responses for
	// repairs before returning the results we have so far.
	go func() {
		lingeringRetrievals := 0
		for e := range elements {
			lingeringRetrievals += len(e.KeyScoreMembers)
			if e.Error != nil {
				log.Printf("SendVarReadFirstLinger lingering retrieval partial error: %s", e.Error)
				go s.Farm.instrumentation.SelectPartialError()
				continue
			}
			responses[e.Key] = append(responses[e.Key], makeSet(e.KeyScoreMembers))
		}
		for _, tupleSets := range responses {
			_, difference := unionDifference(tupleSets)
			repairs.addMany(difference)
		}
		if len(repairs) > 0 {
			go func() {
				s.Farm.instrumentation.SelectRepairNeeded(len(repairs))
				s.Farm.repairStrategy(repairs.slice())
			}()
		}
		s.Farm.instrumentation.SelectRetrieved(lingeringRetrievals) // additive
	}()
	return response, nil
}

func scatterSelects(
	clusters []cluster.Cluster,
	fn func(cluster.Cluster) <-chan cluster.Element,
	wg *sync.WaitGroup,
	dst chan cluster.Element,
) {
	for _, c := range clusters {
		go func(c cluster.Cluster) {
			defer wg.Done()
			for e := range fn(c) {
				dst <- e
			}
		}(c)
	}
}
