package farm

import (
	"time"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
)

// Repairer is the type of the various repairers that can be chosen
// for a Farm. Under the hood, it's a factory function that creates
// the actual repairer (the so called coreRepairer).
type Repairer func(*Farm) coreRepairer

// keyMember is like a KeyScoreMember just without score.
// It is used to pass "suspects" to the coreRepairer.
type keyMember struct {
	Key    string
	Member string
}

// coreRepairer is the interface that is used by a Farm to request
// repairs.
type coreRepairer interface {
	// requestRepair requests to check and (if needed) repair
	// keyMembers. The coreRepairer calls the Repair method of the
	// Farm for the keyMembers with a frequency and probability
	// that depends on the concrete implementation. This method is
	// safe to be called concurrently.
	requestRepair([]keyMember)
	// close shuts down the coreRepairer. Implementations of
	// coreRepairer may do things like starting go routines, which
	// will be stopped by this method. Pending repair requests
	// will not be processed anymore after this method
	// returns. This method must only be called once. Closing a
	// closed coreRepairer and calling requestRepair of a closed
	// coreRepairer has undefined behavior.
	close()
}

// NopRepairer doesn't do any repairs whatsoever. Use it if you want
// to disable repairs.
func NopRepairer(*Farm) coreRepairer { return &nopCoreRepairer{} }

type nopCoreRepairer struct{}

func (r *nopCoreRepairer) requestRepair([]keyMember) {}
func (r *nopCoreRepairer) close()                    {}

// RateLimitedRepairer processes repair requests asynchronously. It
// will try all requested repairs (in undefined order) but limits the
// rate of repairs to a fixed maximum value. The maxBacklog parameter
// limits the size of the backlog (that will pile up if the rate of
// repair requests exceeds the repair rate limit). If maxBacklog is
// reached, new repair requests will be dropped. For an unlimited
// backlog, use 0 or a negative value.
func RateLimitedRepairer(repairsPerSecond int, maxBacklog int) func(*Farm) coreRepairer {
	return func(farm *Farm) coreRepairer {
		r := rateLimitedCoreRepairer{
			farm:             farm,
			repairsPerSecond: repairsPerSecond,
			maxBacklog:       maxBacklog,
			repairs:          make(map[keyMember]bool),
			addRepairs:       make(chan []keyMember),
			quit:             make(chan bool),
		}
		go r.loop()
		return &r
	}
}

type rateLimitedCoreRepairer struct {
	farm             *Farm
	repairsPerSecond int
	maxBacklog       int
	repairs          map[keyMember]bool
	addRepairs       chan []keyMember
	quit             chan bool
}

func (r *rateLimitedCoreRepairer) requestRepair(kms []keyMember) {
	r.farm.instrumentation.RepairCall()
	r.farm.instrumentation.RepairRequestCount(len(kms))
	r.addRepairs <- kms
}

func (r *rateLimitedCoreRepairer) close() {
	r.quit <- true
}

func (r *rateLimitedCoreRepairer) loop() {
	ticker := time.NewTicker(time.Second / time.Duration(r.repairsPerSecond))
	// We start with a nil tickerChannel (i.e. we will not read
	// anything from that channel in the select below). We will
	// only switch tickerChannel to the actual ticker.C when there
	// are repairs to process. (Note that a ticker nobody reads
	// from will implicitly stop ticking until it is read from
	// again.)
	var tickerChannel <-chan time.Time
	for {
		select {
		case newRepairs := <-r.addRepairs:
			for _, newRepair := range newRepairs {
				if r.maxBacklog <= 0 || len(r.repairs) < r.maxBacklog {
					r.repairs[newRepair] = true
				} else {
					r.farm.instrumentation.RepairDiscarded(1)
				}
			}
			if len(r.repairs) > 0 {
				// We have repairs now. Switch on the ticker.
				tickerChannel = ticker.C
			}
		case <-tickerChannel:
			for repair := range r.repairs {
				go r.farm.Repair(repair)
				delete(r.repairs, repair)
				break // Only do one (random) repair.
			}
			if len(r.repairs) == 0 {
				// No repairs. Put the ticker to sleep.
				tickerChannel = nil
			}
		case <-r.quit:
			ticker.Stop()
			return
		}
	}
}

type scoreResponseTuple struct {
	cluster     int
	score       float64
	wasInserted bool // false: This keyMember was deleted.
	err         error
}

// Repair queries all clusters for the most recent score for the given
// keyMember taking both, the deletes key and the inserts key, into
// account. It then propagates that score and if it was connected to
// the deletes or the inserts key to all clusters that are not up to
// date already.
func (farm *Farm) Repair(km keyMember) {
	began := time.Now()
	clustersUpToDate := map[int]bool{}
	highestScore := 0.
	var wasInserted bool // Whether the highest scoring keyMember was inserted or deleted.

	// Scatter.
	responsesChan := make(chan scoreResponseTuple, len(farm.clusters))
	for i, c := range farm.clusters {
		go func(i int, c cluster.Cluster) {
			score, wasInserted, err := c.Score(km.Key, km.Member)
			responsesChan <- scoreResponseTuple{i, score, wasInserted, err}
		}(i, c)
	}

	// Gather.
	for i := 0; i < cap(responsesChan); i++ {
		resp := <-responsesChan
		if resp.err != nil {
			farm.instrumentation.RepairCheckPartialFailure()
			continue
		}
		if resp.score == highestScore && resp.wasInserted == wasInserted {
			clustersUpToDate[resp.cluster] = true
			continue
		}
		if resp.score > highestScore {
			highestScore = resp.score
			wasInserted = resp.wasInserted
			clustersUpToDate = map[int]bool{}
			clustersUpToDate[resp.cluster] = true
		}
		// Highly unlikely corner case: One cluster returns
		// wasInserted=true, another wasInserted=false, but both
		// return the same score. I that case, we will
		// propagate the first result encountered. (And yes,
		// this situation will screw us up elsewhere, too.)
	}
	farm.instrumentation.RepairCheckDuration(time.Now().Sub(began))

	if highestScore == 0. {
		// All errors (or keyMember not found). Do not proceed.
		farm.instrumentation.RepairCheckCompleteFailure()
		return
	}
	if len(clustersUpToDate) == len(farm.clusters) {
		// Cool. All clusters agree already. Done.
		farm.instrumentation.RepairCheckRedundant()
		return
	}
	// We have a KeyScoreMember, and we have to propagate it to some clusters.
	farm.instrumentation.RepairWriteCount()
	ksm := common.KeyScoreMember{Key: km.Key, Score: highestScore, Member: km.Member}
	for i, c := range farm.clusters {
		if !clustersUpToDate[i] {
			go func(c cluster.Cluster) {
				defer func(began time.Time) {
					farm.instrumentation.RepairWriteDuration(time.Now().Sub(began))
				}(time.Now())
				var err error
				if wasInserted {
					err = c.Insert([]common.KeyScoreMember{ksm})
				} else {
					err = c.Delete([]common.KeyScoreMember{ksm})
				}
				if err == nil {
					farm.instrumentation.RepairWriteSuccess()
				} else {
					farm.instrumentation.RepairWriteFailure()
				}
			}(c)
		}
	}
}
