// Package farm provides a robust API for CRDTs on top of multiple clusters.
package farm

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
	"github.com/soundcloud/roshi/instrumentation"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Farm implements CRDT-enabled ZSET methods over many clusters.
type Farm struct {
	clusters        []cluster.Cluster
	writeQuorum     int
	readStrategy    coreReadStrategy
	repairer        coreRepairer
	instrumentation instrumentation.Instrumentation
}

// New creates and returns a new Farm.
//
// Writes are always sent to all clusters, and writeQuorum determines how many
// individual successful responses need to be received before the client
// receives an overall success. Reads are sent to clusters according to the
// passed ReadStrategy.
//
// Clusters, writeQuorum, and readStrategy are required. Repairer and
// instrumentation may be nil.
func New(
	clusters []cluster.Cluster,
	writeQuorum int,
	readStrategy ReadStrategy,
	repairer Repairer,
	instr instrumentation.Instrumentation,
) *Farm {
	if instr == nil {
		instr = instrumentation.NopInstrumentation{}
	}
	if repairer == nil {
		repairer = NopRepairer
	}
	farm := &Farm{
		clusters:        clusters,
		writeQuorum:     writeQuorum,
		instrumentation: instr,
	}
	farm.readStrategy = readStrategy(farm)
	farm.repairer = repairer(farm)
	return farm
}

// Insert adds each tuple into each underlying cluster, if the scores are
// greater than the already-stored scores. As long as over half of the clusters
// succeed to write all tuples, the overall write succeeds.
func (f *Farm) Insert(tuples []common.KeyScoreMember) error {
	return f.write(
		tuples,
		func(c cluster.Cluster, a []common.KeyScoreMember) error { return c.Insert(a) },
		insertInstrumentation{f.instrumentation},
	)
}

// Selecter defines a synchronous Select API, implemented by Farm.
type Selecter interface {
	Select(keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error)
}

// Select invokes the ReadStrategy of the farm.
func (f *Farm) Select(keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error) {
	// High performance optimization.
	if len(keys) <= 0 {
		return map[string][]common.KeyScoreMember{}, nil
	}
	return f.readStrategy(keys, offset, limit)
}

// Delete removes each tuple from the underlying clusters, if the score is
// greater than the already-stored scores.
func (f *Farm) Delete(tuples []common.KeyScoreMember) error {
	return f.write(
		tuples,
		func(c cluster.Cluster, a []common.KeyScoreMember) error { return c.Delete(a) },
		deleteInstrumentation{f.instrumentation},
	)
}

func (f *Farm) write(
	tuples []common.KeyScoreMember,
	action func(cluster.Cluster, []common.KeyScoreMember) error,
	instr writeInstrumentation,
) error {
	// High performance optimization.
	if len(tuples) <= 0 {
		return nil
	}
	instr.call()
	instr.recordCount(len(tuples))
	defer func(began time.Time) {
		d := time.Now().Sub(began)
		instr.callDuration(d)
		instr.recordDuration(d / time.Duration(len(tuples)))
	}(time.Now())

	// Scatter
	errChan := make(chan error, len(f.clusters))
	for _, c := range f.clusters {
		go func(c cluster.Cluster) {
			errChan <- action(c, tuples)
		}(c)
	}

	// Gather
	errors, got, need := []string{}, 0, f.writeQuorum
	haveQuorum := func() bool { return got-len(errors) >= need }
	for i := 0; i < cap(errChan); i++ {
		err := <-errChan
		if err != nil {
			errors = append(errors, err.Error())
		}
		got++
		if haveQuorum() {
			break
		}
	}

	// Report
	if !haveQuorum() {
		instr.quorumFailure()
		return fmt.Errorf("no quorum (%s)", strings.Join(errors, "; "))
	}
	return nil
}

// unionDifference computes two sets of keys from the input sets. Union is
// defined to be every key-member and its best (highest) score. Difference is
// defined to be those key-members with imperfect agreement across all input
// sets.
func unionDifference(tupleSets []tupleSet) (tupleSet, keyMemberSet) {
	expectedCount := len(tupleSets)
	counts := map[common.KeyScoreMember]int{}
	scores := map[keyMember]float64{}
	for _, tupleSet := range tupleSets {
		for tuple := range tupleSet {
			// For union
			km := keyMember{Key: tuple.Key, Member: tuple.Member}
			if score, ok := scores[km]; !ok || tuple.Score > score {
				scores[km] = tuple.Score
			}
			// For difference
			counts[tuple]++
		}
	}

	union, difference := tupleSet{}, keyMemberSet{}
	for km, bestScore := range scores {
		union.add(common.KeyScoreMember{
			Key:    km.Key,
			Score:  bestScore,
			Member: km.Member,
		})
	}
	for ksm, count := range counts {
		if count < expectedCount {
			difference.add(keyMember{
				Key:    ksm.Key,
				Member: ksm.Member,
			})
		}
	}
	return union, difference
}

type tupleSet map[common.KeyScoreMember]struct{}

func makeSet(a []common.KeyScoreMember) tupleSet {
	s := make(tupleSet, len(a))
	for _, tuple := range a {
		s.add(tuple)
	}
	return s
}

func (s tupleSet) add(tuple common.KeyScoreMember) {
	s[tuple] = struct{}{}
}

func (s tupleSet) has(tuple common.KeyScoreMember) bool {
	_, ok := s[tuple]
	return ok
}

func (s tupleSet) addMany(other tupleSet) {
	for tuple := range other {
		s.add(tuple)
	}
}

func (s tupleSet) slice() []common.KeyScoreMember {
	a := make([]common.KeyScoreMember, 0, len(s))
	for tuple := range s {
		a = append(a, tuple)
	}
	return a
}

func (s tupleSet) orderedLimitedSlice(limit int) []common.KeyScoreMember {
	a := s.slice()
	sort.Sort(common.KeyScoreMembers(a))
	if len(a) > limit {
		a = a[:limit]
	}
	return a
}

type keyMemberSet map[keyMember]struct{}

func (s keyMemberSet) add(km keyMember) {
	s[km] = struct{}{}
}

func (s keyMemberSet) addMany(other keyMemberSet) {
	for km := range other {
		s.add(km)
	}
}

func (s keyMemberSet) slice() []keyMember {
	a := make([]keyMember, 0, len(s))
	for km := range s {
		a = append(a, km)
	}
	return a
}

type writeInstrumentation interface {
	call()
	recordCount(int)
	callDuration(time.Duration)
	recordDuration(time.Duration)
	quorumFailure()
}

type insertInstrumentation struct {
	instrumentation.Instrumentation
}

func (i insertInstrumentation) call()                          { i.InsertCall() }
func (i insertInstrumentation) recordCount(n int)              { i.InsertRecordCount(n) }
func (i insertInstrumentation) callDuration(d time.Duration)   { i.InsertCallDuration(d) }
func (i insertInstrumentation) recordDuration(d time.Duration) { i.InsertRecordDuration(d) }
func (i insertInstrumentation) quorumFailure()                 { i.InsertQuorumFailure() }

type deleteInstrumentation struct {
	instrumentation.Instrumentation
}

func (i deleteInstrumentation) call()                          { i.DeleteCall() }
func (i deleteInstrumentation) recordCount(n int)              { i.DeleteRecordCount(n) }
func (i deleteInstrumentation) callDuration(d time.Duration)   { i.DeleteCallDuration(d) }
func (i deleteInstrumentation) recordDuration(d time.Duration) { i.DeleteRecordDuration(d) }
func (i deleteInstrumentation) quorumFailure()                 { i.DeleteQuorumFailure() }
