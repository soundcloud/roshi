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
	readStrategy    coreReadStrategy
	repairer        coreRepairer
	instrumentation instrumentation.Instrumentation
}

// New creates and returns a new Farm. Instrumentation and repairer may be nil.
func New(
	clusters []cluster.Cluster,
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
	// High performance optimization.
	if len(tuples) <= 0 {
		return nil
	}
	f.instrumentation.InsertCall()
	f.instrumentation.InsertRecordCount(len(tuples))
	defer func(began time.Time) {
		d := time.Now().Sub(began)
		f.instrumentation.InsertCallDuration(d)
		f.instrumentation.InsertRecordDuration(d / time.Duration(len(tuples)))
	}(time.Now())

	// Scatter
	errChan := make(chan error, len(f.clusters))
	for _, c := range f.clusters {
		go func(c cluster.Cluster) {
			errChan <- c.Insert(tuples)
		}(c)
	}

	// Gather
	errors := []string{}
	for i := 0; i < cap(errChan); i++ {
		err := <-errChan
		if err != nil {
			errors = append(errors, err.Error())
		}
	}

	// Report
	if float32(len(f.clusters)-len(errors))/float32(len(f.clusters)) <= 0.5 {
		f.instrumentation.InsertQuorumFailure()
		return fmt.Errorf("no quorum (%s)", strings.Join(errors, "; "))
	}
	return nil
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
	// High performance optimization.
	if len(tuples) <= 0 {
		return nil
	}
	f.instrumentation.DeleteCall()
	f.instrumentation.DeleteRecordCount(len(tuples))
	defer func(began time.Time) {
		d := time.Now().Sub(began)
		f.instrumentation.DeleteCallDuration(d)
		f.instrumentation.DeleteRecordDuration(d / time.Duration(len(tuples)))
	}(time.Now())

	// Scatter
	errChan := make(chan error, len(f.clusters))
	for _, c := range f.clusters {
		go func(c cluster.Cluster) {
			errChan <- c.Delete(tuples)
		}(c)
	}

	// Gather
	errors := []string{}
	for i := 0; i < cap(errChan); i++ {
		err := <-errChan
		if err != nil {
			errors = append(errors, err.Error())
		}
	}

	// Report
	if float32(len(f.clusters)-len(errors))/float32(len(f.clusters)) <= 0.5 {
		f.instrumentation.DeleteQuorumFailure()
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
