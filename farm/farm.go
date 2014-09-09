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

// Farm implements CRDT-semantic ZSET methods over many clusters.
type Farm struct {
	clusters        []cluster.Cluster
	writeQuorum     int
	selecter        Selecter
	repairStrategy  coreRepairStrategy
	instrumentation instrumentation.Instrumentation
}

// New creates and returns a new Farm.
//
// Writes are always sent to all write clusters, and writeQuorum determines
// how many individual successful responses need to be received before the
// client receives an overall success. Reads are sent to read clusters
// according to the passed ReadStrategy.
//
// The repair strategy will only issue repairs against the read clusters.
//
// Instrumentation may be nil; all other parameters are required.
func New(
	clusters []cluster.Cluster,
	writeQuorum int,
	readStrategy ReadStrategy,
	repairStrategy RepairStrategy,
	instr instrumentation.Instrumentation,
) *Farm {
	if instr == nil {
		instr = instrumentation.NopInstrumentation{}
	}
	farm := &Farm{
		clusters:        clusters,
		writeQuorum:     writeQuorum,
		repairStrategy:  repairStrategy(clusters, instr),
		instrumentation: instr,
	}
	farm.selecter = readStrategy(farm)
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
	SelectOffset(keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error)
	SelectRange(keys []string, start, stop common.Cursor, limit int) (map[string][]common.KeyScoreMember, error)
}

// SelectOffset satisfies Selecter and invokes the ReadStrategy of the farm.
func (f *Farm) SelectOffset(keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error) {
	// High performance optimization.
	if len(keys) <= 0 {
		return map[string][]common.KeyScoreMember{}, nil
	}
	return f.selecter.SelectOffset(keys, offset, limit)
}

// SelectRange satisfies Selecter and invokes the ReadStrategy of the farm.
func (f *Farm) SelectRange(keys []string, start, stop common.Cursor, limit int) (map[string][]common.KeyScoreMember, error) {
	// High performance optimization.
	if len(keys) <= 0 {
		return map[string][]common.KeyScoreMember{}, nil
	}
	return f.selecter.SelectRange(keys, start, stop, limit)
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
	var (
		errors     = []string{}
		got        = 0
		need       = f.writeQuorum
		haveQuorum = func() bool { return (got - len(errors)) >= need }
	)
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
	var (
		expectedCount = len(tupleSets)
		scores        = make(map[common.KeyMember]float64, len(tupleSets)*10)
		counts        = make(map[common.KeyScoreMember]int, len(tupleSets)*10)
	)

	for _, tupleSet := range tupleSets {
		for tuple := range tupleSet {
			// For union
			keyMember := common.KeyMember{Key: tuple.Key, Member: tuple.Member}
			if score, ok := scores[keyMember]; !ok || tuple.Score > score {
				scores[keyMember] = tuple.Score
			}

			// For difference
			counts[tuple]++
		}
	}

	var (
		union      = make(tupleSet, len(scores))
		difference = make(keyMemberSet, len(counts))
	)

	for keyMember, bestScore := range scores {
		union.add(common.KeyScoreMember{
			Key:    keyMember.Key,
			Score:  bestScore,
			Member: keyMember.Member,
		})
	}

	for keyScoreMember, count := range counts {
		if count < expectedCount {
			difference.add(common.KeyMember{
				Key:    keyScoreMember.Key,
				Member: keyScoreMember.Member,
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
	sort.Sort(keyScoreMembers(a))
	if len(a) > limit {
		a = a[:limit]
	}
	return a
}

type keyScoreMembers []common.KeyScoreMember

func (a keyScoreMembers) Len() int           { return len(a) }
func (a keyScoreMembers) Less(i, j int) bool { return a[i].Score > a[j].Score }
func (a keyScoreMembers) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type keyMemberSet map[common.KeyMember]struct{}

func (s keyMemberSet) add(keyMember common.KeyMember) {
	s[keyMember] = struct{}{}
}

func (s keyMemberSet) addMany(other keyMemberSet) {
	for keyMember := range other {
		s.add(keyMember)
	}
}

func (s keyMemberSet) slice() []common.KeyMember {
	a := make([]common.KeyMember, 0, len(s))
	for keyMember := range s {
		a = append(a, keyMember)
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

type scoreResponseTuple struct {
	cluster     int
	score       float64
	wasInserted bool // false = this keyMember was deleted
	err         error
}
