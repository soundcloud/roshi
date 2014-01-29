package farm

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
)

func TestMockCluster(t *testing.T) {
	c := newMockCluster()

	// Insert 2
	if err := c.Insert([]common.KeyScoreMember{
		common.KeyScoreMember{Key: "foo", Score: 1, Member: "bar"},
		common.KeyScoreMember{Key: "foo", Score: 2, Member: "baz"},
	}); err != nil {
		t.Fatal(err)
	}

	// Select
	expected := map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			common.KeyScoreMember{Key: "foo", Score: 2, Member: "baz"},
			common.KeyScoreMember{Key: "foo", Score: 1, Member: "bar"},
		},
	}
	ch := c.Select([]string{"foo"}, 0, 10)
	got := map[string][]common.KeyScoreMember{}
	for e := range ch {
		if e.Error != nil {
			t.Errorf("%s: %s", e.Key, e.Error)
		}
		got[e.Key] = e.KeyScoreMembers
	}
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected %+v, got %+v", expected, got)
	}

	// Delete 1
	if err := c.Delete([]common.KeyScoreMember{
		common.KeyScoreMember{Key: "foo", Score: 999, Member: "bar"},
	}); err != nil {
		t.Fatal(err)
	}

	// Select
	expected = map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			common.KeyScoreMember{Key: "foo", Score: 2, Member: "baz"},
		},
	}
	ch = c.Select([]string{"foo"}, 0, 10)
	got = map[string][]common.KeyScoreMember{}
	for e := range ch {
		if e.Error != nil {
			t.Errorf("%s: %s", e.Key, e.Error)
		}
		got[e.Key] = e.KeyScoreMembers
	}
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected %+v, got %+v", expected, got)
	}
}

// mockCluster is *not* safe for concurrent use.
type mockCluster struct {
	m                 map[string]common.KeyScoreMembers
	failing           bool
	countInsert       int32
	countSelect       int32
	countDelete       int32
	countScore        int32
	countKeys         int32
	countOpenChannels int32
}

func newMockCluster() *mockCluster {
	return &mockCluster{
		m: map[string]common.KeyScoreMembers{},
	}
}

func newFailingMockCluster() *mockCluster {
	return &mockCluster{
		m:       map[string]common.KeyScoreMembers{},
		failing: true,
	}
}

func (c *mockCluster) Insert(tuples []common.KeyScoreMember) error {
	atomic.AddInt32(&c.countInsert, 1)
	if c.failing {
		return errors.New("failtown, population you")
	}
	for _, tuple := range tuples {
		a := c.m[tuple.Key]
		a = append(c.m[tuple.Key], common.KeyScoreMember{
			Key:    tuple.Key,
			Score:  tuple.Score,
			Member: tuple.Member,
		})
		sort.Sort(a)
		c.m[tuple.Key] = a
	}
	return nil
}

func (c *mockCluster) Select(keys []string, offset, limit int) <-chan cluster.Element {
	atomic.AddInt32(&c.countSelect, 1)
	ch := make(chan cluster.Element)
	if c.failing {
		close(ch)
		return ch
	}
	atomic.AddInt32(&c.countOpenChannels, 1)
	go func() {
		defer func() {
			close(ch)
			atomic.AddInt32(&c.countOpenChannels, -1)
		}()
		for _, key := range keys {
			ksms := c.m[key]
			if len(ksms) <= offset {
				ch <- cluster.Element{Key: key, KeyScoreMembers: []common.KeyScoreMember{}}
				continue
			}
			ksms = ksms[offset:]
			if len(ksms) > limit {
				ksms = ksms[:limit]
			}
			ch <- cluster.Element{Key: key, KeyScoreMembers: ksms}
		}
	}()
	return ch
}

func (c *mockCluster) Delete(tuples []common.KeyScoreMember) error {
	atomic.AddInt32(&c.countDelete, 1)
	if c.failing {
		return errors.New("failtown, population you")
	}
	for _, toDelete := range tuples {
		for key, existingTuples := range c.m {
			replacementTuples := []common.KeyScoreMember{}
			for _, existingTuple := range existingTuples {
				if existingTuple.Key == toDelete.Key && existingTuple.Member == toDelete.Member {
					continue
				}
				replacementTuples = append(replacementTuples, existingTuple)
			}
			c.m[key] = replacementTuples
		}
	}
	return nil
}

// Score in this mock implementation will never return a score for
// deleted entries.
func (c *mockCluster) Score(key, member string) (float64, bool, error) {
	atomic.AddInt32(&c.countScore, 1)
	if c.failing {
		return 0, false, errors.New("failtown, population you")
	}
	tuples, ok := c.m[key]
	if !ok {
		return 0, false, fmt.Errorf("no member '%s' found for key '%s'", member, key)
	}
	for _, tuple := range tuples {
		if tuple.Member == member {
			return tuple.Score, true, nil
		}
	}
	return 0, false, fmt.Errorf("no member '%s' found for key '%s'", member, key)
}

func (c *mockCluster) Keys() chan string {
	atomic.AddInt32(&c.countKeys, 1)
	ch := make(chan string)
	m := map[string]common.KeyScoreMembers{}
	// Copy c.m so that at least after this method has returned,
	// we don't run into issues with concurrent modifications.
	for k, v := range c.m {
		m[k] = v
	}
	go func() {
		defer close(ch)
		for key := range m {
			ch <- key
		}
	}()
	return ch
}

func newMockClusters(n int) []cluster.Cluster {
	a := make([]cluster.Cluster, n)
	for i := 0; i < n; i++ {
		a[i] = newMockCluster()
	}
	return a
}

func newFailingMockClusters(n int) []cluster.Cluster {
	a := make([]cluster.Cluster, n)
	for i := 0; i < n; i++ {
		a[i] = newFailingMockCluster()
	}
	return a
}
