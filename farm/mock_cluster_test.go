package farm

import (
	"errors"
	"reflect"
	"sort"
	"sync"
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
	ch := c.SelectOffset([]string{"foo"}, 0, 10)
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
	ch = c.SelectOffset([]string{"foo"}, 0, 10)
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
	id                int32
	m                 map[string]map[string]float64 // key: member: score
	failing           bool
	countInsert       int32
	countSelect       int32
	countDelete       int32
	countScore        int32
	countKeys         int32
	countOpenChannels int32
	mutex             *sync.Mutex
}

var mockClusterIDs int32

func newMockCluster() *mockCluster {
	return &mockCluster{
		id:    atomic.AddInt32(&mockClusterIDs, 1),
		m:     map[string]map[string]float64{},
		mutex: &sync.Mutex{},
	}
}

func newFailingMockCluster() *mockCluster {
	return &mockCluster{
		m:       map[string]map[string]float64{},
		failing: true,
		mutex:   &sync.Mutex{},
	}
}

func (c *mockCluster) Insert(keyScoreMembers []common.KeyScoreMember) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	atomic.AddInt32(&c.countInsert, 1)
	if c.failing {
		return errors.New("failtown, population you")
	}

	for _, keyScoreMember := range keyScoreMembers {
		members, ok := c.m[keyScoreMember.Key]
		if !ok {
			// first insert for this key
			c.m[keyScoreMember.Key] = map[string]float64{keyScoreMember.Member: keyScoreMember.Score}
			continue
		}
		score, ok := members[keyScoreMember.Member]
		if ok && keyScoreMember.Score <= score {
			// existing member has a better score
			continue
		}
		// existing member doesn't exist or has a lower score
		c.m[keyScoreMember.Key][keyScoreMember.Member] = keyScoreMember.Score
	}
	return nil
}

func (c *mockCluster) SelectOffset(keys []string, offset, limit int) <-chan cluster.Element {
	atomic.AddInt32(&c.countSelect, 1)
	ch := make(chan cluster.Element)
	if c.failing {
		close(ch)
		return ch
	}
	atomic.AddInt32(&c.countOpenChannels, 1)
	go func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()

		defer func() {
			close(ch)
			atomic.AddInt32(&c.countOpenChannels, -1)
		}()

		for _, key := range keys {
			members, ok := c.m[key]
			if !ok {
				ch <- cluster.Element{Key: key, KeyScoreMembers: []common.KeyScoreMember{}}
				continue
			}

			slice := members2slice(key, members)
			if len(slice) <= offset {
				ch <- cluster.Element{Key: key, KeyScoreMembers: []common.KeyScoreMember{}}
				continue
			}

			slice = slice[offset:]
			if len(slice) > limit {
				slice = slice[:limit]
			}
			ch <- cluster.Element{Key: key, KeyScoreMembers: slice}
		}
	}()
	return ch
}

func (c *mockCluster) SelectRange(keys []string, start, stop common.Cursor, limit int) <-chan cluster.Element {
	ch := make(chan cluster.Element)
	go func() { close(ch) }()
	return ch
}

func members2slice(key string, members map[string]float64) []common.KeyScoreMember {
	a := scoreMemberSlice{}
	for member, score := range members {
		a = append(a, scoreMember{score, member})
	}
	sort.Sort(a)

	keyScoreMembers := make([]common.KeyScoreMember, len(a))
	for i := range a {
		keyScoreMembers[i] = common.KeyScoreMember{
			Key:    key,
			Score:  a[i].score,
			Member: a[i].member,
		}
	}
	return keyScoreMembers
}

type scoreMember struct {
	score  float64
	member string
}

type scoreMemberSlice []scoreMember

func (a scoreMemberSlice) Len() int           { return len(a) }
func (a scoreMemberSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a scoreMemberSlice) Less(i, j int) bool { return a[i].score > a[j].score }

func (c *mockCluster) Delete(keyScoreMembers []common.KeyScoreMember) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	atomic.AddInt32(&c.countDelete, 1)
	if c.failing {
		return errors.New("failtown, population you")
	}

	for _, toDelete := range keyScoreMembers {
		members, ok := c.m[toDelete.Key]
		if !ok {
			// key doesn't exist
			continue
		}
		score, ok := members[toDelete.Member]
		if !ok {
			// member doesn't exist in key
			continue
		}
		// Mock cluster allows deletes with same score!
		// This is different than production to ease testing!
		if toDelete.Score < score {
			// incoming member has insufficient score
			continue
		}
		delete(c.m[toDelete.Key], toDelete.Member)
	}
	return nil
}

// Score in this mock implementation will never return a score for
// deleted entries.
func (c *mockCluster) Score(keyMembers []common.KeyMember) (map[common.KeyMember]cluster.Presence, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	atomic.AddInt32(&c.countScore, 1)
	if c.failing {
		return map[common.KeyMember]cluster.Presence{}, errors.New("failtown, population you")
	}

	m := map[common.KeyMember]cluster.Presence{}

	for _, keyMember := range keyMembers {
		members, ok := c.m[keyMember.Key]
		if !ok {
			m[keyMember] = cluster.Presence{Present: false}
			continue
		}
		score, ok := members[keyMember.Member]
		if !ok {
			m[keyMember] = cluster.Presence{Present: false}
			continue
		}
		m[keyMember] = cluster.Presence{
			Present:  true,
			Inserted: true,
			Score:    score,
		}
	}
	return m, nil
}

func (c *mockCluster) Keys(batchSize int) <-chan []string {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	atomic.AddInt32(&c.countKeys, 1)

	// Copy keys from c.m, so that at least after this method has returned,
	// we don't run into issues with concurrent modifications.
	a := make([]string, 0, len(c.m))

	for key := range c.m {
		a = append(a, key)
	}

	ch := make(chan []string)
	go func() {
		defer close(ch)
		batch := []string{}
		for _, key := range a {
			batch = append(batch, key)
			if len(batch) >= batchSize {
				ch <- batch
				batch = []string{}
			}
		}
		if len(batch) > 0 {
			ch <- batch
		}
	}()
	return ch
}

func (c *mockCluster) clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.m = map[string]map[string]float64{}
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
