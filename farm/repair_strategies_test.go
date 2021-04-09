package farm

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"github.com/soundcloud/roshi/common"
	"github.com/soundcloud/roshi/instrumentation"
)

func TestAllRepairs(t *testing.T) {
	// Build farm around mock clusters.
	n := 5
	clusters := newMockClusters(n)
	farm := New(clusters, (n/2)+1, SendAllReadAll, NoRepairs, nil)

	// Make inserts, no repair.
	first := common.KeyScoreMember{Key: "foo", Score: 1., Member: "bar"}
	second := common.KeyScoreMember{Key: "foo", Score: 2.34, Member: "bar"}

	farm.Insert([]common.KeyScoreMember{first})         // perfect insert
	clusters[0].Insert([]common.KeyScoreMember{second}) // imperfect insert

	// Pre-repair, we should have divergent view.
	for i := 0; i < n; i++ {
		//t.Logf("pre-repair: cluster %d: %+v", i, clusters[i].(*mockCluster).m)
		expected := first
		if i == 0 {
			expected = second
		}
		got := <-clusters[i].SelectOffset([]string{"foo"}, 0, 10)
		if len(got.KeyScoreMembers) <= 0 {
			t.Errorf("pre-repair: cluster %d: only got %d responses", i, len(got.KeyScoreMembers))
			continue
		}
		if !reflect.DeepEqual(expected, got.KeyScoreMembers[0]) {
			t.Errorf("pre-repair: cluster %d: expected %+v, got %+v", i, expected, got.KeyScoreMembers[0])
			continue
		}
	}

	// Issue repair.
	AllRepairs(clusters, instrumentation.NopInstrumentation{})([]common.KeyMember{common.KeyMember{Key: "foo", Member: "bar"}})

	// Post-repair, we should have perfect agreement on the correct value.
	expected := second
	for i := 0; i < n; i++ {
		//t.Logf("post-repair: cluster %d: %+v", i, clusters[i].(*mockCluster).m)
		if got := <-clusters[i].SelectOffset([]string{"foo"}, 0, 10); !reflect.DeepEqual(expected, got.KeyScoreMembers[0]) {
			t.Errorf("post-repair: cluster %d: expected %+v, got %+v", i, expected, got.KeyScoreMembers[0])
		}
	}
}

func TestRateLimitedRepairs(t *testing.T) {
	// Build farm around mock clusters.
	n := 5
	clusters := newMockClusters(n)
	farm := New(clusters, (n/2)+1, SendAllReadAll, NoRepairs, nil)

	// Make inserts, no repair.
	a := common.KeyScoreMember{Key: "foo", Score: 1.1, Member: "alpha"}
	b := common.KeyScoreMember{Key: "foo", Score: 1.2, Member: "beta"}
	c := common.KeyScoreMember{Key: "foo", Score: 1.3, Member: "delta"}
	d := common.KeyScoreMember{Key: "foo", Score: 2.1, Member: "alpha"}
	e := common.KeyScoreMember{Key: "foo", Score: 2.2, Member: "beta"}
	f := common.KeyScoreMember{Key: "foo", Score: 2.3, Member: "delta"}
	farm.Insert([]common.KeyScoreMember{a})
	farm.Insert([]common.KeyScoreMember{b})
	farm.Insert([]common.KeyScoreMember{c})
	clusters[0].Insert([]common.KeyScoreMember{d})
	clusters[0].Insert([]common.KeyScoreMember{e})
	clusters[0].Insert([]common.KeyScoreMember{f})

	// Perform all repairs as fast as possible.
	maxRepairsPerSecond := 2
	repairFunc := RateLimited(maxRepairsPerSecond, AllRepairs)(clusters, instrumentation.NopInstrumentation{})
	repairFunc([]common.KeyMember{common.KeyMember{Key: "foo", Member: "alpha"}}) // should succeed
	repairFunc([]common.KeyMember{common.KeyMember{Key: "foo", Member: "beta"}})  // should succeed
	repairFunc([]common.KeyMember{common.KeyMember{Key: "foo", Member: "delta"}}) // should fail

	// Make post-repair checks. We only care about clusters 1 and above.
	expected := []common.KeyScoreMember{e, d, c}
	for i := 0; i < n; i++ {
		got := <-clusters[i].SelectOffset([]string{"foo"}, 0, 10)
		t.Logf("post-repair: cluster %d: has %+v", i, got.KeyScoreMembers)
		if i == 0 {
			continue // assume clusters[0] has everything correctly
		}
		if !reflect.DeepEqual(expected, got.KeyScoreMembers) {
			t.Errorf("post-repair: cluster %d: expected %+v, got %+v", i, expected, got.KeyScoreMembers)
		}
	}
}

func TestExplodingGoroutines(t *testing.T) {
	// Make a farm.
	n := 5
	clusters := newMockClusters(n)
	farm := New(clusters, (n/2)+1, SendAllReadAll, AllRepairs, nil)

	// Insert a big key into every cluster except the first.
	key := "foo"
	maxSize := 2000
	tuples := makeBigKey(key, maxSize)
	for i := 1; i < n; i++ {
		clusters[i].Insert(tuples)
	}

	// Issue repair by making a Select.
	before := runtime.NumGoroutine()
	farm.SelectOffset([]string{key}, 0, maxSize)
	runtime.Gosched()
	after := runtime.NumGoroutine()

	if delta := after - before; delta > 5 {
		t.Errorf("The repair-enabled Select created %d goroutines. That's too many.", delta)
	}
}

func makeBigKey(key string, n int) []common.KeyScoreMember {
	tuples := make([]common.KeyScoreMember, n)
	for i := 0; i < n; i++ {
		tuples[i] = common.KeyScoreMember{
			Key:    key,
			Score:  float64(i),
			Member: fmt.Sprint(i),
		}
	}
	return tuples
}
