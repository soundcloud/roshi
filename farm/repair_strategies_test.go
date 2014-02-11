package farm

import (
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
		t.Logf("pre-repair: cluster %d: %+v", i, clusters[i].(*mockCluster).m)
		expected := first
		if i == 0 {
			expected = second
		}
		if got := <-clusters[i].Select([]string{"foo"}, 0, 10); !reflect.DeepEqual(expected, got.KeyScoreMembers[0]) {
			t.Errorf("pre-repair: cluster %d: expected %+v, got %+v", i, expected, got.KeyScoreMembers[0])
		}
	}

	// Issue repair.
	AllRepairs(clusters, instrumentation.NopInstrumentation{})([]keyMember{keyMember{Key: "foo", Member: "bar"}})
	runtime.Gosched()

	// Post-repair, we should have perfect agreement on the correct value.
	expected := second
	for i := 0; i < n; i++ {
		t.Logf("post-repair: cluster %d: %+v", i, clusters[i].(*mockCluster).m)
		if got := <-clusters[i].Select([]string{"foo"}, 0, 10); !reflect.DeepEqual(expected, got.KeyScoreMembers[0]) {
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
	repairFunc := RateLimitedRepairs(clusters, instrumentation.NopInstrumentation{}, maxRepairsPerSecond)
	repairFunc([]keyMember{keyMember{Key: "foo", Member: "alpha"}}) // should succeed
	repairFunc([]keyMember{keyMember{Key: "foo", Member: "beta"}})  // should succeed
	repairFunc([]keyMember{keyMember{Key: "foo", Member: "delta"}}) // should fail

	// Make post-repair checks. We only care about clusters 1 and above.
	expected := []common.KeyScoreMember{e, d, c}
	for i := 0; i < n; i++ {
		got := <-clusters[i].Select([]string{"foo"}, 0, 10)
		t.Logf("post-repair: cluster %d: has %+v", i, got.KeyScoreMembers)
		if i == 0 {
			continue // assume clusters[0] has everything correctly
		}
		if !reflect.DeepEqual(expected, got.KeyScoreMembers) {
			t.Errorf("post-repair: cluster %d: expected %+v, got %+v", i, expected, got.KeyScoreMembers)
		}
	}
}
