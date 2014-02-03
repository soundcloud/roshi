package farm

import (
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soundcloud/roshi/common"
)

func TestInsertSelect(t *testing.T) {
	clusters := newMockClusters(3)
	farm := New(clusters, len(clusters), SendOneReadOne, NopRepairer, 0, nil, nil)

	if err := farm.Insert([]common.KeyScoreMember{
		common.KeyScoreMember{Key: "foo", Score: 5, Member: "five"},
		common.KeyScoreMember{Key: "foo", Score: 4, Member: "four"},
		common.KeyScoreMember{Key: "foo", Score: 9, Member: "nine"},
		common.KeyScoreMember{Key: "bar", Score: 7, Member: "seven"},
		common.KeyScoreMember{Key: "bar", Score: 8, Member: "eight"},
	}); err != nil {
		t.Fatal(err)
	}

	got, err := farm.Select([]string{"foo", "bar", "invalid"}, 0, 10)
	if err != nil {
		t.Fatal(err)
	}

	expected := map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			common.KeyScoreMember{Key: "foo", Score: 9, Member: "nine"},
			common.KeyScoreMember{Key: "foo", Score: 5, Member: "five"},
			common.KeyScoreMember{Key: "foo", Score: 4, Member: "four"},
		},
		"bar": []common.KeyScoreMember{
			common.KeyScoreMember{Key: "bar", Score: 8, Member: "eight"},
			common.KeyScoreMember{Key: "bar", Score: 7, Member: "seven"},
		},
		"invalid": []common.KeyScoreMember{},
	}

	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected\n %+v, got\n %+v", expected, got)
	}
}

func TestOffsetLimit(t *testing.T) {
	clusters := newMockClusters(3)
	f := New(clusters, len(clusters), SendAllReadAll, NopRepairer, 0, nil, nil)

	if err := f.Insert([]common.KeyScoreMember{
		common.KeyScoreMember{Key: "foo", Score: 5, Member: "five"},
		common.KeyScoreMember{Key: "bar", Score: 8, Member: "eight"},
		common.KeyScoreMember{Key: "bar", Score: 7, Member: "seven"},
		common.KeyScoreMember{Key: "foo", Score: 9, Member: "nine"},
		common.KeyScoreMember{Key: "bar", Score: 3, Member: "three"},
		common.KeyScoreMember{Key: "foo", Score: 4, Member: "four"},
		common.KeyScoreMember{Key: "baz", Score: 2, Member: "two"},
	}); err != nil {
		t.Fatal(err)
	}

	got, err := f.Select([]string{"foo", "bar", "baz", "invalid"}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	expected := map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			common.KeyScoreMember{Key: "foo", Score: 5, Member: "five"},
		},
		"bar": []common.KeyScoreMember{
			common.KeyScoreMember{Key: "bar", Score: 7, Member: "seven"},
		},
		"baz":     []common.KeyScoreMember{},
		"invalid": []common.KeyScoreMember{},
	}

	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected\n %+v, got\n %+v", expected, got)
	}
}

func TestSendAllReadAllSelectAfterNoQuorum(t *testing.T) {
	// Build a farm of 3 clusters: 2 failing, 1 successful
	clusters := newFailingMockClusters(2)
	clusters = append(clusters, newMockCluster())
	f := New(clusters, len(clusters), SendAllReadAll, NopRepairer, 0, nil, nil)

	// Make a single KSM.
	foo := common.KeyScoreMember{Key: "foo", Score: 1.0, Member: "bar"}

	// The Insert should fail.
	if err := f.Insert([]common.KeyScoreMember{foo}); err == nil {
		t.Error("expected error, got none")
	}

	// But because we have optimistic set-union semantics, Select should return
	// the written data.
	expected := map[string][]common.KeyScoreMember{"foo": []common.KeyScoreMember{foo}}
	got, err := f.Select([]string{"foo"}, 0, 10)
	if err != nil {
		t.Fatalf("expected successful read, but got: %s", err)
	}
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected %+v, got %+v", expected, got)
	}
}

func TestWalkerReadRepair(t *testing.T) {
	ksms := []common.KeyScoreMember{
		common.KeyScoreMember{Key: "a", Score: 1, Member: "x"},
		common.KeyScoreMember{Key: "a", Score: 2, Member: "y"},
		common.KeyScoreMember{Key: "b", Score: 3, Member: "z"},
		common.KeyScoreMember{Key: "c", Score: 1, Member: "x"},
	}
	// Build a farm of 3 clusters, 2 with some data, 1 empty.
	clusters := newMockClusters(3)
	for _, cluster := range clusters[:2] {
		if err := cluster.Insert(ksms); err != nil {
			t.Fatal(err)
		}
	}
	rp := NewRatePolice(time.Second, 10)
	_ = New(clusters, len(clusters), SendAllReadAll, RateLimitedRepairer(10, 0), 10, rp, nil)
	time.Sleep(3 * time.Second)

	// Keys should have been called for all clusters by now.
	for i, cluster := range clusters {
		if atomic.LoadInt32(&cluster.(*mockCluster).countKeys) == 0 {
			t.Errorf("Keys method not called in cluster %d.", i)
		}
	}
	// clusters[2] should have received all KSMs via read-repair.
	for _, ksm := range ksms {
		score, _, _ := clusters[2].Score(ksm.Key, ksm.Member)
		if score != ksm.Score {
			t.Errorf("Expected KSM %+v but found score %f.", ksm, score)
		}
	}
}
