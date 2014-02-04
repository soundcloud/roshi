package farm

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
)

var (
	ksm = common.KeyScoreMember{Key: "key", Score: 3.14, Member: "member"}
)

func checkResult(result map[string][]common.KeyScoreMember, err error) error {
	if err != nil {
		return err
	}
	if expected, got := 0, len(result["nokey"]); expected != got {
		return fmt.Errorf("expected result length %v, got %v", expected, got)
	}
	if expected, got := 1, len(result["key"]); expected != got {
		return fmt.Errorf("expected result length %v, got %v", expected, got)
	}
	if expected, got := ksm, result["key"][0]; expected != got {
		return fmt.Errorf("expected ksm %v, got %v", expected, got)
	}
	return nil
}

func totalSelectCount(clusters []cluster.Cluster) int {
	var sum int
	for _, c := range clusters {
		sum += int(atomic.LoadInt32(&c.(*mockCluster).countSelect))
	}
	return sum
}

func totalOpenChannelCount(clusters []cluster.Cluster) int {
	var sum int
	for _, c := range clusters {
		sum += int(atomic.LoadInt32(&c.(*mockCluster).countOpenChannels))
	}
	return sum
}

// MockRepairer is similar to NopRepairer, but counts the keyMembers
// for which a repair was requested. This is useful in unit tests.
func MockRepairer(*Farm) coreRepairer { return &mockCoreRepairer{} }

type mockCoreRepairer struct{ requestCount int32 }

func (r *mockCoreRepairer) requestRepair(reqs []keyMember) {
	atomic.AddInt32(&r.requestCount, int32(len(reqs)))
}

func (r *mockCoreRepairer) close() {}

func TestSendOneReadOne(t *testing.T) {
	clusters := newMockClusters(3)
	farm := New(clusters, len(clusters), SendOneReadOne, MockRepairer, 0, nil, nil, nil)
	farm.Insert([]common.KeyScoreMember{ksm})

	result, err := farm.Select([]string{"key", "nokey"}, 0, 10)
	if err := checkResult(result, err); err != nil {
		t.Error(err)
	}
	if expected, got := 1, totalSelectCount(clusters); expected != got {
		t.Errorf("expected %d select calls, got %d", expected, got)
	}
	if expected, got := 0, int(atomic.LoadInt32(&farm.repairer.(*mockCoreRepairer).requestCount)); expected != got {
		t.Errorf("expected %d repairs, got %d", expected, got)
	}
	if totalOpenChannelCount(clusters) > 0 {
		t.Error("not all channels closed")
	}
}

func TestSendAllReadAll(t *testing.T) {
	clusters := newMockClusters(3)
	farm := New(clusters, len(clusters), SendAllReadAll, MockRepairer, 0, nil, nil, nil)
	farm.Insert([]common.KeyScoreMember{ksm})

	result, err := farm.Select([]string{"key", "nokey"}, 0, 10)
	if err := checkResult(result, err); err != nil {
		t.Error(err)
	}
	if expected, got := 3, totalSelectCount(clusters); expected != got {
		t.Errorf("expected %d select calls, got %d", expected, got)
	}
	if expected, got := 0, int(atomic.LoadInt32(&farm.repairer.(*mockCoreRepairer).requestCount)); expected != got {
		t.Errorf("expected %d repairs, got %d", expected, got)
	}

	// Now delete the ksm from one cluster and then read it again,
	// triggering a repair.
	clusters[0].Delete([]common.KeyScoreMember{ksm})
	result, err = farm.Select([]string{"key", "nokey"}, 0, 10)
	if err := checkResult(result, err); err != nil {
		t.Error(err)
	}
	if expected, got := 6, totalSelectCount(clusters); expected != got {
		t.Errorf("expected %d select calls, got %d", expected, got)
	}
	if expected, got := 1, int(atomic.LoadInt32(&farm.repairer.(*mockCoreRepairer).requestCount)); expected != got {
		t.Errorf("expected %d repairs, got %d", expected, got)
	}

	// Now replace cluster 0 with a failing one. No repairs should
	// happen. Result should still be returned as normal.
	clusters[0] = newFailingMockCluster()
	result, err = farm.Select([]string{"key", "nokey"}, 0, 10)
	if err := checkResult(result, err); err != nil {
		t.Error(err)
	}
	if expected, got := 7, totalSelectCount(clusters); expected != got {
		t.Errorf("expected %d select calls, got %d", expected, got)
	}
	if expected, got := 1, int(atomic.LoadInt32(&farm.repairer.(*mockCoreRepairer).requestCount)); expected != got {
		// Repair count still 1.
		t.Errorf("expected %d repairs, got %d", expected, got)
	}

	// Finally change the ksm in cluster 1 to one with a less
	// recent timestamp. The more recent ksm should be returned,
	// and a repair should be requested.
	clusters[1].Insert([]common.KeyScoreMember{
		common.KeyScoreMember{Key: "key", Score: 3.1, Member: "member"},
	})
	result, err = farm.Select([]string{"key", "nokey"}, 0, 10)
	if err := checkResult(result, err); err != nil {
		t.Error(err)
	}
	if expected, got := 10, totalSelectCount(clusters); expected != got {
		t.Errorf("expected %d select calls, got %d", expected, got)
	}
	if expected, got := 2, int(atomic.LoadInt32(&farm.repairer.(*mockCoreRepairer).requestCount)); expected != got {
		t.Errorf("expected %d repairs, got %d", expected, got)
	}
	if totalOpenChannelCount(clusters) > 0 {
		t.Error("not all channels closed")
	}
}

func TestSendAllReadFirstLinger(t *testing.T) {
	clusters := newMockClusters(3)
	farm := New(clusters, len(clusters), SendAllReadFirstLinger, MockRepairer, 0, nil, nil, nil)
	farm.Insert([]common.KeyScoreMember{ksm})

	result, err := farm.Select([]string{"key", "nokey"}, 0, 10)
	// Sleep to give the "lingering" goroutine a chance to run.
	time.Sleep(time.Millisecond)
	if err := checkResult(result, err); err != nil {
		t.Error(err)
	}
	if expected, got := 3, totalSelectCount(clusters); expected != got {
		t.Errorf("expected %d select calls, got %d", expected, got)
	}
	if expected, got := 0, int(atomic.LoadInt32(&farm.repairer.(*mockCoreRepairer).requestCount)); expected != got {
		t.Errorf("expected %d repairs, got %d", expected, got)
	}

	// Now delete the ksm from one cluster and then read it again,
	// triggering a repair. We ignore the result as it will
	// randomly come from cluster 0 or another one (that still has
	// the ksm).
	clusters[0].Delete([]common.KeyScoreMember{ksm})
	_, err = farm.Select([]string{"key", "nokey"}, 0, 10)
	if err != nil {
		t.Error(err)
	}
	// Sleep to give the "lingering" goroutine a chance to run.
	time.Sleep(time.Millisecond)
	if expected, got := 6, totalSelectCount(clusters); expected != got {
		t.Errorf("expected %d select calls, got %d", expected, got)
	}
	if expected, got := 1, int(atomic.LoadInt32(&farm.repairer.(*mockCoreRepairer).requestCount)); expected != got {
		t.Errorf("expected %d repairs, got %d", expected, got)
	}

	// Now replace cluster 0 with a failing one. No repairs should
	// happen. Result should again be returned reproducibly.
	clusters[0] = newFailingMockCluster()
	result, err = farm.Select([]string{"key", "nokey"}, 0, 10)
	if err != nil {
		t.Error(err)
	}
	// Sleep to give the "lingering" goroutine a chance to run.
	time.Sleep(time.Millisecond)
	if expected, got := 0, len(result["nokey"]); expected != got {
		t.Errorf("expected result length %v, got %v", expected, got)
	}
	if expected, got := 1, len(result["key"]); expected != got {
		t.Errorf("expected result length %v, got %v", expected, got)
	}
	if expected, got := ksm, result["key"][0]; expected != got {
		t.Errorf("expected ksm %v, got %v", expected, got)
	}
	if expected, got := 7, totalSelectCount(clusters); expected != got {
		t.Errorf("expected %d select calls, got %d", expected, got)
	}
	if expected, got := 1, int(atomic.LoadInt32(&farm.repairer.(*mockCoreRepairer).requestCount)); expected != got {
		// Repair count still 1.
		t.Errorf("expected %d repairs, got %d", expected, got)
	}

	// Finally change the ksm in cluster 1 to one with a less
	// recent timestamp. A random ksm will be returned (so ignore
	// it once more), and a repair should be requested.
	clusters[1].Insert([]common.KeyScoreMember{
		common.KeyScoreMember{Key: "key", Score: 3.1, Member: "member"},
	})
	_, err = farm.Select([]string{"key", "nokey"}, 0, 10)
	// Sleep to give the "lingering" goroutine a chance to run.
	time.Sleep(time.Millisecond)
	if err != nil {
		t.Error(err)
	}
	if expected, got := 10, totalSelectCount(clusters); expected != got {
		t.Errorf("expected %d select calls, got %d", expected, got)
	}
	if expected, got := 2, int(atomic.LoadInt32(&farm.repairer.(*mockCoreRepairer).requestCount)); expected != got {
		t.Errorf("expected %d repairs, got %d", expected, got)
	}

	if totalOpenChannelCount(clusters) > 0 {
		t.Error("not all channels closed")
	}
}

func TestSendVarReadFirstLinger(t *testing.T) {
	clusters := newMockClusters(3)
	rp := NewRatePolice(time.Second, 10)
	farm := New(
		clusters,
		len(clusters),
		SendVarReadFirstLinger(2, time.Millisecond, rp),
		MockRepairer,
		0,
		nil,
		rp,
		nil,
	)
	farm.Insert([]common.KeyScoreMember{ksm})

	result, err := farm.Select([]string{"key", "nokey"}, 0, 10)
	// Sleep to give the "lingering" goroutine a chance to run.
	time.Sleep(time.Millisecond)
	if err := checkResult(result, err); err != nil {
		t.Error(err)
	}
	if expected, got := 3, totalSelectCount(clusters); expected != got {
		t.Errorf("expected %d select calls, got %d", expected, got)
	}
	if expected, got := 0, int(atomic.LoadInt32(&farm.repairer.(*mockCoreRepairer).requestCount)); expected != got {
		t.Errorf("expected %d repairs, got %d", expected, got)
	}

	// Do the same again (within 1s). This time, it should do SendOne only.
	result, err = farm.Select([]string{"key", "nokey"}, 0, 10)
	// Sleep to give the "lingering" goroutine a chance to run.
	time.Sleep(time.Millisecond)
	if err := checkResult(result, err); err != nil {
		t.Error(err)
	}
	if expected, got := 4, totalSelectCount(clusters); expected != got {
		t.Errorf("expected %d select calls, got %d", expected, got)
	}
	if expected, got := 0, int(atomic.LoadInt32(&farm.repairer.(*mockCoreRepairer).requestCount)); expected != got {
		t.Errorf("expected %d repairs, got %d", expected, got)
	}

	// Still within the 1s, replace all clusters by
	// failingClusters. It should do SendOne again, but then
	// promote to SendAll because of the error returned. SendAll
	// will then give errors back, too, so an error should be
	// returned, and no repairs should be performed.
	for i := range clusters {
		clusters[i] = newFailingMockCluster()
	}
	result, err = farm.Select([]string{"key", "nokey"}, 0, 10)
	// Sleep to give the "lingering" goroutine a chance to run.
	time.Sleep(time.Millisecond)
	if err == nil {
		t.Error("Error expected but got nil.")
	}
	if expected, got := 3, totalSelectCount(clusters); expected != got {
		t.Errorf("expected %d select calls, got %d", expected, got)
	}
	if expected, got := 0, int(atomic.LoadInt32(&farm.repairer.(*mockCoreRepairer).requestCount)); expected != got {
		t.Errorf("expected %d repairs, got %d", expected, got)
	}

	if totalOpenChannelCount(clusters) > 0 {
		t.Error("not all channels closed")
	}
}
