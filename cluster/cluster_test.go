package cluster_test

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
	"github.com/soundcloud/roshi/shard"
	"github.com/garyburd/redigo/redis"
)

func TestInsertSelectKeys(t *testing.T) {
	addresses := os.Getenv("TEST_REDIS_ADDRESSES")
	if addresses == "" {
		t.Logf("To run this test, set the TEST_REDIS_ADDRESSES environment variable")
		return
	}

	// Build a new cluster with a high max size.
	c := integrationCluster(t, addresses, 1000)

	// Make a bunch of inserts.
	if err := c.Insert([]common.KeyScoreMember{
		{"foo", 50, "alpha"},
		{"foo", 99, "beta"},
		{"foo", 11, "delta"},
		{"bar", 45, "gamma"},
		{"bar", 21, "kappa"},
		{"bar", 76, "iota"},
		{"baz", 33, "sigma"},
		{"baz", 34, "omicron"},
		{"baz", 35, "nu"},
	}); err != nil {
		t.Fatal(err)
	}

	// Select everything.
	ch := c.Select([]string{"foo", "bar", "baz"}, 0, 10)
	m := map[string][]common.KeyScoreMember{}
	for e := range ch {
		if e.Error != nil {
			t.Errorf("during Select: key '%s': %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	for key, expected := range map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			{"foo", 99, "beta"},
			{"foo", 50, "alpha"},
			{"foo", 11, "delta"},
		},
		"bar": []common.KeyScoreMember{
			{"bar", 76, "iota"},
			{"bar", 45, "gamma"},
			{"bar", 21, "kappa"},
		},
		"baz": []common.KeyScoreMember{
			{"baz", 35, "nu"},
			{"baz", 34, "omicron"},
			{"baz", 33, "sigma"},
		},
	} {
		if got := m[key]; !reflect.DeepEqual(expected, got) {
			t.Errorf("%s: expected\n %v, got\n %v", key, expected, got)
		}
		t.Logf("%s: %v OK", key, expected)
	}

	// Just select the first element from each key.
	ch = c.Select([]string{"foo", "bar", "baz"}, 0, 1)
	m = map[string][]common.KeyScoreMember{}
	for e := range ch {
		if e.Error != nil {
			t.Errorf("during Select: key '%s': %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	for key, expected := range map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			{"foo", 99, "beta"},
		},
		"bar": []common.KeyScoreMember{
			{"bar", 76, "iota"},
		},
		"baz": []common.KeyScoreMember{
			{"baz", 35, "nu"},
		},
	} {
		if got := m[key]; !reflect.DeepEqual(expected, got) {
			t.Errorf("%s: expected\n %v, got\n %v", key, expected, got)
			continue
		}
		t.Logf("%s: %v OK", key, expected)
	}

	// Just select the second element from each key.
	ch = c.Select([]string{"foo", "bar", "baz"}, 1, 1)
	m = map[string][]common.KeyScoreMember{}
	for e := range ch {
		if e.Error != nil {
			t.Errorf("during Select: key '%s': %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	for key, expected := range map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			{"foo", 50, "alpha"},
		},
		"bar": []common.KeyScoreMember{
			{"bar", 45, "gamma"},
		},
		"baz": []common.KeyScoreMember{
			{"baz", 34, "omicron"},
		},
	} {
		if got := m[key]; !reflect.DeepEqual(expected, got) {
			t.Errorf("%s: expected\n %v, got\n %v", key, expected, got)
			continue
		}
		t.Logf("%s: %v OK", key, expected)
	}
	keysChannel := c.Keys()
	keys := map[string]bool{}
	for key := range keysChannel {
		keys[key] = true
	}
	if got, expected := keys, map[string]bool{"foo": true, "bar": true, "baz": true}; !reflect.DeepEqual(got, expected) {
		t.Errorf("Expected key set %+v, got %+v", expected, got)
	}
}

func TestInsertIdempotency(t *testing.T) {
	addresses := os.Getenv("TEST_REDIS_ADDRESSES")
	if addresses == "" {
		t.Logf("To run this test, set the TEST_REDIS_ADDRESSES environment variable")
		return
	}

	// Build a new cluster with a low max size.
	z := integrationCluster(t, addresses, 3)

	// Make an inserts.
	if err := z.Insert([]common.KeyScoreMember{
		{"foo", 50, "alpha"},
		{"foo", 99, "beta"},
		{"foo", 11, "delta"},
	}); err != nil {
		t.Fatal(err)
	}

	// An older insert on foo-alpha should be rejected.
	z.Insert([]common.KeyScoreMember{{"foo", 48, "alpha"}})
	c := z.Select([]string{"foo"}, 0, 10)
	m := map[string][]common.KeyScoreMember{}
	for e := range c {
		if e.Error != nil {
			t.Errorf("during Select: key '%s': %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	if expected, got := []common.KeyScoreMember{
		{"foo", 99, "beta"},
		{"foo", 50, "alpha"},
		{"foo", 11, "delta"},
	}, m["foo"]; !reflect.DeepEqual(expected, got) {
		t.Fatalf("after older insert, expected\n %v, got\n %v", expected, got)
	}

	// An older delete on foo-alpha should be rejected
	z.Delete([]common.KeyScoreMember{{"foo", 49, "alpha"}})
	c = z.Select([]string{"foo"}, 0, 10)
	m = map[string][]common.KeyScoreMember{}
	for e := range c {
		if e.Error != nil {
			t.Errorf("during Select: key '%s': %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	if expected, got := []common.KeyScoreMember{
		{"foo", 99, "beta"},
		{"foo", 50, "alpha"},
		{"foo", 11, "delta"},
	}, m["foo"]; !reflect.DeepEqual(expected, got) {
		t.Fatalf("after older delete, expected\n %v, got\n %v", expected, got)
	}

	// A newer insert on foo-alpha should be accepted.
	z.Insert([]common.KeyScoreMember{{"foo", 50.2, "alpha"}})
	c = z.Select([]string{"foo"}, 0, 10)
	m = map[string][]common.KeyScoreMember{}
	for e := range c {
		if e.Error != nil {
			t.Errorf("during Select: key '%s': %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	if expected, got := []common.KeyScoreMember{
		{"foo", 99, "beta"},
		{"foo", 50.2, "alpha"},
		{"foo", 11, "delta"},
	}, m["foo"]; !reflect.DeepEqual(expected, got) {
		t.Fatalf("after newer insert, expected\n %v, got\n %v", expected, got)
	}

	// A newer delete on foo-alpha should be accepted.
	z.Delete([]common.KeyScoreMember{{"foo", 50.3, "alpha"}})
	c = z.Select([]string{"foo"}, 0, 10)
	m = map[string][]common.KeyScoreMember{}
	for e := range c {
		if e.Error != nil {
			t.Errorf("during Select: key '%s': %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	if expected, got := []common.KeyScoreMember{
		{"foo", 99, "beta"},
		{"foo", 11, "delta"},
	}, m["foo"]; !reflect.DeepEqual(expected, got) {
		t.Fatalf("after newer delete, expected\n %v, got\n %v", expected, got)
	}
}

func TestInsertMaxSize(t *testing.T) {
	addresses := os.Getenv("TEST_REDIS_ADDRESSES")
	if addresses == "" {
		t.Logf("To run this test, set the TEST_REDIS_ADDRESSES environment variable")
		return
	}

	// Build a new cluster with a low max size.
	z := integrationCluster(t, addresses, 3)

	// Make a bunch of inserts on a single key.
	if err := z.Insert([]common.KeyScoreMember{
		{"foo", 50, "alpha"},
		{"foo", 99, "beta"},
		{"foo", 11, "delta"},
		{"foo", 45, "gamma"},
		{"foo", 21, "kappa"},
		{"foo", 76, "iota"},
		{"foo", 33, "sigma"},
		{"foo", 34, "omicron"},
		{"foo", 35, "nu"},
	}); err != nil {
		t.Fatal(err)
	}

	// Select everything.
	c := z.Select([]string{"foo"}, 0, 10)
	m := map[string][]common.KeyScoreMember{}
	for e := range c {
		if e.Error != nil {
			t.Errorf("during Select: key '%s': %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	for key, expected := range map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			{"foo", 99, "beta"},
			{"foo", 76, "iota"},
			{"foo", 50, "alpha"},
		},
	} {
		if got := m[key]; !reflect.DeepEqual(expected, got) {
			t.Errorf("%s: expected\n %v, got\n %v", key, expected, got)
			continue
		}
		t.Logf("%s: %v OK", key, expected)
	}

	// Make another single insert with a new score, overwriting a key.
	z.Insert([]common.KeyScoreMember{{"foo", 51, "alpha"}})

	// Should have the same output with an updated score.
	c = z.Select([]string{"foo"}, 0, 10)
	m = map[string][]common.KeyScoreMember{}
	for e := range c {
		if e.Error != nil {
			t.Errorf("during Select: key '%s': %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	for key, expected := range map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			{"foo", 99, "beta"},
			{"foo", 76, "iota"},
			{"foo", 51, "alpha"},
		},
	} {
		if got := m[key]; !reflect.DeepEqual(expected, got) {
			t.Errorf("%s: expected\n %v, got\n %v", key, expected, got)
			continue
		}
		t.Logf("%s: %v OK", key, expected)
	}

	// Make another single insert of a brand new key.
	z.Insert([]common.KeyScoreMember{{"foo", 60, "woop"}})

	// Should have new output.
	c = z.Select([]string{"foo"}, 0, 10)
	m = map[string][]common.KeyScoreMember{}
	for e := range c {
		if e.Error != nil {
			t.Errorf("during Select: key '%s': %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	for key, expected := range map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			{"foo", 99, "beta"},
			{"foo", 76, "iota"},
			{"foo", 60, "woop"},
		},
	} {
		if got := m[key]; !reflect.DeepEqual(expected, got) {
			t.Errorf("%s: expected\n %v, got\n %v", key, expected, got)
			continue
		}
		t.Logf("%s: %v OK", key, expected)
	}
}

func TestJSONMarshalling(t *testing.T) {
	ksm := common.KeyScoreMember{
		Key:    "This is incorrect UTF-8: " + string([]byte{0, 192, 0, 193}),
		Score:  99,
		Member: "This is still incorrect UTF-8: " + string([]byte{0, 192, 0, 193}),
	}
	jsonData, err := json.Marshal(ksm)
	if err != nil {
		t.Fatal(err)
	}
	var unmarshalledKSM common.KeyScoreMember
	if err := json.Unmarshal(jsonData, &unmarshalledKSM); err != nil {
		t.Fatal(err)
	}
	if ksm != unmarshalledKSM {
		t.Errorf(
			"JSON marschalling/unmarshalling roundtrip failed. Original: %v, JSON: %v, result: %v",
			ksm,
			jsonData,
			unmarshalledKSM,
		)
	}
}

func integrationCluster(t *testing.T, addresses string, maxSize int) cluster.Cluster {
	s := shard.New(
		strings.Split(addresses, ","),
		1*time.Second, // connect timeout
		1*time.Second, // read timeout
		1*time.Second, // write timeout
		10,            // max connections per instance
		shard.Murmur3, // hash
	)

	for i := 0; i < s.Size(); i++ {
		s.WithIndex(i, func(conn redis.Conn) error {
			_, err := conn.Do("FLUSHDB")
			if err != nil {
				t.Fatal(err)
			}
			return nil
		})
	}

	return cluster.New(s, maxSize, nil)
}
