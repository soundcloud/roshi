package cluster_test

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/soundcloud/roshi/cluster"
	"github.com/soundcloud/roshi/common"
	"github.com/soundcloud/roshi/pool"
)

func TestInsertSelectOffsetKeys(t *testing.T) {
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
	m := map[string][]common.KeyScoreMember{}
	for e := range c.SelectOffset([]string{"foo", "bar", "baz"}, 0, 10) {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
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
	m = map[string][]common.KeyScoreMember{}
	for e := range c.SelectOffset([]string{"foo", "bar", "baz"}, 0, 1) {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
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
	m = map[string][]common.KeyScoreMember{}
	for e := range c.SelectOffset([]string{"foo", "bar", "baz"}, 1, 1) {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
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
	keysChannel := c.Keys(1)
	keys := map[string]bool{}
	for batch := range keysChannel {
		for _, key := range batch {
			keys[key] = true
		}
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
	c := integrationCluster(t, addresses, 3)

	// Make an inserts.
	if err := c.Insert([]common.KeyScoreMember{
		{"foo", 50, "alpha"},
		{"foo", 99, "beta"},
		{"foo", 11, "delta"},
	}); err != nil {
		t.Fatal(err)
	}

	// An older insert on foo-alpha should be rejected.
	c.Insert([]common.KeyScoreMember{{"foo", 48, "alpha"}})
	m := map[string][]common.KeyScoreMember{}
	for e := range c.SelectOffset([]string{"foo"}, 0, 10) {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
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
	c.Delete([]common.KeyScoreMember{{"foo", 49, "alpha"}})
	m = map[string][]common.KeyScoreMember{}
	for e := range c.SelectOffset([]string{"foo"}, 0, 10) {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
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
	c.Insert([]common.KeyScoreMember{{"foo", 50.2, "alpha"}})
	m = map[string][]common.KeyScoreMember{}
	for e := range c.SelectOffset([]string{"foo"}, 0, 10) {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
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
	c.Delete([]common.KeyScoreMember{{"foo", 50.3, "alpha"}})
	m = map[string][]common.KeyScoreMember{}
	for e := range c.SelectOffset([]string{"foo"}, 0, 10) {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
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
	c := integrationCluster(t, addresses, 3)

	// Make a bunch of inserts on a single key.
	if err := c.Insert([]common.KeyScoreMember{
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
	m := map[string][]common.KeyScoreMember{}
	for e := range c.SelectOffset([]string{"foo"}, 0, 10) {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
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
	c.Insert([]common.KeyScoreMember{{"foo", 51, "alpha"}})

	// Should have the same output with an updated score.
	m = map[string][]common.KeyScoreMember{}
	for e := range c.SelectOffset([]string{"foo"}, 0, 10) {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
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
	c.Insert([]common.KeyScoreMember{{"foo", 60, "woop"}})

	// Should have new output.
	m = map[string][]common.KeyScoreMember{}
	for e := range c.SelectOffset([]string{"foo"}, 0, 10) {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
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

func TestSelectRange(t *testing.T) {
	addresses := os.Getenv("TEST_REDIS_ADDRESSES")
	if addresses == "" {
		t.Logf("To run this test, set the TEST_REDIS_ADDRESSES environment variable")
		return
	}

	// Build a new cluster.
	c := integrationCluster(t, addresses, 1000)

	// Make a bunch of inserts.
	if err := c.Insert([]common.KeyScoreMember{
		{"foo", 50.1, "alpha"},
		{"foo", 99.2, "beta"},
		{"foo", 11.3, "delta"},
		{"foo", 45.4, "gamma"},
		{"foo", 21.5, "kappa"},
		{"foo", 76.6, "iota"},
		{"foo", 33.7, "sigma"},
		{"foo", 34.8, "omicron"},
		{"foo", 35.9, "nu"},
		{"bar", 66.6, "rho"},
		{"bar", 33.3, "psi"},
		{"bar", 99.9, "tau"},
	}); err != nil {
		t.Fatal(err)
	}

	// Middle of the list, a real element cursor.
	ch := c.SelectRange([]string{"foo"}, common.Cursor{Score: 45.4, Member: "gamma"}, common.Cursor{}, 100)
	expected := []common.KeyScoreMember{
		{"foo", 35.9, "nu"},
		{"foo", 34.8, "omicron"},
		{"foo", 33.7, "sigma"},
		{"foo", 21.5, "kappa"},
		{"foo", 11.3, "delta"},
	}
	e := <-ch
	if e.Error != nil {
		t.Fatalf("key %q: %s", e.Key, e.Error)
	}
	if got := e.KeyScoreMembers; !reflect.DeepEqual(expected, got) {
		t.Fatalf("key %q: expected \n\t%+v, got \n\t%+v", e.Key, expected, got)
	}
	if _, ok := <-ch; ok {
		t.Fatalf("key %q: expected 1 element on the channel, got multiple")
	}

	// Top of the list.
	ch = c.SelectRange([]string{"foo"}, common.Cursor{Score: math.MaxFloat64}, common.Cursor{}, 100)
	expected = []common.KeyScoreMember{
		{"foo", 99.2, "beta"},
		{"foo", 76.6, "iota"},
		{"foo", 50.1, "alpha"},
		{"foo", 45.4, "gamma"},
		{"foo", 35.9, "nu"},
		{"foo", 34.8, "omicron"},
		{"foo", 33.7, "sigma"},
		{"foo", 21.5, "kappa"},
		{"foo", 11.3, "delta"},
	}
	e = <-ch
	if e.Error != nil {
		t.Fatalf("key %q: %s", e.Key, e.Error)
	}
	if got := e.KeyScoreMembers; !reflect.DeepEqual(expected, got) {
		t.Fatalf("key %q: expected \n\t%+v, got \n\t%+v", e.Key, expected, got)
	}
	if _, ok := <-ch; ok {
		t.Fatalf("key %q: expected 1 element on the channel, got multiple")
	}

	// Restricted limit.
	ch = c.SelectRange([]string{"foo"}, common.Cursor{Score: 50.1, Member: "alpha"}, common.Cursor{}, 3)
	expected = []common.KeyScoreMember{
		{"foo", 45.4, "gamma"},
		{"foo", 35.9, "nu"},
		{"foo", 34.8, "omicron"},
	}
	e = <-ch
	if e.Error != nil {
		t.Fatalf("key %q: %s", e.Key, e.Error)
	}
	if got := e.KeyScoreMembers; !reflect.DeepEqual(expected, got) {
		t.Fatalf("key %q: expected \n\t%+v, got \n\t%+v", e.Key, expected, got)
	}
	if _, ok := <-ch; ok {
		t.Fatalf("key %q: expected 1 element on the channel, got multiple")
	}

	// Multiple keys, top of the list, all elements.
	ch = c.SelectRange([]string{"bar", "foo"}, common.Cursor{Score: math.MaxFloat64, Member: ""}, common.Cursor{}, 100)
	m := map[string][]common.KeyScoreMember{}
	for e := range ch {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	for key, expected := range map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			{"foo", 99.2, "beta"},
			{"foo", 76.6, "iota"},
			{"foo", 50.1, "alpha"},
			{"foo", 45.4, "gamma"},
			{"foo", 35.9, "nu"},
			{"foo", 34.8, "omicron"},
			{"foo", 33.7, "sigma"},
			{"foo", 21.5, "kappa"},
			{"foo", 11.3, "delta"},
		},
		"bar": []common.KeyScoreMember{
			{"bar", 99.9, "tau"},
			{"bar", 66.6, "rho"},
			{"bar", 33.3, "psi"},
		},
	} {
		if got := m[key]; !reflect.DeepEqual(expected, got) {
			t.Errorf("key %q: expected \n\t%v, got \n\t%v", key, expected, got)
			continue
		}
	}

	// Multiple keys, middle of the list, all elements.
	ch = c.SelectRange([]string{"bar", "foo"}, common.Cursor{Score: 66.6, Member: "rho"}, common.Cursor{}, 100)
	m = map[string][]common.KeyScoreMember{}
	for e := range ch {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	for key, expected := range map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			{"foo", 50.1, "alpha"},
			{"foo", 45.4, "gamma"},
			{"foo", 35.9, "nu"},
			{"foo", 34.8, "omicron"},
			{"foo", 33.7, "sigma"},
			{"foo", 21.5, "kappa"},
			{"foo", 11.3, "delta"},
		},
		"bar": []common.KeyScoreMember{
			{"bar", 33.3, "psi"},
		},
	} {
		if got := m[key]; !reflect.DeepEqual(expected, got) {
			t.Errorf("key %q: expected \n\t%v, got \n\t%v", key, expected, got)
			continue
		}
	}

	// Multiple keys, middle of the list, limited elements.
	ch = c.SelectRange([]string{"bar", "foo"}, common.Cursor{Score: 66.6, Member: "rho"}, common.Cursor{}, 1)
	m = map[string][]common.KeyScoreMember{}
	for e := range ch {
		if e.Error != nil {
			t.Errorf("during Select: key %q: %s", e.Key, e.Error)
		}
		m[e.Key] = e.KeyScoreMembers
	}
	for key, expected := range map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			{"foo", 50.1, "alpha"},
		},
		"bar": []common.KeyScoreMember{
			{"bar", 33.3, "psi"},
		},
	} {
		if got := m[key]; !reflect.DeepEqual(expected, got) {
			t.Errorf("key %q: expected \n\t%v, got \n\t%v", key, expected, got)
			continue
		}
	}

	// Top of the list, using the stopcursor.
	ch = c.SelectRange([]string{"foo"}, common.Cursor{Score: math.MaxFloat64}, common.Cursor{Score: 45.4, Member: "gamma"}, 100)
	expected = []common.KeyScoreMember{
		{"foo", 99.2, "beta"},
		{"foo", 76.6, "iota"},
		{"foo", 50.1, "alpha"},
	}
	e = <-ch
	if e.Error != nil {
		t.Fatalf("key %q: %s", e.Key, e.Error)
	}
	if got := e.KeyScoreMembers; !reflect.DeepEqual(expected, got) {
		t.Fatalf("key %q: expected \n\t%+v, got \n\t%+v", e.Key, expected, got)
	}
	if _, ok := <-ch; ok {
		t.Fatalf("key %q: expected 1 element on the channel, got multiple")
	}

	// Middle of the list, using the stopcursor.
	ch = c.SelectRange([]string{"foo"}, common.Cursor{Score: 35.9, Member: "nu"}, common.Cursor{Score: 21.5, Member: "kappa"}, 100)
	expected = []common.KeyScoreMember{
		{"foo", 34.8, "omicron"},
		{"foo", 33.7, "sigma"},
	}
	e = <-ch
	if e.Error != nil {
		t.Fatalf("key %q: %s", e.Key, e.Error)
	}
	if got := e.KeyScoreMembers; !reflect.DeepEqual(expected, got) {
		t.Fatalf("key %q: expected \n\t%+v, got \n\t%+v", e.Key, expected, got)
	}
	if _, ok := <-ch; ok {
		t.Fatalf("key %q: expected 1 element on the channel, got multiple")
	}
}

func TestCursorRetries(t *testing.T) {
	addresses := os.Getenv("TEST_REDIS_ADDRESSES")
	if addresses == "" {
		t.Logf("To run this test, set the TEST_REDIS_ADDRESSES environment variable")
		return
	}

	// Build a new cluster.
	c := integrationCluster(t, addresses, 1000)

	elements := []common.KeyScoreMember{}
	for i := 0; i < 50; i++ {
		elements = append(elements, common.KeyScoreMember{
			Key:    "foo",
			Score:  1.23,
			Member: fmt.Sprintf("%03d", i)},
		)
	}

	// Insert many elements with the same score.
	if err := c.Insert(elements); err != nil {
		t.Fatal(err)
	}

	// A Select with a low limit should still work, due to retries.
	element := <-c.SelectRange([]string{"foo"}, common.Cursor{Score: 1.23, Member: "003"}, common.Cursor{}, 5)
	if element.Error != nil {
		t.Errorf("got unexpected error: %s", element.Error)
	} else {
		t.Logf("OK: %v", element.KeyScoreMembers)
	}
}

func integrationCluster(t *testing.T, addresses string, maxSize int) cluster.Cluster {
	p := pool.New(
		strings.Split(addresses, ","),
		1*time.Second, // connect timeout
		1*time.Second, // read timeout
		1*time.Second, // write timeout
		10,            // max connections per instance
		pool.Murmur3,  // hash
	)

	for i := 0; i < p.Size(); i++ {
		p.WithIndex(i, func(conn redis.Conn) error {
			_, err := conn.Do("FLUSHDB")
			if err != nil {
				t.Fatal(err)
			}
			return nil
		})
	}

	return cluster.New(p, maxSize, 0, nil)
}
