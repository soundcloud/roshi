package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/gorilla/pat"
	"github.com/soundcloud/roshi/common"
)

func TestEvaluateScalarPercentage(t *testing.T) {
	for _, tuple := range []struct {
		s        string
		n        int
		expected int
		valid    bool
	}{
		{"", 3, -1, false},
		{"-1%", 3, -1, false},
		{"abc", 3, -1, false},

		{"0%", 3, -1, false},
		{"00%", 3, -1, false},
		{"110%", 3, -1, false},

		{"100%", 1, 1, true},
		{"100% ", 1, 1, true},
		{" 100%", 1, 1, true},
		{"	 100%			", 1, 1, true},
		{"99%", 1, 1, true},
		{"50%", 1, 1, true},
		{"1%", 1, 1, true},
		{"0%", 1, -1, false},

		{"100%", 3, 3, true},
		{"99%", 3, 3, true},
		{"67%", 3, 3, true},
		{"65%", 3, 2, true},
		{"51%", 3, 2, true},
		{"34%", 3, 2, true},
		{"32%", 3, 1, true},

		{"50%", 5, 3, true},
		{"50%", 10, 5, true},

		{"1", 1, 1, true},
		{"1", 2, 1, true},
		{"1", 3, 1, true},
		{"1", 99, 1, true},
		{"99", 1, -1, false},
	} {
		got, err := evaluateScalarPercentage(tuple.s, tuple.n)
		if (tuple.valid && err != nil) || (!tuple.valid && err == nil) {
			t.Errorf("%q of %d: expected valid=%v, got error '%v' (value %d)", tuple.s, tuple.n, tuple.valid, err, got)
			continue
		}
		if tuple.valid && tuple.expected != got {
			t.Errorf("%q of %d: expected %d, got %d", tuple.s, tuple.n, tuple.expected, got)
			continue
		}
	}
}

func TestHandleInsert(t *testing.T) {
	farm := newMockFarm()
	r := pat.New()
	r.Post("/", handleInsert(farm))
	server := httptest.NewServer(r)
	defer server.Close()

	requestBody, _ := json.Marshal([]common.KeyScoreMember{
		common.KeyScoreMember{Key: "foo", Score: 123, Member: "abc"},
		common.KeyScoreMember{Key: "bar", Score: 200, Member: "xxx"},
		common.KeyScoreMember{Key: "foo", Score: 456, Member: "def"},
		common.KeyScoreMember{Key: "bar", Score: 900, Member: "yyy"},
	})
	resp, err := http.Post(server.URL, "text/plain", bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, _ := ioutil.ReadAll(resp.Body)
	t.Log(strings.TrimSpace(string(respBody)))
	if resp.StatusCode != 200 {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}

	if expected, got := []common.KeyScoreMember{
		common.KeyScoreMember{Key: "foo", Score: 456, Member: "def"},
		common.KeyScoreMember{Key: "foo", Score: 123, Member: "abc"},
	}, farm.m["foo"]; !reflect.DeepEqual(expected, got) {
		t.Errorf("expected %+v, got %+v", expected, got)
	}

	if expected, got := []common.KeyScoreMember{
		common.KeyScoreMember{Key: "bar", Score: 900, Member: "yyy"},
		common.KeyScoreMember{Key: "bar", Score: 200, Member: "xxx"},
	}, farm.m["bar"]; !reflect.DeepEqual(expected, got) {
		t.Errorf("expected %+v, got %+v", expected, got)
	}
}

func TestSelectDefaults(t *testing.T) {
	server := fixtureServer()
	defer server.Close()

	body, _ := json.Marshal([][]byte{[]byte("foo"), []byte("bar")})
	req, _ := http.NewRequest("GET", server.URL, bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}

	var normalResponse struct {
		Records map[string][]common.KeyScoreMember `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&normalResponse); err != nil {
		t.Fatal(err)
	}
	if expected, got := map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			common.KeyScoreMember{Key: "foo", Score: 789, Member: "ghi"},
			common.KeyScoreMember{Key: "foo", Score: 456, Member: "def"},
			common.KeyScoreMember{Key: "foo", Score: 123, Member: "abc"},
		},
		"bar": []common.KeyScoreMember{
			common.KeyScoreMember{Key: "bar", Score: 750, Member: "zzz"},
			common.KeyScoreMember{Key: "bar", Score: 500, Member: "yyy"},
			common.KeyScoreMember{Key: "bar", Score: 250, Member: "xxx"},
		},
	}, normalResponse.Records; !reflect.DeepEqual(expected, got) {
		t.Errorf("expected %+v, got %+v", expected, got)
	}
}

func TestSelectOffsetLimit(t *testing.T) {
	server := fixtureServer()
	defer server.Close()

	body, _ := json.Marshal([][]byte{[]byte("foo"), []byte("bar")})
	req, _ := http.NewRequest("GET", server.URL+"?offset=1&limit=1", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}

	var normalResponse struct {
		Records map[string][]common.KeyScoreMember `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&normalResponse); err != nil {
		t.Fatal(err)
	}
	if expected, got := map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			common.KeyScoreMember{Key: "foo", Score: 456, Member: "def"},
		},
		"bar": []common.KeyScoreMember{
			common.KeyScoreMember{Key: "bar", Score: 500, Member: "yyy"},
		},
	}, normalResponse.Records; !reflect.DeepEqual(expected, got) {
		t.Errorf("expected %+v, got %+v", expected, got)
	}
}

func TestSelectCoalesce(t *testing.T) {
	server := fixtureServer()
	defer server.Close()

	body, _ := json.Marshal([][]byte{[]byte("foo"), []byte("bar")})
	req, _ := http.NewRequest("GET", server.URL+"?coalesce=true", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}

	var coalescedResponse struct {
		Records []common.KeyScoreMember `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&coalescedResponse); err != nil {
		t.Fatal(err)
	}
	if expected, got := []common.KeyScoreMember{
		common.KeyScoreMember{Key: "foo", Score: 789, Member: "ghi"},
		common.KeyScoreMember{Key: "bar", Score: 750, Member: "zzz"},
		common.KeyScoreMember{Key: "bar", Score: 500, Member: "yyy"},
		common.KeyScoreMember{Key: "foo", Score: 456, Member: "def"},
		common.KeyScoreMember{Key: "bar", Score: 250, Member: "xxx"},
		common.KeyScoreMember{Key: "foo", Score: 123, Member: "abc"},
	}, coalescedResponse.Records; !reflect.DeepEqual(expected, got) {
		t.Errorf("expected %+v, got %+v", expected, got)
	}
}

func TestSelectCoalesceOffsetLimit(t *testing.T) {
	server := fixtureServer()
	defer server.Close()

	body, _ := json.Marshal([][]byte{[]byte("foo"), []byte("bar")})
	req, _ := http.NewRequest("GET", server.URL+"?coalesce=true&offset=2&limit=2", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}

	var coalescedResponse struct {
		Records []common.KeyScoreMember `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&coalescedResponse); err != nil {
		t.Fatal(err)
	}
	if expected, got := []common.KeyScoreMember{
		common.KeyScoreMember{Key: "bar", Score: 500, Member: "yyy"},
		common.KeyScoreMember{Key: "foo", Score: 456, Member: "def"},
	}, coalescedResponse.Records; !reflect.DeepEqual(expected, got) {
		t.Errorf("expected %+v, got %+v", expected, got)
	}
}

func TestHandleDelete(t *testing.T) {
	server := fixtureServer()
	defer server.Close()

	// Make DELETE
	body, _ := json.Marshal([]common.KeyScoreMember{
		common.KeyScoreMember{Key: "foo", Score: 0, Member: "ghi"},
		common.KeyScoreMember{Key: "bar", Score: 0, Member: "yyy"},
	})
	req, _ := http.NewRequest("DELETE", server.URL, bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}

	// Make SELECT and confirm
	body, _ = json.Marshal([][]byte{[]byte("foo"), []byte("bar")})
	req, _ = http.NewRequest("GET", server.URL, bytes.NewReader(body))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}

	var normalResponse struct {
		Records map[string][]common.KeyScoreMember `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&normalResponse); err != nil {
		t.Fatal(err)
	}
	if expected, got := map[string][]common.KeyScoreMember{
		"foo": []common.KeyScoreMember{
			// common.KeyScoreMember{Key: "foo", Score: 789, Member: "ghi"}, // deleted
			common.KeyScoreMember{Key: "foo", Score: 456, Member: "def"},
			common.KeyScoreMember{Key: "foo", Score: 123, Member: "abc"},
		},
		"bar": []common.KeyScoreMember{
			common.KeyScoreMember{Key: "bar", Score: 750, Member: "zzz"},
			// common.KeyScoreMember{Key: "bar", Score: 500, Member: "yyy"}, // deleted
			common.KeyScoreMember{Key: "bar", Score: 250, Member: "xxx"},
		},
	}, normalResponse.Records; !reflect.DeepEqual(expected, got) {
		t.Errorf("expected %+v, got %+v", expected, got)
	}
}

func TestFlattenOrdering(t *testing.T) {
	// TODO(pb): need flattenOffset and flattenCursor
}

func fixtureServer() *httptest.Server {
	farm := newMockFarm()
	farm.Insert([]common.KeyScoreMember{
		common.KeyScoreMember{Key: "foo", Score: 123, Member: "abc"},
		common.KeyScoreMember{Key: "foo", Score: 456, Member: "def"},
		common.KeyScoreMember{Key: "foo", Score: 789, Member: "ghi"},
		common.KeyScoreMember{Key: "bar", Score: 250, Member: "xxx"},
		common.KeyScoreMember{Key: "bar", Score: 500, Member: "yyy"},
		common.KeyScoreMember{Key: "bar", Score: 750, Member: "zzz"},
	})
	r := pat.New()
	r.Post("/", handleInsert(farm))
	r.Get("/", handleSelect(farm))
	r.Delete("/", handleDelete(farm))
	return httptest.NewServer(r)
}

type mockFarm struct {
	m map[string][]common.KeyScoreMember
}

func newMockFarm() *mockFarm {
	return &mockFarm{
		m: map[string][]common.KeyScoreMember{},
	}
}

func (f *mockFarm) Insert(tuples []common.KeyScoreMember) error {
	for _, tuple := range tuples {
		newTuples := append(f.m[tuple.Key], tuple)
		sort.Sort(keyScoreMembers(newTuples))
		f.m[tuple.Key] = newTuples
	}
	return nil
}

func (f *mockFarm) SelectOffset(keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error) {
	m := map[string][]common.KeyScoreMember{}
	for _, key := range keys {
		m[key] = f.m[key]

		if len(m[key]) < offset {
			m[key] = []common.KeyScoreMember{}
			continue
		}
		m[key] = m[key][offset:]
		if len(m[key]) > limit {
			m[key] = m[key][:limit]
		}
	}
	return m, nil
}

func (f *mockFarm) SelectRange(keys []string, start, stop common.Cursor, limit int) (map[string][]common.KeyScoreMember, error) {
	return map[string][]common.KeyScoreMember{}, fmt.Errorf("not yet implemented")
}

func (f *mockFarm) Delete(tuples []common.KeyScoreMember) error {
	toDelete := map[string]map[string]bool{}
	for _, tuple := range tuples {
		if _, ok := toDelete[tuple.Key]; !ok {
			toDelete[tuple.Key] = map[string]bool{}
		}
		toDelete[tuple.Key][tuple.Member] = true
	}

	replacementMap := map[string][]common.KeyScoreMember{}
	for key, keyScoreMembers := range f.m {
		replacements := []common.KeyScoreMember{}
		for _, keyScoreMember := range keyScoreMembers {

			if _, ok := toDelete[key]; ok && toDelete[key][keyScoreMember.Member] {
				continue // delete
			}
			replacements = append(replacements, keyScoreMember)

		}
		replacementMap[key] = replacements
	}
	f.m = replacementMap

	return nil
}
