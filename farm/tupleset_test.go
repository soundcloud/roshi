package farm

import (
	"reflect"
	"testing"

	"github.com/soundcloud/roshi/common"
)

func TestMakeSet(t *testing.T) {
	t1 := common.KeyScoreMember{Key: "k", Score: 1, Member: "v"}
	t2 := common.KeyScoreMember{Key: "x", Score: 1, Member: "z"}
	s1 := makeSet([]common.KeyScoreMember{t1, t2})
	s2 := tupleSet{}
	s2.add(t1)
	s2.add(t2)
	if !reflect.DeepEqual(s1, s2) {
		t.Errorf("\n%v not equal to\n%v", s1, s2)
	}
}

func TestAddHas(t *testing.T) {
	s := tupleSet{}
	t1 := common.KeyScoreMember{Key: "k", Score: 1, Member: "v"}
	t2 := common.KeyScoreMember{Key: "x", Score: 1, Member: "z"}
	s.add(t1)
	if expected, got := true, s.has(t1); expected != got {
		t.Errorf("%+v: expected %v, got %v", t1, expected, got)
	}
	if expected, got := false, s.has(t2); expected != got {
		t.Errorf("%+v: expected %v, got %v", t1, expected, got)
	}
}

func TestAddMany(t *testing.T) {
	t1 := common.KeyScoreMember{Key: "k", Score: 1, Member: "v"}
	t2 := common.KeyScoreMember{Key: "x", Score: 1, Member: "z"}
	t3 := common.KeyScoreMember{Key: "z", Score: 1, Member: "w"}
	s1 := makeSet([]common.KeyScoreMember{t1, t2})
	s2 := makeSet([]common.KeyScoreMember{t3})
	s1.addMany(s2)
	s3 := makeSet([]common.KeyScoreMember{t1, t2, t3})
	if !reflect.DeepEqual(s1, s3) {
		t.Errorf("\n%v not equal to\n%v", s1, s3)
	}
}

func TestOrderedLimitedSlice(t *testing.T) {
	t1 := common.KeyScoreMember{Key: "a", Score: 5, Member: "second"}
	t2 := common.KeyScoreMember{Key: "a", Score: 3, Member: "third"}
	t3 := common.KeyScoreMember{Key: "a", Score: 9, Member: "first"}
	s := makeSet([]common.KeyScoreMember{t1, t2, t3})

	got := s.orderedLimitedSlice(4)
	if expected := []common.KeyScoreMember{t3, t1, t2}; !reflect.DeepEqual(expected, got) {
		t.Errorf("expected\n%v, got\n%v", expected, got)
	}

	got = s.orderedLimitedSlice(3)
	if expected := []common.KeyScoreMember{t3, t1, t2}; !reflect.DeepEqual(expected, got) {
		t.Errorf("expected\n%v, got\n%v", expected, got)
	}

	got = s.orderedLimitedSlice(2)
	if expected := []common.KeyScoreMember{t3, t1}; !reflect.DeepEqual(expected, got) {
		t.Errorf("expected\n%v, got\n%v", expected, got)
	}

	got = s.orderedLimitedSlice(1)
	if expected := []common.KeyScoreMember{t3}; !reflect.DeepEqual(expected, got) {
		t.Errorf("expected\n%v, got\n%v", expected, got)
	}

	got = s.orderedLimitedSlice(0)
	if expected := []common.KeyScoreMember{}; !reflect.DeepEqual(expected, got) {
		t.Errorf("expected\n%v, got\n%v", expected, got)
	}
}
