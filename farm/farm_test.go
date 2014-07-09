package farm

import (
	"reflect"
	"strings"
	"testing"

	"github.com/soundcloud/roshi/common"
)

func TestUnionDifferenceOfOne(t *testing.T) {
	inputSet := tupleSet{
		common.KeyScoreMember{Key: "a", Score: 5, Member: "a"}: struct{}{},
		common.KeyScoreMember{Key: "b", Score: 3, Member: "b"}: struct{}{},
		common.KeyScoreMember{Key: "c", Score: 1, Member: "c"}: struct{}{},
	}
	union, difference := unionDifference([]tupleSet{inputSet})
	if expected, got := inputSet, union; !reflect.DeepEqual(expected, got) {
		t.Errorf("union: expected %v, got %v", expected, got)
	}
	if len(difference) != 0 {
		t.Errorf("difference: expected empty set, got %v", difference)
	}
}

func TestUnionDifferenceFullResponse(t *testing.T) {
	for input, expected := range map[string]string{
		//
		//   cluster 1  A B C  A B C  A B C  A B -  A - -  A B -  A B -  A B -
		//   cluster 2  A B C  A B C  A B -  A - -  - - -  A - -  A - C  A - C
		//   cluster 3  A B C  A B    A - -  - - -  - - -  - - C  - B -  - B C
		//  ==========  =====  =====  =====  =====  =====  =====  =====  =====
		//       union  A B C  A B C  A B C  A B -  A - -  A B C  A B C  A B C
		//  difference  - - -  - - C  - B C  A B -  A - -  A B C  A B C  A B C
		//
		"A5B3C1 A5B3C1 A5B3C1": "A5B3C1 / ---",
		"A5B3C1 A5B3C1 A5B3--": "A5B3C1 / --C",
		"A5B3C1 A5B3-- A5----": "A5B3C1 / -BC",
		"A5B3-- A5---- ------": "A5B3-- / AB-",
		"A5---- ------ ------": "A5---- / A--",
		"A5B3-- A5---- ----C1": "A5B3C1 / ABC",
		"A5B3-- A5--C1 --B3--": "A5B3C1 / ABC",
		"A5B3-- A5--C1 --B3C1": "A5B3C1 / ABC",
	} {
		inputSets := s2tupleSets(t, input)
		expectedSets := s2pair(t, expected)
		union, difference := unionDifference(inputSets)
		if !reflect.DeepEqual(union, expectedSets.union) {
			t.Errorf("%s: union: expected %v, got %v", input, expectedSets.union, union)
		}
		if !reflect.DeepEqual(difference, expectedSets.difference) {
			t.Errorf("%s: difference: expected %v, got %v", input, expectedSets.difference, difference)
		}
	}
}

func TestUnionDifferenceMismatchedScores(t *testing.T) {
	for input, expected := range map[string]string{
		//
		//   cluster 1  A5 B3 C1  A5 B3 C1  A5 B3 C1  A5 B2 C1  A5 B3 C1
		//   cluster 2  A5 B3 C1  A5 B3 C1  A5 B2 C1  A5 B2 C1  A5 B2 --
		//   cluster 3  A5 B3 C1  A5 B2 C1  A5 B2 C1  A5 -- C1  A5 -- --
		//  ==========  ========  ========  ========  ========  ========
		//       union  A5 B3 C1  A5 B3 C1  A5 B3 C1  A5 B2 C1  A5 B3 C1
		//  difference  -  -  -   -  B  -   -  B  -   -  B  -   -  B  C
		//
		"A5B3C1 A5B3C1 A5B3C1": "A5B3C1 / ---",
		"A5B3C1 A5B3C1 A5B2C1": "A5B3C1 / -B-",
		"A5B3C1 A5B2C1 A5B2C1": "A5B3C1 / -B-",
		"A5B2C1 A5B2C1 A5--C1": "A5B2C1 / -B-",
		"A5B3C1 A5B2-- A5----": "A5B3C1 / -BC",
	} {
		inputSets := s2tupleSets(t, input)
		expectedSets := s2pair(t, expected)
		union, difference := unionDifference(inputSets)
		if !reflect.DeepEqual(union, expectedSets.union) {
			t.Errorf("%s: union: expected %v, got %v", input, expectedSets.union, union)
		}
		if !reflect.DeepEqual(difference, expectedSets.difference) {
			t.Errorf("%s: difference: expected %v, got %v", input, expectedSets.difference, difference)
		}
	}
}

func s2tupleSets(t *testing.T, s string) []tupleSet {
	a := []tupleSet{}
	for _, s := range strings.Split(s, " ") {
		a = append(a, s2tupleSet(t, s))
	}
	return a
}

func s2pair(t *testing.T, s string) testUnionDifferencePair {
	toks := strings.Split(s, "/")
	if len(toks) != 2 {
		t.Fatalf("invalid pair string %q", s)
	}
	return testUnionDifferencePair{
		union:      s2tupleSet(t, strings.TrimSpace(toks[0])),
		difference: s2keyMemberSet(t, strings.TrimSpace(toks[1])),
	}
}

type testUnionDifferencePair struct {
	union      tupleSet
	difference keyMemberSet
}

var (
	testTupleA5 = common.KeyScoreMember{Key: "a", Score: 5, Member: "a"}
	testTupleB3 = common.KeyScoreMember{Key: "b", Score: 3, Member: "b"}
	testTupleB2 = common.KeyScoreMember{Key: "b", Score: 2, Member: "b"}
	testTupleC1 = common.KeyScoreMember{Key: "c", Score: 1, Member: "c"}

	testKeyMemberA = common.KeyMember{Key: "a", Member: "a"}
	testKeyMemberB = common.KeyMember{Key: "b", Member: "b"}
	testKeyMemberC = common.KeyMember{Key: "c", Member: "c"}
)

func s2tupleSet(t *testing.T, s string) tupleSet {
	switch s {
	case "------":
		return tupleSet{}
	case "A5----":
		return tupleSet{testTupleA5: struct{}{}}
	case "--B3--":
		return tupleSet{testTupleB3: struct{}{}}
	case "----C1":
		return tupleSet{testTupleC1: struct{}{}}
	case "A5B3--":
		return tupleSet{testTupleA5: struct{}{}, testTupleB3: struct{}{}}
	case "A5B2--":
		return tupleSet{testTupleA5: struct{}{}, testTupleB2: struct{}{}}
	case "A5--C1":
		return tupleSet{testTupleA5: struct{}{}, testTupleC1: struct{}{}}
	case "--B3C1":
		return tupleSet{testTupleB3: struct{}{}, testTupleC1: struct{}{}}
	case "A5B3C1":
		return tupleSet{testTupleA5: struct{}{}, testTupleB3: struct{}{}, testTupleC1: struct{}{}}
	case "A5B2C1":
		return tupleSet{testTupleA5: struct{}{}, testTupleB2: struct{}{}, testTupleC1: struct{}{}}
	default:
		t.Fatalf("invalid set string %q", s)
	}
	return tupleSet{}
}

func s2keyMemberSet(t *testing.T, s string) keyMemberSet {
	switch s {
	case "---":
		return keyMemberSet{}
	case "A--":
		return keyMemberSet{testKeyMemberA: struct{}{}}
	case "-B-":
		return keyMemberSet{testKeyMemberB: struct{}{}}
	case "--C":
		return keyMemberSet{testKeyMemberC: struct{}{}}
	case "AB-":
		return keyMemberSet{testKeyMemberA: struct{}{}, testKeyMemberB: struct{}{}}
	case "-BC":
		return keyMemberSet{testKeyMemberB: struct{}{}, testKeyMemberC: struct{}{}}
	case "A-C":
		return keyMemberSet{testKeyMemberA: struct{}{}, testKeyMemberC: struct{}{}}
	case "ABC":
		return keyMemberSet{testKeyMemberA: struct{}{}, testKeyMemberB: struct{}{}, testKeyMemberC: struct{}{}}
	default:
		t.Fatalf("invalid set string %q", s)
	}
	return keyMemberSet{}
}
