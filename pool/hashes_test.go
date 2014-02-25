package pool

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestMurmur3Correctness(t *testing.T) {
	if Murmur3("hello") != 0x248bfa47 {
		t.Fail()
	}
	if Murmur3("Winter is coming") != 0x43617e8f {
		t.Fail()
	}
}

const (
	stdevN         int     = 100000
	stdevModulo    uint32  = 64
	stdevTolerance float64 = 0.01 // as a percent of N
)

var stdevSuffixes = []string{
	"foo:bar:baz",
	"aaa:bbb:ccc",
	"ddd:eeefff:ghi",
	"abc:def:1234567",
	"012_231:xxx",
	"xy:ab:feff",
	"thing:or:other:thing",
	"aaaaaaa:fffcece:aaa",
	"0",
	"1",
	"2",
	"3",
	"4",
	"5",
	"6",
	"7",
	"8",
	"9",
}

func stdevKeygen() string {
	return fmt.Sprintf(
		"redis:%d:%s",
		rand.Intn(99999999),
		stdevSuffixes[rand.Intn(len(stdevSuffixes))],
	)
}

func testStdev(t *testing.T, hash func(string) uint32) {
	stdevVal := stdev(stdevKeygen, stdevN, hash, stdevModulo)
	stdevPct := (stdevVal * 100) / float64(stdevN)
	if stdevPct > (100 * stdevTolerance) {
		t.Fatalf("%s stdev %.4f (%.3f) exceeds tolerance %.3f", reflect.TypeOf(hash).Name(), stdevVal, stdevPct, stdevTolerance)
	}
	t.Logf("%s stdev %.4f (%.3f%%)", reflect.TypeOf(hash).Name(), stdevVal, stdevPct)
}

func TestMurmur3Stdev(t *testing.T) {
	testStdev(t, Murmur3)
}

func TestFNVStdev(t *testing.T) {
	testStdev(t, FNV)
}

func TestFNVaStdev(t *testing.T) {
	testStdev(t, FNVa)
}

func stdev(keyGenerator func() string, n int, hash func(string) uint32, modulo uint32) float64 {
	m := map[uint32]int{}
	for i := 0; i < n; i++ {
		s := keyGenerator()
		h := hash(s)
		v := h % modulo
		m[v]++
	}

	counts, total := make([]int, modulo), 0
	for i := 0; i < int(modulo); i++ {
		counts[i] = m[uint32(i)]
		total += m[uint32(i)]
	}
	mean := float64(total) / float64(modulo)

	sumSquares := 0.0
	for _, v := range m {
		square := math.Pow(math.Abs(float64(v)-mean), 2)
		sumSquares += square
	}

	averageSumSquares := sumSquares / float64(modulo)
	return math.Sqrt(averageSumSquares)
}

const benchmarkString = "redis:keyspace:123456"

func BenchmarkMurmur3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Murmur3(benchmarkString)
	}
}

func BenchmarkFNV(b *testing.B) {
	for i := 0; i < b.N; i++ {
		FNV(benchmarkString)
	}
}

func BenchmarkFNVa(b *testing.B) {
	for i := 0; i < b.N; i++ {
		FNVa(benchmarkString)
	}
}
