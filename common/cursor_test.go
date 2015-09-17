package common

import (
	"bytes"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestCursorSerialization(t *testing.T) {
	for _, cursor := range []Cursor{
		Cursor{Score: 0.0, Member: " "},
		Cursor{Score: 1.1, Member: `%20`},
		Cursor{Score: 123.456, Member: "abc"},
		Cursor{Score: 0.00001, Member: "foo\x00bar"}, // catch missing enc.Close()
	} {
		var (
			s   = cursor.String()
			got = Cursor{}
		)
		if err := got.Parse(s); err != nil {
			t.Errorf("%#+v: serialized to %q, parse failed: %s", cursor, s, err)
			continue
		}
		if !reflect.DeepEqual(cursor, got) {
			t.Errorf("%#+v: serialized to %q, parsed back to %#+v", cursor, s, got)
			continue
		}
		t.Logf("%#+v: %q", cursor, s)
	}
}

func TestCursorSafety(t *testing.T) {
	var (
		scores = []float64{
			0.0,
			1.23,
			float64(time.Now().UnixNano()) / 1e9,
			math.MaxFloat64,
			math.SmallestNonzeroFloat64,
		}
		members = [][]byte{
			[]byte{0},
			[]byte{0, 0, 0},
			[]byte{1, 2, 3},
			[]byte{12, 34, 56, 78, 90},
			[]byte{255},
			[]byte{255, 0, 0, 0},
			[]byte{255, 0, 128, 0, 64, 0, 32, 0, 16, 0, 8, 0, 4, 0, 2, 0},
		}
	)

	for _, score := range scores {
		for _, member := range members {
			in := Cursor{Score: score, Member: string(member)}
			serialized := in.String()

			var out Cursor
			if err := out.Parse(serialized); err != nil {
				t.Errorf("score %f member %v: when parsing: %s", score, member, err)
				continue
			}

			if !reflect.DeepEqual(in, out) {
				t.Errorf("score %f member %v: after parsing, want %+v, have %+v", score, member, in, out)
				continue
			}
		}
	}
}

func TestIssue37(t *testing.T) {
	c := Cursor{}
	if err := c.Parse("4743834931740803072A"); err != nil {
		t.Fatal(err)
	}
	if want, have := "", c.Member; want != have {
		t.Errorf("want %q, have %q", want, have)
	}
}

func BenchmarkCursorString(b *testing.B) {
	var cursor = Cursor{Score: 1.23, Member: "abcdefg"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = cursor.String()
	}

	b.ReportAllocs()
}

func BenchmarkCursorEncode(b *testing.B) {
	var (
		cursor = Cursor{Score: 1.23, Member: "abcdefg"}
		dst    = &bytes.Buffer{}
	)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cursor.Encode(dst)
	}

	b.ReportAllocs()
}
