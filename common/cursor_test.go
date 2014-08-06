package common

import (
	"reflect"
	"testing"
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
