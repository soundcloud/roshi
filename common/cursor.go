package common

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math"
)

// Cursor is used as part of SelectCursor.
type Cursor struct {
	Score  float64
	Member string
}

const cursorFormat = `%d!%s` // uint64(float64bits(score)) "!" string(base64(member))

// String returns a string representation of the cursor, suitable for
// returning in responses.
func (c Cursor) String() string {
	var (
		buf = bytes.Buffer{}
		enc = base64.NewEncoder(base64.StdEncoding, &buf)
	)
	if _, err := enc.Write([]byte(c.Member)); err != nil {
		panic(err)
	}
	if err := enc.Close(); err != nil {
		panic(err)
	}
	return fmt.Sprintf(cursorFormat, math.Float64bits(c.Score), buf.Bytes())
}

// Parse parses the cursor string into the Cursor object.
func (c *Cursor) Parse(s string) error {
	var (
		score  uint64
		member string
	)
	_, err := fmt.Fscanf(bytes.NewReader([]byte(s)), cursorFormat, &score, &member)
	if err != nil {
		return fmt.Errorf("invalid cursor string (%s)", err)
	}
	decoded, err := ioutil.ReadAll(base64.NewDecoder(base64.StdEncoding, bytes.NewReader([]byte(member))))
	if err != nil {
		return fmt.Errorf("invalid cursor string (%s)", err)
	}

	c.Score = math.Float64frombits(score)
	c.Member = string(decoded)
	return nil
}
