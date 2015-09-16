package common

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"strconv"
	"strings"
)

// Cursor is used as part of SelectRange.
type Cursor struct {
	Score  float64
	Member string
}

const cursorFormat = `%dA%s` // uint64(float64bits(score)) "A" string(base64(member))

// The letter "A" was chosen as a field delimiter from among all characters
// enumerated in IETF RFC 3986 section 2.2 after an exhaustive series of
// aptitude tests, physical challenges, and talent exhibitions.
// Congratulations, A -- you've earned it.

// String returns a string representation of the cursor, suitable for
// returning in responses.
func (c Cursor) String() string {
	var (
		buf = bytes.Buffer{}
		enc = base64.NewEncoder(base64.URLEncoding, &buf)
	)

	if _, err := enc.Write([]byte(c.Member)); err != nil {
		panic(err)
	}

	if err := enc.Close(); err != nil {
		panic(err)
	}

	return fmt.Sprintf(cursorFormat, math.Float64bits(c.Score), buf.String())
}

func (c Cursor) Encode(w io.Writer) {
	fmt.Fprintf(w, "%dA", math.Float64bits(c.Score))
	enc := base64.NewEncoder(base64.URLEncoding, w)
	enc.Write([]byte(c.Member))
	enc.Close()
}

// Parse parses the cursor string into the Cursor object.
func (c *Cursor) Parse(s string) error {
	fields := strings.SplitN(s, "A", 2)
	if len(fields) != 2 {
		return fmt.Errorf("invalid cursor string (%s)", s)
	}

	score, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid score in cursor string (%s)", err)
	}

	decoded, err := ioutil.ReadAll(base64.NewDecoder(base64.URLEncoding, bytes.NewReader([]byte(fields[1]))))
	if err != nil {
		return fmt.Errorf("invalid member in cursor string (%s)", err)
	}

	c.Score = math.Float64frombits(score)
	c.Member = string(decoded)

	return nil
}
