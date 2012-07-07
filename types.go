package g2s

import (
	"fmt"
)

type StatType int

const (
	Counting StatType = iota
	Timing
	Gauge
)

type stat struct {
	statType     StatType
	sampled      bool
	samplingRate float32
}

func (st *stat) setSamplingRate(samplingRate float32) error {
	if samplingRate <= 0.0 || samplingRate > 1.0 {
		return fmt.Errorf("%.2f: must be 0 < rate <= 1.0", samplingRate)
	}
	st.sampled = true
	st.samplingRate = samplingRate
	return nil
}

//
//
//

type registration struct {
	bucket   string
	statType StatType
	err      chan error
}

//
//
//

type samplingChange struct {
	bucket       string
	samplingRate float32
	err          chan error
}

//
//
//

type update interface {
	Message() string
}

type counterUpdate struct {
	bucket string
	n      int
}

func (u *counterUpdate) Message() string {
	return fmt.Sprintf("%s:%d|c", u.bucket, u.n)
}

type timingUpdate struct {
	bucket string
	ms     int
}

func (u *timingUpdate) Message() string {
	return fmt.Sprintf("%s:%d|ms", u.bucket, u.ms)
}

type gaugeUpdate struct {
	bucket string
	val    string
}

func (u *gaugeUpdate) Message() string {
	return fmt.Sprintf("%s:%s|g", u.bucket, u.val)
}
