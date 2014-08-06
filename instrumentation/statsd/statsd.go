// Package statsd implements a Instrumentation on a g2s.Statter.
package statsd

import (
	"time"

	"github.com/peterbourgon/g2s"
	"github.com/soundcloud/roshi/instrumentation"
)

// Satisfaction guaranteed.
var _ instrumentation.Instrumentation = statsdInstrumentation{}

type statsdInstrumentation struct {
	statter    g2s.Statter
	sampleRate float32
	prefix     string
}

// New returns a new Instrumentation that forwards metrics to statsd. All
// bucket names take the form e.g. "insert.record.count" and are prefixed with
// the common bucketPrefix.
func New(statter g2s.Statter, sampleRate float32, bucketPrefix string) instrumentation.Instrumentation {
	return statsdInstrumentation{
		statter:    statter,
		sampleRate: sampleRate,
		prefix:     bucketPrefix,
	}
}

func (i statsdInstrumentation) InsertCall() {
	i.statter.Counter(i.sampleRate, i.prefix+"insert.call.count", 1)
}

func (i statsdInstrumentation) InsertRecordCount(n int) {
	i.statter.Counter(i.sampleRate, i.prefix+"insert.record.count", n)
}

func (i statsdInstrumentation) InsertCallDuration(d time.Duration) {
	i.statter.Timing(i.sampleRate, i.prefix+"insert.call.duration", d)
}

func (i statsdInstrumentation) InsertRecordDuration(d time.Duration) {
	i.statter.Timing(i.sampleRate, i.prefix+"insert.record.duration", d)
}

func (i statsdInstrumentation) InsertQuorumFailure() {
	i.statter.Counter(i.sampleRate, i.prefix+"insert.quorum_failure.count", 1)
}

func (i statsdInstrumentation) SelectCall() {
	i.statter.Counter(i.sampleRate, i.prefix+"select.call.count", 1)
}

func (i statsdInstrumentation) SelectKeys(n int) {
	i.statter.Counter(i.sampleRate, i.prefix+"select.keys.count", n)
}

func (i statsdInstrumentation) SelectSendTo(n int) {
	i.statter.Counter(i.sampleRate, i.prefix+"select.send_to.count", n)
}

func (i statsdInstrumentation) SelectFirstResponseDuration(d time.Duration) {
	i.statter.Timing(i.sampleRate, i.prefix+"select.first_response.duration", d)
}

func (i statsdInstrumentation) SelectPartialError() {
	i.statter.Counter(i.sampleRate, i.prefix+"select.partial_error.count", 1)
}

func (i statsdInstrumentation) SelectBlockingDuration(d time.Duration) {
	i.statter.Timing(i.sampleRate, i.prefix+"select.blocking.duration", d)
}

func (i statsdInstrumentation) SelectOverheadDuration(d time.Duration) {
	i.statter.Timing(i.sampleRate, i.prefix+"select.overhead.duration", d)
}

func (i statsdInstrumentation) SelectDuration(d time.Duration) {
	i.statter.Timing(i.sampleRate, i.prefix+"select.duration", d)
}

func (i statsdInstrumentation) SelectSendAllPermitGranted() {
	i.statter.Counter(i.sampleRate, i.prefix+"select.send_all_permit_granted.count", 1)
}

func (i statsdInstrumentation) SelectSendAllPermitRejected() {
	i.statter.Counter(i.sampleRate, i.prefix+"select.send_all_permit_rejected.count", 1)
}

func (i statsdInstrumentation) SelectSendAllPromotion() {
	i.statter.Counter(i.sampleRate, i.prefix+"select.send_all_promotion.count", 1)
}

func (i statsdInstrumentation) SelectRetrieved(n int) {
	i.statter.Counter(i.sampleRate, i.prefix+"select.retrieved.count", n)
}

func (i statsdInstrumentation) SelectReturned(n int) {
	i.statter.Counter(i.sampleRate, i.prefix+"select.returned.count", n)
}

func (i statsdInstrumentation) SelectRepairNeeded(n int) {
	i.statter.Counter(i.sampleRate, i.prefix+"select.repair_needed.count", n)
}

func (i statsdInstrumentation) DeleteCall() {
	i.statter.Counter(i.sampleRate, i.prefix+"delete.call.count", 1)
}

func (i statsdInstrumentation) DeleteRecordCount(n int) {
	i.statter.Counter(i.sampleRate, i.prefix+"delete.record.count", n)
}

func (i statsdInstrumentation) DeleteCallDuration(d time.Duration) {
	i.statter.Timing(i.sampleRate, i.prefix+"delete.call.duration", d)
}

func (i statsdInstrumentation) DeleteRecordDuration(d time.Duration) {
	i.statter.Timing(i.sampleRate, i.prefix+"delete.record.duration", d)
}

func (i statsdInstrumentation) DeleteQuorumFailure() {
	i.statter.Counter(i.sampleRate, i.prefix+"delete.quorum_failure.count", 1)
}

func (i statsdInstrumentation) RepairCall() {
	i.statter.Counter(i.sampleRate, i.prefix+"repair.call.count", 1)
}

func (i statsdInstrumentation) RepairRequest(n int) {
	i.statter.Counter(i.sampleRate, i.prefix+"repair.request.count", n)
}

func (i statsdInstrumentation) RepairDiscarded(n int) {
	i.statter.Counter(i.sampleRate, i.prefix+"repair.discarded.count", n)
}

func (i statsdInstrumentation) RepairWriteSuccess(n int) {
	i.statter.Counter(i.sampleRate, i.prefix+"repair.write_success.count", n)
}

func (i statsdInstrumentation) RepairWriteFailure(n int) {
	i.statter.Counter(i.sampleRate, i.prefix+"repair.write_failure.count", n)
}

func (i statsdInstrumentation) WalkKeys(n int) {
	i.statter.Counter(i.sampleRate, i.prefix+"walk.keys.count", n)
}
