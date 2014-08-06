// Package plaintext implements an Instrumentation on an io.Writer.
package plaintext

import (
	"fmt"
	"io"
	"time"

	"github.com/soundcloud/roshi/instrumentation"
)

// Satisfaction guaranteed.
var _ instrumentation.Instrumentation = plaintextInstrumentation{}

type plaintextInstrumentation struct{ io.Writer }

// New returns a new Instrumentation that prints metrics to the passed
// io.Writer. All metrics are prefixed with an appropriate bucket name, and
// take the form e.g. "insert.record.count 10".
func New(w io.Writer) instrumentation.Instrumentation {
	return plaintextInstrumentation{w}
}

func (i plaintextInstrumentation) InsertCall() {
	fmt.Fprintf(i, "insert.call.count 1")
}

func (i plaintextInstrumentation) InsertRecordCount(n int) {
	fmt.Fprintf(i, "insert.record.count %d", n)
}

func (i plaintextInstrumentation) InsertCallDuration(d time.Duration) {
	fmt.Fprintf(i, "insert.call.duration_ms %d", d.Nanoseconds()/1e6)
}

func (i plaintextInstrumentation) InsertRecordDuration(d time.Duration) {
	fmt.Fprintf(i, "insert.record.duration_ms %d", d.Nanoseconds()/1e6)
}

func (i plaintextInstrumentation) InsertQuorumFailure() {
	fmt.Fprintf(i, "insert.quorum_failure.count 1")
}

func (i plaintextInstrumentation) SelectCall() {
	fmt.Fprintf(i, "select.call.count 1")
}

func (i plaintextInstrumentation) SelectKeys(n int) {
	fmt.Fprintf(i, "select.call.count %d", n)
}

func (i plaintextInstrumentation) SelectSendTo(n int) {
	fmt.Fprintf(i, "select.send_to.count %d", n)
}

func (i plaintextInstrumentation) SelectFirstResponseDuration(d time.Duration) {
	fmt.Fprintf(i, "select.first_response.duration_ms %d", d.Nanoseconds()/1e6)
}

func (i plaintextInstrumentation) SelectPartialError() {
	fmt.Fprintf(i, "select.partial_error.count 1")
}

func (i plaintextInstrumentation) SelectBlockingDuration(d time.Duration) {
	fmt.Fprintf(i, "select.blocking.duration_ms %d", d.Nanoseconds()/1e6)
}

func (i plaintextInstrumentation) SelectOverheadDuration(d time.Duration) {
	fmt.Fprintf(i, "select.overhead.duration_ms %d", d.Nanoseconds()/1e6)
}

func (i plaintextInstrumentation) SelectDuration(d time.Duration) {
	fmt.Fprintf(i, "select.duration_ms %d", d.Nanoseconds()/1e6)
}

func (i plaintextInstrumentation) SelectSendAllPermitGranted() {
	fmt.Fprintf(i, "select.send_all_permit_granted.count 1")
}

func (i plaintextInstrumentation) SelectSendAllPermitRejected() {
	fmt.Fprintf(i, "select.send_all_permit_rejected.count 1")
}

func (i plaintextInstrumentation) SelectSendAllPromotion() {
	fmt.Fprintf(i, "select.send_all_promotion.count 1")
}

func (i plaintextInstrumentation) SelectRetrieved(n int) {
	fmt.Fprintf(i, "select.retrieved.count %d", n)
}

func (i plaintextInstrumentation) SelectReturned(n int) {
	fmt.Fprintf(i, "select.returned.count %d", n)
}

func (i plaintextInstrumentation) SelectRepairNeeded(n int) {
	fmt.Fprintf(i, "select.repair_needed.count %d", n)
}

func (i plaintextInstrumentation) DeleteCall() {
	fmt.Fprintf(i, "delete.call.count 1")
}

func (i plaintextInstrumentation) DeleteRecordCount(n int) {
	fmt.Fprintf(i, "delete.record.count %d", n)
}

func (i plaintextInstrumentation) DeleteCallDuration(d time.Duration) {
	fmt.Fprintf(i, "delete.call.duration_ms %d", d.Nanoseconds()/1e6)
}

func (i plaintextInstrumentation) DeleteRecordDuration(d time.Duration) {
	fmt.Fprintf(i, "delete.record.duration_ms %d", d.Nanoseconds()/1e6)
}

func (i plaintextInstrumentation) DeleteQuorumFailure() {
	fmt.Fprintf(i, "delete.quorum_failure.count 1")
}

func (i plaintextInstrumentation) RepairCall() {
	fmt.Fprintf(i, "repair.call.count 1")
}

func (i plaintextInstrumentation) RepairRequest(n int) {
	fmt.Fprintf(i, "repair.request.count %d", n)
}

func (i plaintextInstrumentation) RepairDiscarded(n int) {
	fmt.Fprintf(i, "repair.discarded.count %d", n)
}

func (i plaintextInstrumentation) RepairWriteSuccess(n int) {
	fmt.Fprintf(i, "repair.write_success.count %d", n)
}

func (i plaintextInstrumentation) RepairWriteFailure(n int) {
	fmt.Fprintf(i, "repair.write_failure.count %d", n)
}

func (i plaintextInstrumentation) WalkKeys(n int) {
	fmt.Fprintf(i, "walk.keys.count %d", n)
}
