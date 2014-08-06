package instrumentation

import "time"

// MultiInstrumentation satisfies the Instrumentation interface by demuxing
// each call to multiple instrumentation targets.
type MultiInstrumentation struct {
	instrs []Instrumentation
}

// Satisfaction guaranteed.
var _ Instrumentation = MultiInstrumentation{}

// NewMultiInstrumentation creates a new MultiInstrumentation that will demux
// all calls to the provided Instrumentation targets.
func NewMultiInstrumentation(instrs ...Instrumentation) Instrumentation {
	return MultiInstrumentation{
		instrs: instrs,
	}
}

// InsertCall satisfies the Instrumentation interface.
func (i MultiInstrumentation) InsertCall() {
	for _, instr := range i.instrs {
		instr.InsertCall()
	}
}

// InsertRecordCount satisfies the Instrumentation interface.
func (i MultiInstrumentation) InsertRecordCount(n int) {
	for _, instr := range i.instrs {
		instr.InsertRecordCount(n)
	}
}

// InsertCallDuration satisfies the Instrumentation interface.
func (i MultiInstrumentation) InsertCallDuration(d time.Duration) {
	for _, instr := range i.instrs {
		instr.InsertCallDuration(d)
	}
}

// InsertRecordDuration satisfies the Instrumentation interface but does no
// work.
func (i MultiInstrumentation) InsertRecordDuration(d time.Duration) {
	for _, instr := range i.instrs {
		instr.InsertRecordDuration(d)
	}
}

// InsertQuorumFailure satisfies the Instrumentation interface.
func (i MultiInstrumentation) InsertQuorumFailure() {
	for _, instr := range i.instrs {
		instr.InsertQuorumFailure()
	}
}

// SelectCall satisfies the Instrumentation interface.
func (i MultiInstrumentation) SelectCall() {
	for _, instr := range i.instrs {
		instr.SelectCall()
	}
}

// SelectKeys satisfies the Instrumentation interface.
func (i MultiInstrumentation) SelectKeys(n int) {
	for _, instr := range i.instrs {
		instr.SelectKeys(n)
	}
}

// SelectSendTo satisfies the Instrumentation interface.
func (i MultiInstrumentation) SelectSendTo(n int) {
	for _, instr := range i.instrs {
		instr.SelectSendTo(n)
	}
}

// SelectFirstResponseDuration satisfies the Instrumentation interface but
// does no work.
func (i MultiInstrumentation) SelectFirstResponseDuration(d time.Duration) {
	for _, instr := range i.instrs {
		instr.SelectFirstResponseDuration(d)
	}
}

// SelectPartialError satisfies the Instrumentation interface but does no
// work.
func (i MultiInstrumentation) SelectPartialError() {
	for _, instr := range i.instrs {
		instr.SelectPartialError()
	}
}

// SelectBlockingDuration satisfies the Instrumentation interface but does no
// work.
func (i MultiInstrumentation) SelectBlockingDuration(d time.Duration) {
	for _, instr := range i.instrs {
		instr.SelectBlockingDuration(d)
	}
}

// SelectOverheadDuration satisfies the Instrumentation interface but does no
// work.
func (i MultiInstrumentation) SelectOverheadDuration(d time.Duration) {
	for _, instr := range i.instrs {
		instr.SelectOverheadDuration(d)
	}
}

// SelectDuration satisfies the Instrumentation interface.
func (i MultiInstrumentation) SelectDuration(d time.Duration) {
	for _, instr := range i.instrs {
		instr.SelectDuration(d)
	}
}

// SelectSendAllPermitGranted satisfies the Instrumentation interface.
func (i MultiInstrumentation) SelectSendAllPermitGranted() {
	for _, instr := range i.instrs {
		instr.SelectSendAllPermitGranted()
	}
}

// SelectSendAllPermitRejected satisfies the Instrumentation interface.
func (i MultiInstrumentation) SelectSendAllPermitRejected() {
	for _, instr := range i.instrs {
		instr.SelectSendAllPermitRejected()
	}
}

// SelectSendAllPromotion satisfies the Instrumentation interface.
func (i MultiInstrumentation) SelectSendAllPromotion() {
	for _, instr := range i.instrs {
		instr.SelectSendAllPromotion()
	}
}

// SelectRetrieved satisfies the Instrumentation interface.
func (i MultiInstrumentation) SelectRetrieved(n int) {
	for _, instr := range i.instrs {
		instr.SelectRetrieved(n)
	}
}

// SelectReturned satisfies the Instrumentation interface.
func (i MultiInstrumentation) SelectReturned(n int) {
	for _, instr := range i.instrs {
		instr.SelectReturned(n)
	}
}

// SelectRepairNeeded satisfies the Instrumentation interface.
func (i MultiInstrumentation) SelectRepairNeeded(n int) {
	for _, instr := range i.instrs {
		instr.SelectRepairNeeded(n)
	}
}

// DeleteCall satisfies the Instrumentation interface.
func (i MultiInstrumentation) DeleteCall() {
	for _, instr := range i.instrs {
		instr.DeleteCall()
	}
}

// DeleteRecordCount satisfies the Instrumentation interface.
func (i MultiInstrumentation) DeleteRecordCount(n int) {
	for _, instr := range i.instrs {
		instr.DeleteRecordCount(n)
	}
}

// DeleteCallDuration satisfies the Instrumentation interface.
func (i MultiInstrumentation) DeleteCallDuration(d time.Duration) {
	for _, instr := range i.instrs {
		instr.DeleteCallDuration(d)
	}
}

// DeleteRecordDuration satisfies the Instrumentation interface but does no
// work.
func (i MultiInstrumentation) DeleteRecordDuration(d time.Duration) {
	for _, instr := range i.instrs {
		instr.DeleteRecordDuration(d)
	}
}

// DeleteQuorumFailure satisfies the Instrumentation interface.
func (i MultiInstrumentation) DeleteQuorumFailure() {
	for _, instr := range i.instrs {
		instr.DeleteQuorumFailure()
	}
}

// RepairCall satisfies the Instrumentation interface.
func (i MultiInstrumentation) RepairCall() {
	for _, instr := range i.instrs {
		instr.RepairCall()
	}
}

// RepairRequest satisfies the Instrumentation interface.
func (i MultiInstrumentation) RepairRequest(n int) {
	for _, instr := range i.instrs {
		instr.RepairRequest(n)
	}
}

// RepairDiscarded satisfies the Instrumentation interface.
func (i MultiInstrumentation) RepairDiscarded(n int) {
	for _, instr := range i.instrs {
		instr.RepairDiscarded(n)
	}
}

// RepairWriteSuccess satisfies the Instrumentation interface.
func (i MultiInstrumentation) RepairWriteSuccess(n int) {
	for _, instr := range i.instrs {
		instr.RepairWriteSuccess(n)
	}
}

// RepairWriteFailure satisfies the Instrumentation interface.
func (i MultiInstrumentation) RepairWriteFailure(n int) {
	for _, instr := range i.instrs {
		instr.RepairWriteFailure(n)
	}
}

// WalkKeys satisfies the Instrumentation interface.
func (i MultiInstrumentation) WalkKeys(n int) {
	for _, instr := range i.instrs {
		instr.WalkKeys(n)
	}
}
