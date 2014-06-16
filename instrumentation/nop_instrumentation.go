package instrumentation

import "time"

// NopInstrumentation satisfies the Instrumentation interface but does no work.
type NopInstrumentation struct{}

// InsertCall satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) InsertCall() {}

// InsertRecordCount satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) InsertRecordCount(int) {}

// InsertCallDuration satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) InsertCallDuration(time.Duration) {}

// InsertRecordDuration satisfies the Instrumentation interface but does no
// work.
func (i NopInstrumentation) InsertRecordDuration(time.Duration) {}

// InsertQuorumFailure satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) InsertQuorumFailure() {}

// SelectCall satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) SelectCall() {}

// SelectKeys satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) SelectKeys(int) {}

// SelectSendTo satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) SelectSendTo(int) {}

// SelectFirstResponseDuration satisfies the Instrumentation interface but
// does no work.
func (i NopInstrumentation) SelectFirstResponseDuration(time.Duration) {}

// SelectPartialError satisfies the Instrumentation interface but does no
// work.
func (i NopInstrumentation) SelectPartialError() {}

// SelectBlockingDuration satisfies the Instrumentation interface but does no
// work.
func (i NopInstrumentation) SelectBlockingDuration(time.Duration) {}

// SelectOverheadDuration satisfies the Instrumentation interface but does no
// work.
func (i NopInstrumentation) SelectOverheadDuration(time.Duration) {}

// SelectDuration satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) SelectDuration(time.Duration) {}

// SelectSendAllPromotion satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) SelectSendAllPromotion() {}

// SelectRetrieved satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) SelectRetrieved(int) {}

// SelectReturned satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) SelectReturned(int) {}

// SelectRepairNeeded satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) SelectRepairNeeded(int) {}

// DeleteCall satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) DeleteCall() {}

// DeleteRecordCount satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) DeleteRecordCount(int) {}

// DeleteCallDuration satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) DeleteCallDuration(time.Duration) {}

// DeleteRecordDuration satisfies the Instrumentation interface but does no
// work.
func (i NopInstrumentation) DeleteRecordDuration(time.Duration) {}

// DeleteQuorumFailure satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) DeleteQuorumFailure() {}

// RepairCall satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) RepairCall() {}

// RepairRequest satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) RepairRequest(int) {}

// RepairDiscarded satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) RepairDiscarded(int) {}

// RepairWriteSuccess satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) RepairWriteSuccess(int) {}

// RepairWriteFailure satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) RepairWriteFailure(int) {}

// WalkKeys satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) WalkKeys(int) {}
