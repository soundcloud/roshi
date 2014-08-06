package instrumentation

import "time"

// NopInstrumentation satisfies the Instrumentation interface.
type NopInstrumentation struct{}

// Satisfaction guaranteed.
var _ Instrumentation = NopInstrumentation{}

// InsertCall satisfies the Instrumentation interface.
func (i NopInstrumentation) InsertCall() {}

// InsertRecordCount satisfies the Instrumentation interface.
func (i NopInstrumentation) InsertRecordCount(int) {}

// InsertCallDuration satisfies the Instrumentation interface.
func (i NopInstrumentation) InsertCallDuration(time.Duration) {}

// InsertRecordDuration satisfies the Instrumentation interface.
func (i NopInstrumentation) InsertRecordDuration(time.Duration) {}

// InsertQuorumFailure satisfies the Instrumentation interface.
func (i NopInstrumentation) InsertQuorumFailure() {}

// SelectCall satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectCall() {}

// SelectKeys satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectKeys(int) {}

// SelectSendTo satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectSendTo(int) {}

// SelectFirstResponseDuration satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectFirstResponseDuration(time.Duration) {}

// SelectPartialError satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectPartialError() {}

// SelectBlockingDuration satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectBlockingDuration(time.Duration) {}

// SelectOverheadDuration satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectOverheadDuration(time.Duration) {}

// SelectDuration satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectDuration(time.Duration) {}

// SelectSendAllPermitGranted satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectSendAllPermitGranted() {}

// SelectSendAllPermitRejected satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectSendAllPermitRejected() {}

// SelectSendAllPromotion satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectSendAllPromotion() {}

// SelectRetrieved satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectRetrieved(int) {}

// SelectReturned satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectReturned(int) {}

// SelectRepairNeeded satisfies the Instrumentation interface.
func (i NopInstrumentation) SelectRepairNeeded(int) {}

// DeleteCall satisfies the Instrumentation interface.
func (i NopInstrumentation) DeleteCall() {}

// DeleteRecordCount satisfies the Instrumentation interface.
func (i NopInstrumentation) DeleteRecordCount(int) {}

// DeleteCallDuration satisfies the Instrumentation interface.
func (i NopInstrumentation) DeleteCallDuration(time.Duration) {}

// DeleteRecordDuration satisfies the Instrumentation interface.
func (i NopInstrumentation) DeleteRecordDuration(time.Duration) {}

// DeleteQuorumFailure satisfies the Instrumentation interface.
func (i NopInstrumentation) DeleteQuorumFailure() {}

// RepairCall satisfies the Instrumentation interface.
func (i NopInstrumentation) RepairCall() {}

// RepairRequest satisfies the Instrumentation interface.
func (i NopInstrumentation) RepairRequest(int) {}

// RepairDiscarded satisfies the Instrumentation interface.
func (i NopInstrumentation) RepairDiscarded(int) {}

// RepairWriteSuccess satisfies the Instrumentation interface.
func (i NopInstrumentation) RepairWriteSuccess(int) {}

// RepairWriteFailure satisfies the Instrumentation interface.
func (i NopInstrumentation) RepairWriteFailure(int) {}

// WalkKeys satisfies the Instrumentation interface.
func (i NopInstrumentation) WalkKeys(int) {}
