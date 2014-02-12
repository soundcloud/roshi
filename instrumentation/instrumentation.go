// Package instrumentation defines behaviors to instrument the CRDT stack.
package instrumentation

import (
	"time"
)

// Instrumentation describes the behaviors needed by the CRDT stack to report
// metrics about its performance.
type Instrumentation interface {
	InsertInstrumentation
	SelectInstrumentation
	DeleteInstrumentation
	KeysInstrumentation
	RepairInstrumentation
	WalkInstrumentation
}

// InsertInstrumentation describes metrics for the Insert path.
type InsertInstrumentation interface {
	InsertCall()                        // called for every invocation of Insert
	InsertRecordCount(int)              // +N, where N is how many records were provided to the Insert call
	InsertCallDuration(time.Duration)   // time spent per call
	InsertRecordDuration(time.Duration) // time spent per record (average)
	InsertQuorumFailure()               // called if the Insert failed due to lack of quorum
}

// SelectInstrumentation describes metrics for the Select path.
type SelectInstrumentation interface {
	SelectCall()                               // called for every invocation of Select
	SelectKeys(int)                            // how many keys were requested
	SelectSendTo(int)                          // how many clusters the read strategy sent the read to
	SelectFirstResponseDuration(time.Duration) // how long until we got the first element
	SelectPartialError()                       // called when an individual key gave an error from the cluster
	SelectBlockingDuration(time.Duration)      // time spent waiting for everything
	SelectOverheadDuration(time.Duration)      // time spent not waiting
	SelectDuration(time.Duration)              // overall time performing this read (blocking + overhead)
	SelectSendAllPromotion()                   // called when the read strategy promotes a "SendOne" to a "SendAll" because of missing results
	SelectRetrieved(int)                       // total number of KeyScoreMembers retrieved from the backing store
	SelectReturned(int)                        // total number of KeyScoreMembers returned to the caller
	SelectRepairNeeded(int)                    // +N, where N is every keyMember detected in a difference set (prior to entering repair strategy)
}

// DeleteInstrumentation describes metrics for the Delete path.
type DeleteInstrumentation interface {
	DeleteCall()                        // called for every invocation of Delete
	DeleteRecordCount(int)              // +N, where N is how many records were provided to the Delete call
	DeleteCallDuration(time.Duration)   // time spent per call
	DeleteRecordDuration(time.Duration) // time spent per record (average)
	DeleteQuorumFailure()               // called if the Delete failed due to lack of quorum
}

// KeysInstrumentation describes metrics for the Keys path.
type KeysInstrumentation interface {
	KeysFailure()           // called when an instance runs into an error and is therefore skipped
	KeysInstanceCompleted() // called when done scanning the keys of one instance (with or without errors)
	KeysClusterCompleted()  // called when done scanning the keys of a whole cluster (with or without errors)
}

// RepairInstrumentation describes metrics for Repairs.
type RepairInstrumentation interface {
	RepairCall()            // called for every requested repair
	RepairRequest(int)      // +N, where N is the total number of keyMembers for which repair was requested
	RepairDiscarded(int)    // +N, where N is keyMembers requested to repair but discarded due to e.g. rate limits
	RepairWriteSuccess(int) // +N, where N is keyMembers successfully written to a cluster as a result of a repair
	RepairWriteFailure(int) // +N, where N is keyMembers unsuccessfully written to a cluster as a result of a repair
}

// WalkInstrumentation describes metrics for walkers.
type WalkInstrumentation interface {
	WalkKeys(int) // +N, where N is the number of keys received from a Scanner and sent for Select
}

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

// KeysFailure satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) KeysFailure() {}

// KeysInstanceCompleted satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) KeysInstanceCompleted() {}

// KeysClusterCompleted satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) KeysClusterCompleted() {}

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
