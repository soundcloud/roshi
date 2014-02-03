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
	SelectFirstResponseDuration(time.Duration) // how long until we got the first element
	SelectPartialError()                       // called when an individual key gave an error from the cluster
	SelectBlockingDuration(time.Duration)      // time spent waiting for everything
	SelectOverheadDuration(time.Duration)      // time spent not waiting
	SelectDuration(time.Duration)              // overall time performing this read (blocking + overhead)
	SelectSendAllPromotion()                   // called when the read strategy promotes a "SendOne" to a "SendAll" because of missing results
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
	KeysFarmCompleted()     // called when done scanning the keys of a whole farm (with or without errors)
	KeysThrottled()         // called when key scanning is stopped (for 1s) to not interfere with actual queries
}

// RepairInstrumentation describes metrics for Repairs.
type RepairInstrumentation interface {
	RepairCall()                       // called for every invocation of requestRepair
	RepairRequestCount(int)            // +N, where N is the total number of keyMembers for which repair was requested
	RepairDiscarded(int)               // +N, where N is the total number of keyMembers for which the repair request was discarded by the Repairer
	RepairCheckPartialFailure()        // called when a single cluster returns a read error during a check
	RepairCheckCompleteFailure()       // called when no cluster returned any data during a check (hence no repair could be performed)
	RepairCheckDuration(time.Duration) // time spent checking
	RepairCheckRedundant()             // called when a repair was requested but the check revealed that no action is required
	RepairWriteCount()                 // called when the check revealed that a repair is required (i.e. a KeyScoreMember has to be written)
	RepairWriteSuccess()               // called (separately per cluster) after a KeyScoreMember was successfully written
	RepairWriteFailure()               // called (separately per cluster) after writing a KeyScoreMember failed
	RepairWriteDuration(time.Duration) // time spent (per cluster) writing the KeyScoreMember
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

// KeysFarmCompleted satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) KeysFarmCompleted() {}

// KeysFarmCompleted satisfies the Instrumentation interface but does no work.
func (i NopInstrumentation) KeysThrottled() {}

// RepairCall satisfies the Instrumnetation interface but does no work.
func (i NopInstrumentation) RepairCall() {}

// RepairRequestCount satisfies the Instrumnetation interface but does no work.
func (i NopInstrumentation) RepairRequestCount(int) {}

// RepairDiscarded satisfies the Instrumnetation interface but does no work.
func (i NopInstrumentation) RepairDiscarded(int) {}

// RepairCheckPartialFailure satisfies the Instrumnetation interface but does no work.
func (i NopInstrumentation) RepairCheckPartialFailure() {}

// RepairCheckCompleteFailure satisfies the Instrumnetation interface but does no work.
func (i NopInstrumentation) RepairCheckCompleteFailure() {}

// RepairCheckDuration satisfies the Instrumnetation interface but does no work.
func (i NopInstrumentation) RepairCheckDuration(time.Duration) {}

// RepairCheckRedundant satisfies the Instrumnetation interface but does no work.
func (i NopInstrumentation) RepairCheckRedundant() {}

// RepairWriteCount satisfies the Instrumnetation interface but does no work.
func (i NopInstrumentation) RepairWriteCount() {}

// RepairWriteSuccess satisfies the Instrumnetation interface but does no work.
func (i NopInstrumentation) RepairWriteSuccess() {}

// RepairWriteFailure satisfies the Instrumnetation interface but does no work.
func (i NopInstrumentation) RepairWriteFailure() {}

// RepairWriteDuration satisfies the Instrumnetation interface but does no work.
func (i NopInstrumentation) RepairWriteDuration(time.Duration) {}
