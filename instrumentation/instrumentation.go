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
	SelectSendAllPermitGranted()               // called when the permitter allows SendVarReadFirstLinger to send to all clusters
	SelectSendAllPermitRejected()              // called when the permitter doesn't allow SendVarReadFirstLinger to send to all clusters
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
