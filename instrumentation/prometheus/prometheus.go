// Package prometheus implements Instrumentation against exported Prometheus
// metrics.
package prometheus

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/soundcloud/roshi/instrumentation"
)

// Satisfaction guaranteed.
var _ instrumentation.Instrumentation = PrometheusInstrumentation{}

// PrometheusInstrumentation holds metrics for all instrumented methods.
type PrometheusInstrumentation struct {
	insertCallCount                  prometheus.Counter
	insertRecordCount                prometheus.Counter
	insertCallDuration               prometheus.Summary
	insertRecordDuration             prometheus.Summary
	insertQuorumFailureCount         prometheus.Counter
	selectCallCount                  prometheus.Counter
	selectKeysCount                  prometheus.Counter
	selectSendToCount                prometheus.Counter
	selectFirstResponseDuration      prometheus.Summary
	selectPartialErrorCount          prometheus.Counter
	selectBlockingDuration           prometheus.Summary
	selectOverheadDuration           prometheus.Summary
	selectDuration                   prometheus.Summary
	selectSendAllPermitGrantedCount  prometheus.Counter
	selectSendAllPermitRejectedCount prometheus.Counter
	selectSendAllPromotionCount      prometheus.Counter
	selectRetrievedCount             prometheus.Counter
	selectReturnedCount              prometheus.Counter
	selectRepairNeededCount          prometheus.Counter
	deleteCallCount                  prometheus.Counter
	deleteRecordCount                prometheus.Counter
	deleteCallDuration               prometheus.Summary
	deleteRecordDuration             prometheus.Summary
	deleteQuorumFailureCount         prometheus.Counter
	repairCallCount                  prometheus.Counter
	repairRequestCount               prometheus.Counter
	repairDiscardedCount             prometheus.Counter
	repairWriteSuccessCount          prometheus.Counter
	repairWriteFailureCount          prometheus.Counter
	walkKeysCount                    prometheus.Counter
}

// New returns a new Instrumentation that prints metrics to the passed
// io.Writer. All metrics are prefixed with an appropriate bucket name, and
// take the form e.g. "insert.record.count 10".
func New(prefix string, maxSummaryAge time.Duration) PrometheusInstrumentation {
	i := PrometheusInstrumentation{
		insertCallCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "insert_call_count",
			Help:      "How many insert calls have been made.",
		}),
		insertRecordCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "insert_record_count",
			Help:      "How many records have been inserted.",
		}),
		insertCallDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: prefix,
			Name:      "insert_call_duration_nanoseconds",
			Help:      "Insert duration per-call.",
			MaxAge:    maxSummaryAge,
		}),
		insertRecordDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: prefix,
			Name:      "insert_record_duration_nanoseconds",
			Help:      "Insert duration per-record.",
			MaxAge:    maxSummaryAge,
		}),
		insertQuorumFailureCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "insert_quorum_failure_count",
			Help:      "Insert quorum failure count.",
		}),
		selectCallCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "select_call_count",
			Help:      "How many select calls have been made.",
		}),
		selectKeysCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "select_keys_count",
			Help:      "How many keys have been selected.",
		}),
		selectSendToCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "select_send_to_count",
			Help:      "How many clusters have received select calls.",
		}),
		selectFirstResponseDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: prefix,
			Name:      "select_first_response_duration_nanoseconds",
			Help:      "Select first response duration.",
			MaxAge:    maxSummaryAge,
		}),
		selectPartialErrorCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "select_partial_error_count",
			Help:      "How many partial errors have occurred in selects.",
		}),
		selectBlockingDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: prefix,
			Name:      "select_blocking_duration_nanoseconds",
			Help:      "Select blocking duration.",
			MaxAge:    maxSummaryAge,
		}),
		selectOverheadDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: prefix,
			Name:      "select_overhead_duration_nanoseconds",
			Help:      "Select overhead duration.",
			MaxAge:    maxSummaryAge,
		}),
		selectDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: prefix,
			Name:      "select_duration_nanoseconds",
			Help:      "Overall select duration.",
			MaxAge:    maxSummaryAge,
		}),
		selectSendAllPermitGrantedCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "select_send_all_permit_granted_count",
			Help:      "How many select requests were granted initial permission to send-all, in appropriate read strategies.",
		}),
		selectSendAllPermitRejectedCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "select_send_all_permit_rejected_count",
			Help:      "How many select requests were denied initial permission to send-all, in appropriate read strategies.",
		}),
		selectSendAllPromotionCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "select_send_all_promotion_count",
			Help:      "How many select requests were promoted to a send-all, in appropriate read strategies.",
		}),
		selectRetrievedCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "select_retrieved_count",
			Help:      "How many key-score-member tuples have been retrieved from clusters by select calls.",
		}),
		selectReturnedCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "select_returned_count",
			Help:      "How many key-score-member tuples have been returned to clients by select calls.",
		}),
		selectRepairNeededCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "select_repair_needed_count",
			Help:      "How many repairs have been detected and requested by select calls.",
		}),
		deleteCallCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "delete_call_count",
			Help:      "How many delete calls have been made.",
		}),
		deleteRecordCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "delete_record_count",
			Help:      "How many records have been deleted in delete calls.",
		}),
		deleteCallDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: prefix,
			Name:      "delete_call_duration_nanoseconds",
			Help:      "Delete duration, per-call.",
			MaxAge:    maxSummaryAge,
		}),
		deleteRecordDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: prefix,
			Name:      "delete_record_duration_nanoseconds",
			Help:      "Delete duration, per-record.",
			MaxAge:    maxSummaryAge,
		}),
		deleteQuorumFailureCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "delete_quorum_failure_count",
			Help:      "Delete quorum failure count.",
		}),
		repairCallCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "repair_call_count",
			Help:      "How many repair calls have been made.",
		}),
		repairRequestCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "repair_request_count",
			Help:      "How many key-member tuples have been repaired.",
		}),
		repairDiscardedCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "repair_discarded_count",
			Help:      "How many repair calls have been discarded due to rate or buffer limits.",
		}),
		repairWriteSuccessCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "repair_write_success_count",
			Help:      "Repair write success count.",
		}),
		repairWriteFailureCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "repair_write_failure_count",
			Help:      "Repair write failure count.",
		}),
		walkKeysCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prefix,
			Name:      "walk_keys_count",
			Help:      "How many keys have been walked by the walker process.",
		}),
	}

	prometheus.MustRegister(i.insertCallCount)
	prometheus.MustRegister(i.insertRecordCount)
	prometheus.MustRegister(i.insertCallDuration)
	prometheus.MustRegister(i.insertRecordDuration)
	prometheus.MustRegister(i.insertQuorumFailureCount)
	prometheus.MustRegister(i.selectCallCount)
	prometheus.MustRegister(i.selectKeysCount)
	prometheus.MustRegister(i.selectSendToCount)
	prometheus.MustRegister(i.selectFirstResponseDuration)
	prometheus.MustRegister(i.selectPartialErrorCount)
	prometheus.MustRegister(i.selectBlockingDuration)
	prometheus.MustRegister(i.selectOverheadDuration)
	prometheus.MustRegister(i.selectDuration)
	prometheus.MustRegister(i.selectSendAllPermitGrantedCount)
	prometheus.MustRegister(i.selectSendAllPermitRejectedCount)
	prometheus.MustRegister(i.selectSendAllPromotionCount)
	prometheus.MustRegister(i.selectRetrievedCount)
	prometheus.MustRegister(i.selectReturnedCount)
	prometheus.MustRegister(i.selectRepairNeededCount)
	prometheus.MustRegister(i.deleteCallCount)
	prometheus.MustRegister(i.deleteRecordCount)
	prometheus.MustRegister(i.deleteCallDuration)
	prometheus.MustRegister(i.deleteRecordDuration)
	prometheus.MustRegister(i.deleteQuorumFailureCount)
	prometheus.MustRegister(i.repairCallCount)
	prometheus.MustRegister(i.repairRequestCount)
	prometheus.MustRegister(i.repairDiscardedCount)
	prometheus.MustRegister(i.repairWriteSuccessCount)
	prometheus.MustRegister(i.repairWriteFailureCount)
	prometheus.MustRegister(i.walkKeysCount)

	return i
}

// Install installs the Prometheus handlers, so the metrics are available.
func (i PrometheusInstrumentation) Install(pattern string, mux *http.ServeMux) {
	mux.Handle(pattern, prometheus.Handler())
}

// InsertCall satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) InsertCall() {
	i.insertCallCount.Inc()
}

// InsertRecordCount satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) InsertRecordCount(n int) {
	i.insertRecordCount.Add(float64(n))
}

// InsertCallDuration satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) InsertCallDuration(d time.Duration) {
	i.insertCallDuration.Observe(float64(d.Nanoseconds()))
}

// InsertRecordDuration satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) InsertRecordDuration(d time.Duration) {
	i.insertRecordDuration.Observe(float64(d.Nanoseconds()))
}

// InsertQuorumFailure satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) InsertQuorumFailure() {
	i.insertQuorumFailureCount.Inc()
}

// SelectCall satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectCall() {
	i.selectCallCount.Inc()
}

// SelectKeys satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectKeys(n int) {
	i.selectKeysCount.Add(float64(n))
}

// SelectSendTo satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectSendTo(n int) {
	i.selectSendToCount.Add(float64(n))
}

// SelectFirstResponseDuration satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectFirstResponseDuration(d time.Duration) {
	i.selectFirstResponseDuration.Observe(float64(d.Nanoseconds()))
}

// SelectPartialError satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectPartialError() {
	i.selectPartialErrorCount.Inc()
}

// SelectBlockingDuration satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectBlockingDuration(d time.Duration) {
	i.selectBlockingDuration.Observe(float64(d.Nanoseconds()))
}

// SelectOverheadDuration satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectOverheadDuration(d time.Duration) {
	i.selectOverheadDuration.Observe(float64(d.Nanoseconds()))
}

// SelectDuration satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectDuration(d time.Duration) {
	i.selectDuration.Observe(float64(d.Nanoseconds()))
}

// SelectSendAllPermitGranted satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectSendAllPermitGranted() {
	i.selectSendAllPermitGrantedCount.Inc()
}

// SelectSendAllPermitRejected satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectSendAllPermitRejected() {
	i.selectSendAllPermitRejectedCount.Inc()
}

// SelectSendAllPromotion satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectSendAllPromotion() {
	i.selectSendAllPromotionCount.Inc()
}

// SelectRetrieved satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectRetrieved(n int) {
	i.selectRetrievedCount.Add(float64(n))
}

// SelectReturned satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectReturned(n int) {
	i.selectReturnedCount.Add(float64(n))
}

// SelectRepairNeeded satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) SelectRepairNeeded(n int) {
	i.selectRepairNeededCount.Add(float64(n))
}

// DeleteCall satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) DeleteCall() {
	i.deleteCallCount.Inc()
}

// DeleteRecordCount satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) DeleteRecordCount(n int) {
	i.deleteRecordCount.Add(float64(n))
}

// DeleteCallDuration satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) DeleteCallDuration(d time.Duration) {
	i.deleteCallDuration.Observe(float64(d.Nanoseconds()))
}

// DeleteRecordDuration satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) DeleteRecordDuration(d time.Duration) {
	i.deleteRecordDuration.Observe(float64(d.Nanoseconds()))
}

// DeleteQuorumFailure satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) DeleteQuorumFailure() {
	i.deleteQuorumFailureCount.Inc()
}

// RepairCall satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) RepairCall() {
	i.repairCallCount.Inc()
}

// RepairRequest satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) RepairRequest(n int) {
	i.repairRequestCount.Add(float64(n))
}

// RepairDiscarded satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) RepairDiscarded(n int) {
	i.repairDiscardedCount.Add(float64(n))
}

// RepairWriteSuccess satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) RepairWriteSuccess(n int) {
	i.repairWriteSuccessCount.Add(float64(n))
}

// RepairWriteFailure satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) RepairWriteFailure(n int) {
	i.repairWriteFailureCount.Add(float64(n))
}

// WalkKeys satisfies the Instrumentation interface.
func (i PrometheusInstrumentation) WalkKeys(n int) {
	i.walkKeysCount.Add(float64(n))
}
