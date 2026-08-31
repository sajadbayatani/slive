package webrtc

import (
	"runtime"
	"sync/atomic"
	"time"
)

// ConnectionMetrics tracks WebRTC connection attempt and failure counters.
type connectionMetrics struct {
	attempts atomic.Uint64
	failures atomic.Uint64
}

// ConnectionMetrics is the package-level metrics registry for peer connections.
var ConnectionMetrics = &connectionMetrics{}

var startTime = time.Now()

// MetricsSnapshot is a point-in-time copy of all observable counters and gauges.
// It is safe to encode and write without holding any handler or forwarder locks.
type MetricsSnapshot struct {
	ConnectionAttemptsTotal uint64 `json:"connection_attempts_total"`
	ConnectionFailuresTotal uint64 `json:"connection_failures_total"`
	ForwarderSubscribers    int    `json:"forwarder_subscribers"`
	ForwarderDroppedTotal   uint64 `json:"forwarder_dropped_total"`
	ForwarderQueueDepth     int    `json:"forwarder_queue_depth"`
	RoomsActive             int    `json:"rooms_active"`
	ParticipantsActive      int    `json:"participants_active"`
	TracksPublished         int    `json:"tracks_published"`
	GCReapedTotal           uint64 `json:"gc_reaped_total"`
	UptimeSeconds           int64  `json:"uptime_seconds"`
	Goroutines              int    `json:"goroutines"`
}

// StartTime returns the process start time used for UptimeSeconds.
func StartTime() time.Time {
	return startTime
}

// IncrementAttempts records a connection negotiation attempt.
func (m *connectionMetrics) IncrementAttempts() {
	m.attempts.Add(1)
}

// IncrementFailures records a failed connection negotiation.
func (m *connectionMetrics) IncrementFailures() {
	m.failures.Add(1)
}

// AttemptsTotal returns the total number of connection attempts.
func (m *connectionMetrics) AttemptsTotal() uint64 {
	return m.attempts.Load()
}

// FailuresTotal returns the total number of connection failures.
func (m *connectionMetrics) FailuresTotal() uint64 {
	return m.failures.Load()
}

// Reset clears all counters. Intended for tests.
func (m *connectionMetrics) Reset() {
	m.attempts.Store(0)
	m.failures.Store(0)
}

// Snapshot returns a point-in-time copy of connection counters plus
// sampled runtime gauges (uptime, goroutines). Forwarder and room gauges
// are zero in this package-level snapshot; the signaling Handler.Snapshot()
// fills those fields while holding the correct lock hierarchy.
func (m *connectionMetrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		ConnectionAttemptsTotal: m.attempts.Load(),
		ConnectionFailuresTotal: m.failures.Load(),
		UptimeSeconds:           int64(time.Since(startTime).Seconds()),
		Goroutines:              runtime.NumGoroutine(),
	}
}
