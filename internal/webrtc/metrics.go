package webrtc

import "sync/atomic"

// ConnectionMetrics tracks WebRTC connection attempt and failure counters.
type connectionMetrics struct {
	attempts atomic.Uint64
	failures atomic.Uint64
}

// ConnectionMetrics is the package-level metrics registry for peer connections.
var ConnectionMetrics = &connectionMetrics{}

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
