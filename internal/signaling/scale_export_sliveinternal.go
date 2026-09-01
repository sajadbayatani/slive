//go:build slive_internal

package signaling

// ReapGhostForTest triggers ghost reap directly (exposed for TASK-030 scale tests).
// Gated behind //go:build slive_internal per TASK-036 B-3 mitigation.
func (h *Handler) ReapGhostForTest(roomID, participantID string) {
	h.reapGhost(roomID, participantID)
}

// ArmGhostForTest arms the ghost timer (exposed for TASK-030 scale tests).
// Gated behind //go:build slive_internal.
func (h *Handler) ArmGhostForTest(roomID, participantID string) {
	h.armGhostTimer(roomID, participantID)
}

// ResetMetrics resets test-visible counters: connection metrics and GC reaped count,
// and per-forwarder dropped totals. Gated behind //go:build slive_internal.
func (h *Handler) ResetMetrics() {
	h.resetMetrics()
}

// ResetGCReapedCount clears the ghost-reap counter. Gated behind //go:build slive_internal.
func (h *Handler) ResetGCReapedCount() {
	h.resetGCReapedCount()
}
