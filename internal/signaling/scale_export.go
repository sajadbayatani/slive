package signaling

// ReapGhostForTest triggers ghost reap directly (exposed for TASK-030 scale tests).
func (h *Handler) ReapGhostForTest(roomID, participantID string) {
	h.reapGhost(roomID, participantID)
}

// ArmGhostForTest arms the ghost timer (exposed for TASK-030 scale tests).
func (h *Handler) ArmGhostForTest(roomID, participantID string) {
	h.armGhostTimer(roomID, participantID)
}
