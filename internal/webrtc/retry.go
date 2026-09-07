package webrtc

import (
	"fmt"
	"time"
)

// ICE candidate retry behaviour. These are vars (not consts) so tests can
// shrink the backoff instead of sleeping through the production delays.
var (
	defaultICERetryAttempts = 3
	defaultICERetryDelay    = 50 * time.Millisecond
)

// AddICECandidateWithRetry adds a remote ICE candidate, retrying transient
// failures a bounded number of times before giving up.
//
// LiveKit-style trickle ICE: candidates received before the remote description
// is set are queued (logged as "queueing ICE candidate") and flushed after
// SetRemoteDescription moves to stable. This prevents the
// "ICE candidate before remote description" race from failing.
func (pc *PeerConnection) AddICECandidateWithRetry(candidate *ICECandidate) error {
	logger := pc.logger
	participantID := ""
	if p := pc.Participant(); p != nil {
		participantID = p.ID()
	}

	// Queue if remote description not yet set and not closed (LiveKit trickle-ICE).
	pc.mu.RLock()
	remoteDesc := pc.pionPC.RemoteDescription()
	isClosed := pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed
	pc.mu.RUnlock()
	if !isClosed && remoteDesc == nil {
		pc.mu.Lock()
		// Re-check under write lock to avoid race
		if pc.state != PeerConnectionStateClosed && pc.state != PeerConnectionStateFailed && pc.pionPC.RemoteDescription() == nil {
			pc.pendingICECandidates = append(pc.pendingICECandidates, candidate)
			pending := len(pc.pendingICECandidates)
			pc.mu.Unlock()
			logger.Info("queueing ICE candidate",
				"event", "ice_candidate_queued",
				"participant_id", participantID,
				"pending", pending,
			)
			return nil
		}
		pc.mu.Unlock()
	}

	var lastErr error
	for attempt := 0; attempt < defaultICERetryAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(defaultICERetryDelay * time.Duration(attempt))
		}
		lastErr = pc.AddICECandidate(candidate)
		if lastErr == nil {
			return nil
		}
		logger.Warn("ICE candidate add failed",
			"event", "ice_retry_failed",
			"participant_id", participantID,
			"attempt", attempt+1,
			"error", lastErr,
		)
	}
	ConnectionMetrics.IncrementFailures()
	return fmt.Errorf("%w: %w", ErrICEFailed, lastErr)
}
