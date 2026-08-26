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
// Exhausted retries are reported as ErrICEFailed wrapping the last underlying
// error, so callers (and the signaling error mapping) can distinguish "the
// candidate was never applied" from one-shot failures such as
// ErrPeerConnectionClosed. SDP operations are deliberately never retried;
// see the package documentation for that decision.
func (pc *PeerConnection) AddICECandidateWithRetry(candidate *ICECandidate) error {
	logger := pc.logger
	participantID := ""
	if p := pc.Participant(); p != nil {
		participantID = p.ID()
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
