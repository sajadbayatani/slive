package webrtc

import (
	"log"
	"time"
)

const (
	defaultICERetryAttempts = 3
	defaultICERetryDelay    = 50 * time.Millisecond
)

// AddICECandidateWithRetry adds a remote ICE candidate, retrying transient failures.
func (pc *PeerConnection) AddICECandidateWithRetry(candidate *ICECandidate) error {
	var lastErr error
	for attempt := 0; attempt < defaultICERetryAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(defaultICERetryDelay * time.Duration(attempt))
		}
		lastErr = pc.AddICECandidate(candidate)
		if lastErr == nil {
			return nil
		}
		log.Printf("webrtc: ICE candidate retry attempt=%d participant=%s err=%v",
			attempt+1, pc.Participant().ID(), lastErr)
	}
	ConnectionMetrics.IncrementFailures()
	return lastErr
}
