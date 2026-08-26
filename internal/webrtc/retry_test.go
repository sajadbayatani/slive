package webrtc

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// quietLogger keeps expected retry warnings out of test output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestAddICECandidateWithRetryExhaustsAndCountsFailure pins the retry
// exhaustion contract: every attempt fails against a closed peer connection,
// the returned error wraps both ErrICEFailed and the root cause, and exactly
// one failure is recorded in ConnectionMetrics.
func TestAddICECandidateWithRetryExhaustsAndCountsFailure(t *testing.T) {
	// Shrink the backoff so three attempts finish instantly; restore for
	// other tests. Metrics are a process-global singleton, so reset and
	// stay off t.Parallel.
	prevAttempts := defaultICERetryAttempts
	prevDelay := defaultICERetryDelay
	defaultICERetryAttempts = 3
	defaultICERetryDelay = time.Millisecond
	t.Cleanup(func() {
		defaultICERetryAttempts = prevAttempts
		defaultICERetryDelay = prevDelay
	})
	ConnectionMetrics.Reset()

	pc, err := NewPeerConnection(PeerConnectionConfig{
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback,
		Logger:       quietLogger(),
	}, domain.NewParticipant("ice-retry", "Rita"), nil)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	if err := pc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	failuresBefore := ConnectionMetrics.FailuresTotal()

	err = pc.AddICECandidateWithRetry(&ICECandidate{})
	if err == nil {
		t.Fatal("expected exhaustion error, got nil")
	}
	if !errors.Is(err, ErrICEFailed) {
		t.Errorf("expected error to wrap ErrICEFailed, got: %v", err)
	}
	if !errors.Is(err, ErrPeerConnectionClosed) {
		t.Errorf("expected error to wrap root cause ErrPeerConnectionClosed, got: %v", err)
	}
	if got := ConnectionMetrics.FailuresTotal(); got != failuresBefore+1 {
		t.Errorf("FailuresTotal = %d, want %d", got, failuresBefore+1)
	}
}

// TestAddICECandidateWithRetryFirstAttemptFailureIsICEFailed verifies that
// any failure leaving AddICECandidateWithRetry — including one where the very
// first attempt fails — is shaped as ErrICEFailed wrapping the root cause,
// since callers can never apply the candidate when this function errors.
func TestAddICECandidateWithRetryFirstAttemptFailureIsICEFailed(t *testing.T) {
	prevAttempts := defaultICERetryAttempts
	prevDelay := defaultICERetryDelay
	defaultICERetryAttempts = 1
	defaultICERetryDelay = time.Millisecond
	t.Cleanup(func() {
		defaultICERetryAttempts = prevAttempts
		defaultICERetryDelay = prevDelay
	})
	ConnectionMetrics.Reset()

	pc, err := NewPeerConnection(PeerConnectionConfig{
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback,
		Logger:       quietLogger(),
	}, domain.NewParticipant("ice-single", "Sara"), nil)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	if err := pc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err = pc.AddICECandidateWithRetry(&ICECandidate{})
	if !errors.Is(err, ErrICEFailed) {
		t.Errorf("expected ErrICEFailed, got: %v", err)
	}
	if !errors.Is(err, ErrPeerConnectionClosed) {
		t.Errorf("expected wrapped root cause ErrPeerConnectionClosed, got: %v", err)
	}
}
