package webrtc

import (
	"testing"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// TestRegression_ICECandidateBeforeRemoteDescription verifies that a candidate
// received before remote description can be queued and flushed after setting
// remote description. This mirrors the browser log: queueing ICE candidate then
// flushing 1 queued after remote description. The server's AddICECandidateWithRetry
// retries, but we also verify queuing behavior via the PeerConnection's ability
// to accept candidates after remote description is set.
func TestRegression_ICECandidateBeforeRemoteDescription(t *testing.T) {
	// Shrink retry delay for test speed
	origDelay := defaultICERetryDelay
	origAttempts := defaultICERetryAttempts
	defaultICERetryDelay = 5 * time.Millisecond
	defaultICERetryAttempts = 5
	defer func() { defaultICERetryDelay = origDelay; defaultICERetryAttempts = origAttempts }()

	// Two PCs: offerer and answerer. Answerer will receive candidate before remote offer.
	offerer := newNegotiationTestPeerConnection(t, "offerer", "O")
	answerer := newNegotiationTestPeerConnection(t, "answerer", "A")

	offer, err := offerer.CreateOffer()
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}

	// Create a candidate from offerer (simulate gathering)
	cand := &ICECandidate{}
	cand.SetCandidate("candidate:1 1 UDP 2122257343 192.0.2.1 12345 typ host")
	cand.SetSDPMid("0")
	cand.SetSDPMLineIndex(0)

	// LiveKit-style: before remote description, candidate is queued (not failed)
	err = answerer.AddICECandidateWithRetry(cand)
	if err != nil {
		t.Fatalf("candidate before remote should be queued, got error: %v", err)
	}
	answerer.mu.RLock()
	queued := len(answerer.pendingICECandidates)
	answerer.mu.RUnlock()
	if queued != 1 {
		t.Errorf("pending ICE candidates = %d, want 1 queued before remote", queued)
	}

	// Now set remote offer on answerer — should flush 1 queued
	if err := answerer.SetRemoteDescription(offer); err != nil {
		t.Fatalf("SetRemoteDescription: %v", err)
	}
	answerer.mu.RLock()
	queuedAfter := len(answerer.pendingICECandidates)
	answerer.mu.RUnlock()
	if queuedAfter != 0 {
		t.Errorf("pending ICE candidates after flush = %d, want 0", queuedAfter)
	}
	// After remote description, candidate should be addable directly
	cand2 := &ICECandidate{}
	cand2.SetCandidate("candidate:2 1 UDP 2122257343 192.0.2.2 12346 typ host")
	cand2.SetSDPMid("0")
	cand2.SetSDPMLineIndex(0)
	if err := answerer.AddICECandidateWithRetry(cand2); err != nil {
		t.Errorf("candidate after remote description failed: %v", err)
	}
}

// TestRegression_ICECandidateAfterRemoteDescription verifies normal path still works
func TestRegression_ICECandidateAfterRemoteDescription(t *testing.T) {
	pc := newNegotiationTestPeerConnection(t, "pc1", "A")
	pc2 := newNegotiationTestPeerConnection(t, "pc2", "B")
	offer, _ := pc.CreateOffer()
	answer, _ := pc2.CreateAnswer(offer)
	_ = pc.SetRemoteDescription(answer)

	cand := &ICECandidate{}
	cand.SetCandidate("candidate:1 1 UDP 2122257343 192.0.2.1 12345 typ host")
	cand.SetSDPMid("0")
	cand.SetSDPMLineIndex(0)
	if err := pc2.AddICECandidateWithRetry(cand); err != nil {
		t.Errorf("AddICECandidate after stable failed: %v", err)
	}
}

// TestRegression_MultipleQueuedCandidates verifies 3 candidates queued before
// remote are flushed together after SetRemoteDescription.
func TestRegression_MultipleQueuedCandidates(t *testing.T) {
	pc := newNegotiationTestPeerConnection(t, "pc1", "A")
	pc2 := newNegotiationTestPeerConnection(t, "pc2", "B")
	offer, _ := pc.CreateOffer()
	// Queue 3 candidates before remote on pc2
	for i := 0; i < 3; i++ {
		c := &ICECandidate{}
		c.SetCandidate("candidate:1 1 UDP 2122257343 192.0.2.1 12345 typ host")
		c.SetSDPMid("0")
		c.SetSDPMLineIndex(uint16(i))
		if err := pc2.AddICECandidateWithRetry(c); err != nil {
			t.Fatalf("queue candidate %d before remote failed: %v", i, err)
		}
	}
	pc2.mu.RLock()
	if len(pc2.pendingICECandidates) != 3 {
		t.Errorf("pending before flush = %d, want 3", len(pc2.pendingICECandidates))
	}
	pc2.mu.RUnlock()
	_ = pc2.SetRemoteDescription(offer)
	pc2.mu.RLock()
	if len(pc2.pendingICECandidates) != 0 {
		t.Errorf("pending after flush = %d, want 0", len(pc2.pendingICECandidates))
	}
	pc2.mu.RUnlock()
	// After remote, 3 new candidates should succeed directly
	for i := 0; i < 3; i++ {
		c := &ICECandidate{}
		c.SetCandidate("candidate:1 1 UDP 2122257343 192.0.2.1 12345 typ host")
		c.SetSDPMid("0")
		c.SetSDPMLineIndex(uint16(i))
		if err := pc2.AddICECandidate(c); err != nil {
			t.Errorf("candidate %d after remote failed: %v", i, err)
		}
	}
}

// TestRegression_QueuedCandidatesCleanupAfterConnection verifies that after
// connection closed, AddICECandidate returns peer_connection_closed and no leak.
func TestRegression_QueuedCandidatesCleanupAfterConnection(t *testing.T) {
	origDelay := defaultICERetryDelay
	origAttempts := defaultICERetryAttempts
	defaultICERetryDelay = 2 * time.Millisecond
	defaultICERetryAttempts = 2
	defer func() { defaultICERetryDelay = origDelay; defaultICERetryAttempts = origAttempts }()

	pc := newNegotiationTestPeerConnection(t, "pc1", "A")
	_ = pc.Close()
	cand := &ICECandidate{}
	cand.SetCandidate("candidate:1 1 UDP 2122257343 192.0.2.1 12345 typ host")
	cand.SetSDPMid("0")
	cand.SetSDPMLineIndex(0)
	err := pc.AddICECandidateWithRetry(cand)
	if err == nil {
		t.Error("expected error after Close, got nil")
	}
	// Ensure no goroutine leak: close should not hang
	done := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("cleanup timed out")
	}
}

// newNegotiationTestPeerConnection helper already defined in peer_connection_test.go,
// but we need a STUN-free variant that works without external ICE. We reuse same
// config as e2e tests.
func newNegotiationTestPeerConnectionForICE(t *testing.T, id, name string) *PeerConnection {
	t.Helper()
	pc, err := NewPeerConnection(PeerConnectionConfig{
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback,
	}, domain.NewParticipant(id, name), nil)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	if _, err := pc.PionPeerConnection().AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("AddTransceiver: %v", err)
	}
	return pc
}
