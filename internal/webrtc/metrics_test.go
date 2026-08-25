package webrtc

import (
	"testing"

	"github.com/sajadbayatani/slive/internal/domain"
)

func TestConnectionMetrics(t *testing.T) {
	ConnectionMetrics.Reset()

	ConnectionMetrics.IncrementAttempts()
	ConnectionMetrics.IncrementAttempts()
	ConnectionMetrics.IncrementFailures()

	if got := ConnectionMetrics.AttemptsTotal(); got != 2 {
		t.Errorf("AttemptsTotal() = %d, want 2", got)
	}
	if got := ConnectionMetrics.FailuresTotal(); got != 1 {
		t.Errorf("FailuresTotal() = %d, want 1", got)
	}
}

func TestNeedsReconnect(t *testing.T) {
	participant := domain.NewParticipant("p1", "Alice")
	pc, err := NewPeerConnection(DefaultPeerConnectionConfig(), participant, nil)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	defer pc.Close()

	if pc.NeedsReconnect() {
		t.Error("new connection should not need reconnect")
	}

	pc.mu.Lock()
	pc.state = PeerConnectionStateDisconnected
	pc.mu.Unlock()

	if !pc.NeedsReconnect() {
		t.Error("disconnected connection should need reconnect")
	}
}
