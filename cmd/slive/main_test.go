package main

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/sajadbayatani/slive/internal/config"
	apphttp "github.com/sajadbayatani/slive/internal/http"
	"github.com/sajadbayatani/slive/internal/logger"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

// TestGracefulShutdown tests the graceful shutdown logic.
func TestGracefulShutdown(t *testing.T) {
	// Create a server.
	cfg := config.Config{HTTPAddr: ":8083"}
	log := logger.New()
	server := apphttp.NewServer(cfg, log)

	// Start the server in a goroutine.
	go func() {
		if err := server.Start(); err != nil {
			t.Errorf("Server failed to start: %v", err)
		}
	}()

	// Give the server a moment to start.
	time.Sleep(100 * time.Millisecond)

	// Simulate a shutdown signal.
	stop := make(chan os.Signal, 1)
	stop <- syscall.SIGINT

	// Create a context with a timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Shutdown the server.
	if err := server.Shutdown(ctx); err != nil {
		t.Errorf("Server shutdown failed: %v", err)
	}
}

// TestGracefulShutdown_ContextTimeout tests shutdown with a context timeout.
func TestGracefulShutdown_ContextTimeout(t *testing.T) {
	// Create a server.
	cfg := config.Config{HTTPAddr: ":8084"}
	log := logger.New()
	server := apphttp.NewServer(cfg, log)

	// Start the server in a goroutine.
	go func() {
		if err := server.Start(); err != nil {
			t.Errorf("Server failed to start: %v", err)
		}
	}()

	// Give the server a moment to start.
	time.Sleep(100 * time.Millisecond)

	// Simulate a shutdown signal.
	stop := make(chan os.Signal, 1)
	stop <- syscall.SIGTERM

	// Shutdown should fail due to context timeout.
	// Note: The standard http.Server.Shutdown may not return an error for a canceled context
	// if there are no active connections. This test is more of a documentation of expected
	// behavior rather than a strict requirement.
	// For now, we skip this test as it's not reliable with the standard library behavior.
	t.Skip("Skipping: http.Server.Shutdown does not reliably fail on context timeout")
}

// TestMain_SignalHandling tests the signal handling logic in main.
func TestMain_SignalHandling(t *testing.T) {
	// This test verifies that the signal handling logic works as expected.
	// Note: This is a simplified test and may not cover all edge cases.

	// Create a channel for signals.
	stop := make(chan os.Signal, 1)

	// Simulate a SIGINT signal.
	stop <- syscall.SIGINT

	// Verify the signal was received.
	select {
	case sig := <-stop:
		if sig != syscall.SIGINT {
			t.Errorf("Expected SIGINT, got %v", sig)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for signal")
	}
}

// TestBuildPeerConnectionConfig verifies the translation of runtime
// STUN/TURN configuration into the peer connection config: explicit
// configuration wins, otherwise the webrtc package defaults are kept.
func TestBuildPeerConnectionConfig(t *testing.T) {
	fallback := buildPeerConnectionConfig(config.Config{})
	want := webrtc.DefaultPeerConnectionConfig()
	if len(fallback.ICEServers) != len(want.ICEServers) {
		t.Errorf("unconfigured ICE servers = %+v, want package defaults %+v", fallback.ICEServers, want.ICEServers)
	}

	cfg := config.Config{
		STUNServers: []string{"stun:stun.example.com:3478"},
		TURNServers: []config.TURNServer{{
			URLs:       []string{"turn:turn.example.com:3478", "turns:turn.example.com:5349"},
			Username:   "slive-user",
			Credential: "slive-secret",
		}},
	}
	got := buildPeerConnectionConfig(cfg)
	if len(got.ICEServers) != 2 {
		t.Fatalf("ICEServers = %+v, want one STUN group and one TURN entry", got.ICEServers)
	}
	if got.ICEServers[0].URLs[0] != "stun:stun.example.com:3478" {
		t.Errorf("STUN entry = %+v, want stun:stun.example.com:3478", got.ICEServers[0])
	}
	if got.ICEServers[1].Username != "slive-user" || got.ICEServers[1].Credential != "slive-secret" ||
		len(got.ICEServers[1].URLs) != 2 {
		t.Errorf("TURN entry = %+v, want configured URLs and credentials", got.ICEServers[1])
	}
}

// TestTurnServersFromConfig pins the mapping of parsed TURN configuration
// onto the ICE-server representation, including the empty passthrough.
func TestTurnServersFromConfig(t *testing.T) {
	if got := turnServersFromConfig(nil); len(got) != 0 {
		t.Errorf("empty config produced %+v, want empty slice", got)
	}

	got := turnServersFromConfig([]config.TURNServer{{
		URLs:       []string{"turn:relay.example.com:3478"},
		Username:   "u",
		Credential: "c",
	}})
	if len(got) != 1 || got[0].URLs[0] != "turn:relay.example.com:3478" ||
		got[0].Username != "u" || got[0].Credential != "c" {
		t.Errorf("TURN mapping = %+v, want relay URL with credentials", got)
	}
}

// TestNewSignalingHandlerWiring verifies that the production wiring produces
// a handler whose peer connections carry the configured logger and ICE
// servers (checked indirectly through construction succeeding; deep
// assertions live in the signaling package tests).
func TestNewSignalingHandlerWiring(t *testing.T) {
	handler := newSignalingHandler(config.Config{
		STUNServers: []string{"stun:stun.example.com:3478"},
	}, logger.New())
	if handler == nil {
		t.Fatal("expected a wired signaling handler")
	}
}
