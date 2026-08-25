package webrtc

import (
	"encoding/json"
	"testing"

	"github.com/pion/webrtc/v3"
)

func TestNewICECandidate(t *testing.T) {
	// Create a Pion ICE candidate
	pionCandidate := &webrtc.ICECandidate{
		Foundation:     "1234567890",
		Priority:       2122260223,
		Address:        "192.168.1.1",
		Protocol:       webrtc.ICEProtocolUDP,
		Port:           12345,
		Typ:            webrtc.ICECandidateTypeHost,
		Component:      1,
	}

	// Create an ICECandidate
	candidate := NewICECandidate(pionCandidate)

	// Verify the candidate was created correctly
	if candidate.Candidate() == "" {
		t.Error("Expected candidate string to be non-empty")
	}

	if candidate.SDPMid() != "" {
		t.Errorf("Expected SDP mid to be empty, got '%s'", candidate.SDPMid())
	}

	if candidate.SDPMLineIndex() != 0 {
		t.Errorf("Expected SDP m-line index to be 0, got %d", candidate.SDPMLineIndex())
	}

	// Test that we can get the Pion ICE candidate init
	init := candidate.PionICECandidateInit()
	if init.Candidate == "" {
		t.Error("Expected Pion ICE candidate init to have candidate string")
	}
}

func TestNewICECandidateFromString(t *testing.T) {
	candidateString := "candidate:1234567890 1 udp 2122260223 192.168.1.1 12345 typ host generation 0 ufrag ABCD network-id 1"

	// Create an ICECandidate from string
	candidate, err := NewICECandidateFromString(candidateString)
	if err != nil {
		t.Fatalf("Failed to create ICECandidate from string: %v", err)
	}

	if candidate.Candidate() != candidateString {
		t.Errorf("Expected candidate string to match")
	}
}

func TestICECandidateSetters(t *testing.T) {
	pionCandidate := &webrtc.ICECandidate{
		Foundation:     "1234567890",
		Priority:       2122260223,
		Address:        "192.168.1.1",
		Protocol:       webrtc.ICEProtocolUDP,
		Port:           12345,
		Typ:            webrtc.ICECandidateTypeHost,
		Component:      1,
	}

	candidate := NewICECandidate(pionCandidate)

	// Test SetCandidate
	newCandidate := "candidate:9876543210 1 udp 2122260223 192.168.1.2 54321 typ host generation 0 ufrag WXYZ network-id 1"
	if err := candidate.SetCandidate(newCandidate); err != nil {
		t.Fatalf("Failed to set candidate: %v", err)
	}
	if candidate.Candidate() != newCandidate {
		t.Errorf("Expected candidate to be updated")
	}

	// Test SetSDPMid
	candidate.SetSDPMid("1")
	if candidate.SDPMid() != "1" {
		t.Errorf("Expected SDP mid to be updated, got '%s'", candidate.SDPMid())
	}

	// Test SetSDPMLineIndex
	candidate.SetSDPMLineIndex(1)
	if candidate.SDPMLineIndex() != 1 {
		t.Errorf("Expected SDP m-line index to be updated, got %d", candidate.SDPMLineIndex())
	}
}

func TestICECandidateMarshalJSON(t *testing.T) {
	pionCandidate := &webrtc.ICECandidate{
		Foundation:     "1234567890",
		Priority:       2122260223,
		Address:        "192.168.1.1",
		Protocol:       webrtc.ICEProtocolUDP,
		Port:           12345,
		Typ:            webrtc.ICECandidateTypeHost,
		Component:      1,
	}

	candidate := NewICECandidate(pionCandidate)

	// Marshal to JSON
	jsonData, err := json.Marshal(candidate)
	if err != nil {
		t.Fatalf("Failed to marshal ICECandidate: %v", err)
	}

	// Verify the JSON structure
	var raw map[string]interface{}
	if err := json.Unmarshal(jsonData, &raw); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if raw["candidate"] == nil {
		t.Errorf("Expected JSON candidate to be present")
	}

	// The candidate string should be non-empty
	if raw["candidate"].(string) == "" {
		t.Errorf("Expected JSON candidate to be non-empty")
	}
}

func TestICECandidateUnmarshalJSON(t *testing.T) {
	jsonData := `{"candidate": "candidate:9876543210 1 udp 2122260223 192.168.1.2 54321 typ host generation 0 ufrag WXYZ network-id 1", "sdpMid": "1", "sdpMLineIndex": 1}`

	var candidate ICECandidate
	if err := json.Unmarshal([]byte(jsonData), &candidate); err != nil {
		t.Fatalf("Failed to unmarshal ICECandidate: %v", err)
	}

	if candidate.Candidate() != "candidate:9876543210 1 udp 2122260223 192.168.1.2 54321 typ host generation 0 ufrag WXYZ network-id 1" {
		t.Errorf("Expected candidate string to match")
	}

	if candidate.SDPMid() != "1" {
		t.Errorf("Expected SDP mid to be '1', got '%s'", candidate.SDPMid())
	}

	if candidate.SDPMLineIndex() != 1 {
		t.Errorf("Expected SDP m-line index to be 1, got %d", candidate.SDPMLineIndex())
	}
}

func TestICECandidateString(t *testing.T) {
	pionCandidate := &webrtc.ICECandidate{
		Foundation:     "1234567890",
		Priority:       2122260223,
		Address:        "192.168.1.1",
		Protocol:       webrtc.ICEProtocolUDP,
		Port:           12345,
		Typ:            webrtc.ICECandidateTypeHost,
		Component:      1,
	}

	candidate := NewICECandidate(pionCandidate)

	str := candidate.String()
	// The string representation should be non-empty
	if str == "" {
		t.Error("Expected string representation to be non-empty")
	}
}
