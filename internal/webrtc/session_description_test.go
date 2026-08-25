package webrtc

import (
	"encoding/json"
	"testing"

	"github.com/pion/webrtc/v3"
)

func TestNewSessionDescription(t *testing.T) {
	// Create a Pion session description
	pionSDP := &webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n...",
	}

	// Create a SessionDescription
	sdp := NewSessionDescription(pionSDP)

	// Verify the session description was created correctly
	if sdp.Type() != webrtc.SDPTypeOffer {
		t.Errorf("Expected SDP type to be 'offer', got '%s'", sdp.Type())
	}

	if sdp.SDP() != "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n..." {
		t.Errorf("Expected SDP string to match")
	}

	if sdp.PionSessionDescription() != pionSDP {
		t.Error("Expected Pion session description to match")
	}
}

func TestNewSessionDescriptionFromString(t *testing.T) {
	sdpString := "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n..."

	// Create a SessionDescription from string
	sdp, err := NewSessionDescriptionFromString(sdpString, webrtc.SDPTypeAnswer)
	if err != nil {
		t.Fatalf("Failed to create SessionDescription from string: %v", err)
	}

	if sdp.Type() != webrtc.SDPTypeAnswer {
		t.Errorf("Expected SDP type to be 'answer', got '%s'", sdp.Type())
	}

	if sdp.SDP() != sdpString {
		t.Errorf("Expected SDP string to match")
	}
}

func TestSessionDescriptionSetters(t *testing.T) {
	pionSDP := &webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n...",
	}

	sdp := NewSessionDescription(pionSDP)

	// Test SetSDP
	newSDP := "v=0\r\no=- 9876543210 2 IN IP4 127.0.0.1\r\n..."
	sdp.SetSDP(newSDP)
	if sdp.SDP() != newSDP {
		t.Errorf("Expected SDP to be updated, got '%s'", sdp.SDP())
	}

	// Test SetType
	sdp.SetType(webrtc.SDPTypeAnswer)
	if sdp.Type() != webrtc.SDPTypeAnswer {
		t.Errorf("Expected SDP type to be 'answer', got '%s'", sdp.Type())
	}
}

func TestSessionDescriptionMarshalJSON(t *testing.T) {
	pionSDP := &webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n...",
	}

	sdp := NewSessionDescription(pionSDP)

	// Marshal to JSON
	jsonData, err := json.Marshal(sdp)
	if err != nil {
		t.Fatalf("Failed to marshal SessionDescription: %v", err)
	}

	// Verify the JSON structure
	var raw map[string]string
	if err := json.Unmarshal(jsonData, &raw); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if raw["type"] != "offer" {
		t.Errorf("Expected JSON type to be 'offer', got '%s'", raw["type"])
	}

	if raw["sdp"] != "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n..." {
		t.Errorf("Expected JSON SDP to match")
	}
}

func TestSessionDescriptionUnmarshalJSON(t *testing.T) {
	jsonData := `{"type": "answer", "sdp": "v=0\r\no=- 9876543210 2 IN IP4 127.0.0.1\r\n..."}`

	var sdp SessionDescription
	if err := json.Unmarshal([]byte(jsonData), &sdp); err != nil {
		t.Fatalf("Failed to unmarshal SessionDescription: %v", err)
	}

	if sdp.Type() != webrtc.SDPTypeAnswer {
		t.Errorf("Expected SDP type to be 'answer', got '%s'", sdp.Type())
	}

	if sdp.SDP() != "v=0\r\no=- 9876543210 2 IN IP4 127.0.0.1\r\n..." {
		t.Errorf("Expected SDP string to match")
	}
}

func TestSessionDescriptionString(t *testing.T) {
	pionSDP := &webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n...",
	}

	sdp := NewSessionDescription(pionSDP)

	str := sdp.String()
	expected := "offer\nv=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n..."
	if str != expected {
		t.Errorf("Expected string representation to be '%s', got '%s'", expected, str)
	}
}
