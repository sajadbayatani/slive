package signaling

import (
	"fmt"
	"testing"

	"github.com/sajadbayatani/slive/internal/domain"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

func TestRoomManager_CreateRoom(t *testing.T) {
	manager := NewRoomManager()

	// Test creating a new room
	room, err := manager.CreateRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	if room.ID() != "test-room" {
		t.Errorf("Expected room ID 'test-room', got '%s'", room.ID())
	}

	if room.State() != domain.RoomStateActive {
		t.Errorf("Expected room state 'active', got '%s'", room.State())
	}

	// Test creating duplicate room
	_, err = manager.CreateRoom("test-room")
	if err != ErrRoomAlreadyExists {
		t.Errorf("Expected ErrRoomAlreadyExists, got: %v", err)
	}
}

func TestRoomManager_GetOrCreateRoom(t *testing.T) {
	manager := NewRoomManager()

	// Test get or create new room
	room1, err := manager.GetOrCreateRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to get or create room: %v", err)
	}

	// Test getting existing room
	room2, err := manager.GetOrCreateRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to get or create room: %v", err)
	}

	if room1 != room2 {
		t.Error("Expected same room instance")
	}
}

func TestRoomManager_CloseRoom(t *testing.T) {
	manager := NewRoomManager()

	// Create a room
	_, err := manager.CreateRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// Close the room
	err = manager.CloseRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to close room: %v", err)
	}

	// Test getting closed room
	room := manager.GetRoom("test-room")
	if room != nil {
		t.Error("Expected nil for closed room")
	}

	// Test closing non-existent room
	err = manager.CloseRoom("non-existent")
	if err != ErrRoomNotFound {
		t.Errorf("Expected ErrRoomNotFound, got: %v", err)
	}
}

func TestConnectionManager_AddRemove(t *testing.T) {
	manager := NewConnectionManager()

	// Create a mock connection (we can't easily create a real WebSocket connection)
	// So we'll just test the manager logic with nil connections for now
	// In a real test, you'd use a mock WebSocket connection

	// Test empty manager
	if len(manager.ConnectionIDs()) != 0 {
		t.Error("Expected empty connection manager")
	}

	// Test getting non-existent connection
	conn := manager.Get("non-existent")
	if conn != nil {
		t.Error("Expected nil for non-existent connection")
	}
}

func TestMessage_CreateAndParse(t *testing.T) {
	// Test creating a message
	msg, err := NewMessage(MessageTypeJoinRoom, JoinRoomRequest{
		RoomID:        "test-room",
		ParticipantID: "test-participant",
	})
	if err != nil {
		t.Fatalf("Failed to create message: %v", err)
	}

	if msg.Type != MessageTypeJoinRoom {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeJoinRoom, msg.Type)
	}

	// Test marshaling and parsing
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal message: %v", err)
	}

	parsedMsg, err := ParseMessage(data)
	if err != nil {
		t.Fatalf("Failed to parse message: %v", err)
	}

	if parsedMsg.Type != msg.Type {
		t.Errorf("Expected message type '%s', got '%s'", msg.Type, parsedMsg.Type)
	}

	// Test unmarshaling data
	var req JoinRoomRequest
	err = parsedMsg.UnmarshalData(&req)
	if err != nil {
		t.Fatalf("Failed to unmarshal message data: %v", err)
	}

	if req.RoomID != "test-room" {
		t.Errorf("Expected room ID 'test-room', got '%s'", req.RoomID)
	}
}

func TestErrorCodeFromDomainError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{"RoomClosed", domain.ErrRoomClosed, ErrorCodeRoomClosed},
		{"ParticipantNotFound", domain.ErrParticipantNotFound, ErrorCodeParticipantNotFound},
		{"TrackNotFound", domain.ErrTrackNotFound, ErrorCodeTrackNotFound},
		{"NilError", nil, ""},
		{"UnknownError", domain.ErrParticipantAlreadyExists, ErrorCodeInternalError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code := errorCodeFromDomainError(tt.err)
			if code != tt.expected {
				t.Errorf("Expected error code '%s', got '%s'", tt.expected, code)
			}
		})
	}
}

// TestErrorCodeFromError pins the general error mapping used by the WebRTC
// signaling handlers: wrapped sentinel chains keep their specific code, and
// anything unrecognized falls through to errorCodeFromDomainError.
func TestErrorCodeFromError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{"Nil", nil, ""},
		// Validation failures carry ErrInvalidRequest through fmt wrapping.
		{"InvalidRequest", fmt.Errorf("%w: sdp exceeds maximum length of %d bytes", ErrInvalidRequest, MaxSDPLength), ErrorCodeInvalidRequest},
		// Transport / registry sentinels.
		{"ConnectionNotFound", ErrConnectionNotFound, ErrorCodeConnectionNotFound},
		{"NoPeerConnectionWrapped", fmt.Errorf("lookup failed: %w", webrtc.ErrNoPeerConnection), ErrorCodeConnectionNotFound},
		// WebRTC sentinels, including nested wrapping.
		{"ICEFailedWrappingClosed", fmt.Errorf("%w: %v", webrtc.ErrICEFailed, webrtc.ErrPeerConnectionClosed), ErrorCodeICEFailed},
		{"PeerConnectionClosed", fmt.Errorf("apply failed: %w", webrtc.ErrPeerConnectionClosed), ErrorCodePeerConnectionClosed},
		{"NegotiationFailed", webrtc.ErrNegotiationFailed, ErrorCodeNegotiationFailed},
		{"WebRTCTrackNotFound", webrtc.ErrTrackNotFound, ErrorCodeTrackNotFound},
		{"InvalidSDP", webrtc.ErrInvalidSDP, ErrorCodeInvalidWebRTCMessage},
		{"InvalidICECandidate", webrtc.ErrInvalidICECandidate, ErrorCodeInvalidWebRTCMessage},
		{"TrackNotReady", webrtc.ErrTrackNotReady, ErrorCodeInvalidWebRTCMessage},
		// Domain errors delegate unchanged.
		{"DomainRoomClosed", domain.ErrRoomClosed, ErrorCodeRoomClosed},
		{"DomainParticipantNotFound", domain.ErrParticipantNotFound, ErrorCodeParticipantNotFound},
		{"DomainTrackNotFound", domain.ErrTrackNotFound, ErrorCodeTrackNotFound},
		{"DomainUnknown", domain.ErrParticipantAlreadyExists, ErrorCodeInternalError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if code := errorCodeFromError(tt.err); code != tt.expected {
				t.Errorf("errorCodeFromError(%v) = %q, want %q", tt.err, code, tt.expected)
			}
		})
	}
}

// TestErrorCodeFromErrorMatchesDomainMapping asserts parity between the
// generic mapping and the domain-only mapping: every error the domain mapping
// classifies must receive the identical code from errorCodeFromError.
func TestErrorCodeFromErrorMatchesDomainMapping(t *testing.T) {
	domainErrors := []error{
		domain.ErrRoomClosed,
		domain.ErrParticipantNotFound,
		domain.ErrTrackNotFound,
		domain.ErrParticipantAlreadyExists,
		domain.ErrParticipantLeft,
		domain.ErrTrackAlreadyPublished,
		domain.ErrTrackAlreadySubscribed,
	}

	for _, err := range domainErrors {
		want := errorCodeFromDomainError(err)
		got := errorCodeFromError(err)
		if got != want {
			t.Errorf("parity broken for %v: errorCodeFromError=%q errorCodeFromDomainError=%q", err, got, want)
		}
		if want == "" {
			t.Errorf("domain error %v unexpectedly unmapped", err)
		}
	}
}

func TestValidateOfferRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     *OfferRequest
		wantErr bool
	}{
		{
			name: "Valid offer request",
			req: &OfferRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "participant2",
				SDP:                 "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n",
				TrackIDs:            []string{"track1"},
			},
			wantErr: false,
		},
		{
			name:    "Nil request",
			req:     nil,
			wantErr: true,
		},
		{
			name: "Empty participant ID",
			req: &OfferRequest{
				ParticipantID:       "",
				TargetParticipantID: "participant2",
				SDP:                 "v=0\r\n",
			},
			wantErr: true,
		},
		{
			name: "Empty target participant ID",
			req: &OfferRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "",
				SDP:                 "v=0\r\n",
			},
			wantErr: true,
		},
		{
			name: "Empty SDP",
			req: &OfferRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "participant2",
				SDP:                 "",
			},
			wantErr: true,
		},
		{
			name: "SDP too long",
			req: &OfferRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "participant2",
				SDP:                 string(make([]byte, MaxSDPLength+1)),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOfferRequest(tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateOfferRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateAnswerRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     *AnswerRequest
		wantErr bool
	}{
		{
			name: "Valid answer request",
			req: &AnswerRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "participant2",
				SDP:                 "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n",
			},
			wantErr: false,
		},
		{
			name:    "Nil request",
			req:     nil,
			wantErr: true,
		},
		{
			name: "Empty participant ID",
			req: &AnswerRequest{
				ParticipantID:       "",
				TargetParticipantID: "participant2",
				SDP:                 "v=0\r\n",
			},
			wantErr: true,
		},
		{
			name: "Empty target participant ID",
			req: &AnswerRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "",
				SDP:                 "v=0\r\n",
			},
			wantErr: true,
		},
		{
			name: "Empty SDP",
			req: &AnswerRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "participant2",
				SDP:                 "",
			},
			wantErr: true,
		},
		{
			name: "SDP too long",
			req: &AnswerRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "participant2",
				SDP:                 string(make([]byte, MaxSDPLength+1)),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAnswerRequest(tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateAnswerRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateICECandidateRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     *ICECandidateRequest
		wantErr bool
	}{
		{
			name: "Valid ICE candidate request",
			req: &ICECandidateRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "participant2",
				Candidate:           "candidate:1234567890 1 udp 2130706431 192.168.1.1 12345 typ host",
				SDPMid:              "0",
				SDPMLineIndex:       0,
			},
			wantErr: false,
		},
		{
			name:    "Nil request",
			req:     nil,
			wantErr: true,
		},
		{
			name: "Empty participant ID",
			req: &ICECandidateRequest{
				ParticipantID:       "",
				TargetParticipantID: "participant2",
				Candidate:           "candidate:1234567890 1 udp 2130706431 192.168.1.1 12345 typ host",
				SDPMid:              "0",
				SDPMLineIndex:       0,
			},
			wantErr: true,
		},
		{
			name: "Empty target participant ID",
			req: &ICECandidateRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "",
				Candidate:           "candidate:1234567890 1 udp 2130706431 192.168.1.1 12345 typ host",
				SDPMid:              "0",
				SDPMLineIndex:       0,
			},
			wantErr: true,
		},
		{
			name: "Empty candidate",
			req: &ICECandidateRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "participant2",
				Candidate:           "",
				SDPMid:              "0",
				SDPMLineIndex:       0,
			},
			wantErr: true,
		},
		{
			name: "Candidate too long",
			req: &ICECandidateRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "participant2",
				Candidate:           string(make([]byte, MaxCandidateLength+1)),
				SDPMid:              "0",
				SDPMLineIndex:       0,
			},
			wantErr: true,
		},
		{
			name: "Empty SDP mid",
			req: &ICECandidateRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "participant2",
				Candidate:           "candidate:1234567890 1 udp 2130706431 192.168.1.1 12345 typ host",
				SDPMid:              "",
				SDPMLineIndex:       0,
			},
			wantErr: true,
		},
		{
			name: "Negative SDP mline index",
			req: &ICECandidateRequest{
				ParticipantID:       "participant1",
				TargetParticipantID: "participant2",
				Candidate:           "candidate:1234567890 1 udp 2130706431 192.168.1.1 12345 typ host",
				SDPMid:              "0",
				SDPMLineIndex:       -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateICECandidateRequest(tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateICECandidateRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
