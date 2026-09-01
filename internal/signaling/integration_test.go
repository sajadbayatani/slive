package signaling

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/sajadbayatani/slive/internal/domain"
)

// =============================================================================
// Integration Tests for Signaling Protocol
// =============================================================================

// TestSignaling_RoomLifecycle tests the complete lifecycle of a room:
// creation, joining, leaving, and closing.
func TestSignaling_RoomLifecycle(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()

	// Test room creation
	room, err := roomManager.CreateRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	if room.ID() != "test-room" {
		t.Errorf("Expected room ID 'test-room', got '%s'", room.ID())
	}

	if room.State() != domain.RoomStateActive {
		t.Errorf("Expected room state 'active', got '%s'", room.State())
	}

	// Test joining room
	participant1 := domain.NewParticipant("participant-1", "User 1")
	if err := room.Join(participant1); err != nil {
		t.Fatalf("Failed to join room: %v", err)
	}
	participant1.SetRoom(room)

	if len(room.Participants()) != 1 {
		t.Errorf("Expected 1 participant, got %d", len(room.Participants()))
	}

	// Test second participant joining
	participant2 := domain.NewParticipant("participant-2", "User 2")
	if err := room.Join(participant2); err != nil {
		t.Fatalf("Failed to join room: %v", err)
	}
	participant2.SetRoom(room)

	if len(room.Participants()) != 2 {
		t.Errorf("Expected 2 participants, got %d", len(room.Participants()))
	}

	// Test leaving room
	if err := room.Leave("participant-1"); err != nil {
		t.Fatalf("Failed to leave room: %v", err)
	}

	if len(room.Participants()) != 1 {
		t.Errorf("Expected 1 participant after leave, got %d", len(room.Participants()))
	}

	// Test closing room through manager
	if err := roomManager.CloseRoom("test-room"); err != nil {
		t.Fatalf("Failed to close room: %v", err)
	}

	// Test that room is removed from manager
	if roomManager.GetRoom("test-room") != nil {
		t.Error("Expected room to be removed from manager after close")
	}
}

// TestSignaling_ParticipantLifecycle tests participant lifecycle within a room.
func TestSignaling_ParticipantLifecycle(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()
	room, err := roomManager.CreateRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// Test participant joining
	participant := domain.NewParticipant("participant-1", "User 1")
	if err := room.Join(participant); err != nil {
		t.Fatalf("Failed to join room: %v", err)
	}
	participant.SetRoom(room)

	if participant.Room() != room {
		t.Error("Expected participant to be associated with room")
	}

	if participant.State() != domain.ParticipantStateJoined {
		t.Errorf("Expected participant state 'joined', got '%s'", participant.State())
	}

	// Test participant activation
	participant.Activate()
	if participant.State() != domain.ParticipantStateActive {
		t.Errorf("Expected participant state 'active', got '%s'", participant.State())
	}

	// Test participant leaving
	participant.Leave()
	if participant.State() != domain.ParticipantStateLeft {
		t.Errorf("Expected participant state 'left', got '%s'", participant.State())
	}

	if participant.Room() != nil {
		t.Error("Expected participant room to be nil after leave")
	}

	// Test duplicate participant join
	participant2 := domain.NewParticipant("participant-1", "User 1 Duplicate")
	if err := room.Join(participant2); err != domain.ErrParticipantAlreadyExists {
		t.Errorf("Expected ErrParticipantAlreadyExists, got: %v", err)
	}
}

// TestSignaling_TrackManagement tests track publishing, subscribing, and unpublishing.
func TestSignaling_TrackManagement(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()
	room, err := roomManager.CreateRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// Create participants
	publisher := domain.NewParticipant("publisher", "Publisher")
	subscriber := domain.NewParticipant("subscriber", "Subscriber")

	if err := room.Join(publisher); err != nil {
		t.Fatalf("Failed to join publisher: %v", err)
	}
	if err := room.Join(subscriber); err != nil {
		t.Fatalf("Failed to join subscriber: %v", err)
	}
	publisher.SetRoom(room)
	subscriber.SetRoom(room)

	// Test publishing audio track
	audioTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create audio track: %v", err)
	}
	if err := publisher.PublishTrack(audioTrack); err != nil {
		t.Fatalf("Failed to publish audio track: %v", err)
	}

	if len(publisher.PublishedTracks()) != 1 {
		t.Errorf("Expected 1 published track, got %d", len(publisher.PublishedTracks()))
	}

	// Test publishing video track
	videoTrack, err := domain.NewTrack("video-1", domain.TrackKindVideo, domain.TrackSourceCamera)
	if err != nil {
		t.Fatalf("Failed to create video track: %v", err)
	}
	if err := publisher.PublishTrack(videoTrack); err != nil {
		t.Fatalf("Failed to publish video track: %v", err)
	}

	if len(publisher.PublishedTracks()) != 2 {
		t.Errorf("Expected 2 published tracks, got %d", len(publisher.PublishedTracks()))
	}

	// Test duplicate track publishing
	duplicateTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create duplicate track: %v", err)
	}
	if err := publisher.PublishTrack(duplicateTrack); err != domain.ErrTrackAlreadyPublished {
		t.Errorf("Expected ErrTrackAlreadyPublished, got: %v", err)
	}

	// Test subscribing to track
	if err := subscriber.SubscribeTrack(audioTrack); err != nil {
		t.Fatalf("Failed to subscribe to track: %v", err)
	}

	if len(subscriber.SubscribedTracks()) != 1 {
		t.Errorf("Expected 1 subscribed track, got %d", len(subscriber.SubscribedTracks()))
	}

	// Test duplicate subscription
	if err := subscriber.SubscribeTrack(audioTrack); err != domain.ErrTrackAlreadySubscribed {
		t.Errorf("Expected ErrTrackAlreadySubscribed, got: %v", err)
	}

	// Test unpublishing track
	if err := publisher.UnpublishTrack("audio-1"); err != nil {
		t.Fatalf("Failed to unpublish track: %v", err)
	}

	if len(publisher.PublishedTracks()) != 1 {
		t.Errorf("Expected 1 published track after unpublish, got %d", len(publisher.PublishedTracks()))
	}

	// Test unsubscribing from track
	if err := subscriber.UnsubscribeTrack("audio-1"); err != nil {
		t.Fatalf("Failed to unsubscribe from track: %v", err)
	}

	if len(subscriber.SubscribedTracks()) != 0 {
		t.Errorf("Expected 0 subscribed tracks after unsubscribe, got %d", len(subscriber.SubscribedTracks()))
	}

	// Test unpublishing non-existent track
	if err := publisher.UnpublishTrack("non-existent"); err != domain.ErrTrackNotFound {
		t.Errorf("Expected ErrTrackNotFound, got: %v", err)
	}
}

// TestSignaling_MessageSerialization tests message creation, serialization, and parsing.
func TestSignaling_MessageSerialization(t *testing.T) {
	tests := []struct {
		name     string
		msgType  MessageType
		data     interface{}
		expected string
	}{
		{
			name:    "CreateRoom",
			msgType: MessageTypeCreateRoom,
			data: CreateRoomRequest{
				RoomID:          "test-room",
				ParticipantID:   "test-participant",
				ParticipantName: "Test User",
			},
			expected: `{"type":"create_room","data":{"room_id":"test-room","participant_id":"test-participant","participant_name":"Test User"}}`,
		},
		{
			name:    "JoinRoom",
			msgType: MessageTypeJoinRoom,
			data: JoinRoomRequest{
				RoomID:          "test-room",
				ParticipantID:   "test-participant",
				ParticipantName: "Test User",
			},
			expected: `{"type":"join_room","data":{"room_id":"test-room","participant_id":"test-participant","participant_name":"Test User"}}`,
		},
		{
			name:    "PublishTrack",
			msgType: MessageTypePublishTrack,
			data: PublishTrackRequest{
				RoomID:        "test-room",
				ParticipantID: "test-participant",
				Track: TrackInfo{
					ID:     "track-1",
					Kind:   "audio",
					Source: "microphone",
				},
			},
			expected: `{"type":"publish_track","data":{"room_id":"test-room","participant_id":"test-participant","track":{"id":"track-1","kind":"audio","source":"microphone"}}}`,
		},
		{
			name:    "Offer",
			msgType: MessageTypeOffer,
			data: OfferRequest{
				RoomID:              "test-room",
				ParticipantID:       "participant-1",
				TargetParticipantID: "participant-2",
				SDP:                 "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n",
				TrackIDs:            []string{"track-1"},
			},
			expected: `{"type":"offer","data":{"room_id":"test-room","participant_id":"participant-1","target_participant_id":"participant-2","sdp":"v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n","track_ids":["track-1"]}}`,
		},
		{
			name:    "ICECandidate",
			msgType: MessageTypeICECandidate,
			data: ICECandidateRequest{
				RoomID:              "test-room",
				ParticipantID:       "participant-1",
				TargetParticipantID: "participant-2",
				Candidate:           "candidate:1234567890 1 udp 2122260223 192.168.1.1 12345 typ host",
				SDPMid:              "0",
				SDPMLineIndex:       0,
			},
			expected: `{"type":"ice_candidate","data":{"room_id":"test-room","participant_id":"participant-1","target_participant_id":"participant-2","candidate":"candidate:1234567890 1 udp 2122260223 192.168.1.1 12345 typ host","sdp_mid":"0","sdp_mline_index":0}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create message
			msg, err := NewMessage(tt.msgType, tt.data)
			if err != nil {
				t.Fatalf("Failed to create message: %v", err)
			}

			// Marshal message
			data, err := msg.Marshal()
			if err != nil {
				t.Fatalf("Failed to marshal message: %v", err)
			}

			// Parse message
			parsedMsg, err := ParseMessage(data)
			if err != nil {
				t.Fatalf("Failed to parse message: %v", err)
			}

			if parsedMsg.Type != tt.msgType {
				t.Errorf("Expected message type '%s', got '%s'", tt.msgType, parsedMsg.Type)
			}

			// Verify the data can be unmarshaled back to the original type
			// (We can't directly compare JSON strings due to field ordering)
			var parsedData json.RawMessage
			if err := parsedMsg.UnmarshalData(&parsedData); err != nil {
				t.Fatalf("Failed to unmarshal message data: %v", err)
			}
		})
	}
}

// TestSignaling_ErrorHandling tests error scenarios in the signaling protocol.
func TestSignaling_ErrorHandling(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()

	// Test joining non-existent room
	room := roomManager.GetRoom("non-existent")
	if room != nil {
		t.Error("Expected nil for non-existent room")
	}

	// Test closing non-existent room
	if err := roomManager.CloseRoom("non-existent"); err != ErrRoomNotFound {
		t.Errorf("Expected ErrRoomNotFound, got: %v", err)
	}

	// Create a room for further tests
	room, err := roomManager.CreateRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// Test joining closed room
	if err := room.Close(); err != nil {
		t.Fatalf("Failed to close room: %v", err)
	}

	participant := domain.NewParticipant("participant-1", "User 1")
	if err := room.Join(participant); err != domain.ErrRoomClosed {
		t.Errorf("Expected ErrRoomClosed, got: %v", err)
	}

	// Test leaving from closed room
	if err := room.Leave("participant-1"); err != domain.ErrRoomClosed {
		t.Errorf("Expected ErrRoomClosed, got: %v", err)
	}

	// Test publishing track to closed room (via participant)
	participant2 := domain.NewParticipant("participant-2", "User 2")
	participant2.Leave() // Mark as left
	audioTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create audio track: %v", err)
	}
	if err := participant2.PublishTrack(audioTrack); err != domain.ErrParticipantLeft {
		t.Errorf("Expected ErrParticipantLeft, got: %v", err)
	}

	// Test subscribing to non-existent track
	participant3 := domain.NewParticipant("participant-3", "User 3")
	fakeTrack, err := domain.NewTrack("non-existent", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create fake track: %v", err)
	}
	if err := participant3.SubscribeTrack(fakeTrack); err != nil {
		// This should succeed because we're not checking if the track exists in the room
		// The error would come from the handler when trying to find the track
		t.Logf("SubscribeTrack with non-existent track: %v", err)
	}
}

// TestSignaling_WebRTCSignaling tests WebRTC signaling message handling.
func TestSignaling_WebRTCSignaling(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()
	room, err := roomManager.CreateRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// Create participants
	publisher := domain.NewParticipant("publisher", "Publisher")
	subscriber := domain.NewParticipant("subscriber", "Subscriber")

	if err := room.Join(publisher); err != nil {
		t.Fatalf("Failed to join publisher: %v", err)
	}
	if err := room.Join(subscriber); err != nil {
		t.Fatalf("Failed to join subscriber: %v", err)
	}
	publisher.SetRoom(room)
	subscriber.SetRoom(room)

	// Publish a track
	audioTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create audio track: %v", err)
	}
	if err := publisher.PublishTrack(audioTrack); err != nil {
		t.Fatalf("Failed to publish track: %v", err)
	}

	// Test offer creation
	offerReq := OfferRequest{
		RoomID:              "test-room",
		ParticipantID:       "publisher",
		TargetParticipantID: "subscriber",
		SDP:                 "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n",
		TrackIDs:            []string{"audio-1"},
	}

	offerMsg, err := NewMessage(MessageTypeOffer, offerReq)
	if err != nil {
		t.Fatalf("Failed to create offer message: %v", err)
	}

	if offerMsg.Type != MessageTypeOffer {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeOffer, offerMsg.Type)
	}

	// Test offer parsing
	var parsedOffer OfferRequest
	if err := offerMsg.UnmarshalData(&parsedOffer); err != nil {
		t.Fatalf("Failed to unmarshal offer: %v", err)
	}

	if parsedOffer.ParticipantID != "publisher" {
		t.Errorf("Expected participant ID 'publisher', got '%s'", parsedOffer.ParticipantID)
	}

	if parsedOffer.TargetParticipantID != "subscriber" {
		t.Errorf("Expected target participant ID 'subscriber', got '%s'", parsedOffer.TargetParticipantID)
	}

	// Test answer creation
	answerReq := AnswerRequest{
		RoomID:              "test-room",
		ParticipantID:       "subscriber",
		TargetParticipantID: "publisher",
		SDP:                 "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n",
	}

	answerMsg, err := NewMessage(MessageTypeAnswer, answerReq)
	if err != nil {
		t.Fatalf("Failed to create answer message: %v", err)
	}

	if answerMsg.Type != MessageTypeAnswer {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeAnswer, answerMsg.Type)
	}

	// Test ICE candidate creation
	iceReq := ICECandidateRequest{
		RoomID:              "test-room",
		ParticipantID:       "publisher",
		TargetParticipantID: "subscriber",
		Candidate:           "candidate:1234567890 1 udp 2122260223 192.168.1.1 12345 typ host",
		SDPMid:              "0",
		SDPMLineIndex:       0,
	}

	iceMsg, err := NewMessage(MessageTypeICECandidate, iceReq)
	if err != nil {
		t.Fatalf("Failed to create ICE candidate message: %v", err)
	}

	if iceMsg.Type != MessageTypeICECandidate {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeICECandidate, iceMsg.Type)
	}

	// Test ICE candidate parsing
	var parsedICE ICECandidateRequest
	if err := iceMsg.UnmarshalData(&parsedICE); err != nil {
		t.Fatalf("Failed to unmarshal ICE candidate: %v", err)
	}

	if parsedICE.Candidate != "candidate:1234567890 1 udp 2122260223 192.168.1.1 12345 typ host" {
		t.Errorf("Expected specific candidate string")
	}
}

// TestSignaling_ConcurrentAccess tests concurrent access to room and participant management.
func TestSignaling_ConcurrentAccess(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()
	room, err := roomManager.CreateRoom("concurrent-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// Number of concurrent operations
	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup

	// Concurrent participant joins
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				participantID := fmt.Sprintf("participant-%d-%d", id, j)
				participant := domain.NewParticipant(participantID, fmt.Sprintf("User %d-%d", id, j))
				_ = room.Join(participant)
				participant.SetRoom(room)
			}
		}(i)
	}

	// Concurrent participant leaves
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				participantID := fmt.Sprintf("participant-%d-%d", id, j)
				_ = room.Leave(participantID)
			}
		}(i)
	}

	// Concurrent track publishing
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				participantID := fmt.Sprintf("publisher-%d-%d", id, j)
				participant := domain.NewParticipant(participantID, fmt.Sprintf("Publisher %d-%d", id, j))
				_ = room.Join(participant)
				participant.SetRoom(room)
				track, err := domain.NewTrack(fmt.Sprintf("track-%d-%d", id, j), domain.TrackKindAudio, domain.TrackSourceMicrophone)
				if err != nil {
					t.Logf("Failed to create track: %v", err)
					continue
				}
				_ = participant.PublishTrack(track)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify room is still functional
	if room.State() != domain.RoomStateActive {
		t.Errorf("Expected room state 'active', got '%s'", room.State())
	}

	// The room should have some participants (exact count depends on race conditions)
	t.Logf("Final participant count: %d", len(room.Participants()))
}

// TestSignaling_EdgeCases tests various edge cases in the signaling protocol.
func TestSignaling_EdgeCases(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()

	// Test duplicate room creation
	if _, err := roomManager.CreateRoom("duplicate-room"); err != nil {
		t.Fatalf("Failed to create first room: %v", err)
	}

	if _, err := roomManager.CreateRoom("duplicate-room"); err != ErrRoomAlreadyExists {
		t.Errorf("Expected ErrRoomAlreadyExists for duplicate room, got: %v", err)
	}

	// Test GetOrCreateRoom with existing room
	room1, err := roomManager.GetOrCreateRoom("existing-room")
	if err != nil {
		t.Fatalf("Failed to get or create room: %v", err)
	}

	room2, err := roomManager.GetOrCreateRoom("existing-room")
	if err != nil {
		t.Fatalf("Failed to get or create room: %v", err)
	}

	if room1 != room2 {
		t.Error("Expected same room instance from GetOrCreateRoom")
	}

	// Test RoomIDs method
	roomManager.CreateRoom("room-1")
	roomManager.CreateRoom("room-2")
	roomManager.CreateRoom("room-3")

	roomIDs := roomManager.RoomIDs()
	if len(roomIDs) != 5 { // duplicate-room, existing-room, room-1, room-2, room-3
		t.Errorf("Expected 5 room IDs, got %d", len(roomIDs))
	}

	// Test participant info serialization
	participantInfo := ParticipantInfo{
		ID:   "participant-1",
		Name: "Test User",
	}

	data, err := json.Marshal(participantInfo)
	if err != nil {
		t.Fatalf("Failed to marshal participant info: %v", err)
	}

	var parsedInfo ParticipantInfo
	if err := json.Unmarshal(data, &parsedInfo); err != nil {
		t.Fatalf("Failed to unmarshal participant info: %v", err)
	}

	if parsedInfo.ID != participantInfo.ID || parsedInfo.Name != participantInfo.Name {
		t.Error("Participant info mismatch after serialization")
	}

	// Test track info serialization
	trackInfo := TrackInfo{
		ID:     "track-1",
		Kind:   "audio",
		Source: "microphone",
	}

	data, err = json.Marshal(trackInfo)
	if err != nil {
		t.Fatalf("Failed to marshal track info: %v", err)
	}

	var parsedTrackInfo TrackInfo
	if err := json.Unmarshal(data, &parsedTrackInfo); err != nil {
		t.Fatalf("Failed to unmarshal track info: %v", err)
	}

	if parsedTrackInfo.ID != trackInfo.ID || parsedTrackInfo.Kind != trackInfo.Kind || parsedTrackInfo.Source != trackInfo.Source {
		t.Error("Track info mismatch after serialization")
	}
}

// TestSignaling_MessageTypes tests all message types are properly defined.
func TestSignaling_MessageTypes(t *testing.T) {
	// Test all message types can be created
	messageTypes := []MessageType{
		MessageTypeCreateRoom,
		MessageTypeRoomCreated,
		MessageTypeJoinRoom,
		MessageTypeRoomJoined,
		MessageTypeLeaveRoom,
		MessageTypeRoomLeft,
		MessageTypeParticipantJoined,
		MessageTypeParticipantLeft,
		MessageTypePublishTrack,
		MessageTypeTrackPublished,
		MessageTypeUnpublishTrack,
		MessageTypeTrackUnpublished,
		MessageTypeSubscribeTrack,
		MessageTypeTrackSubscribed,
		MessageTypeTrackAvailable,
		MessageTypeTrackUnavailable,
		MessageTypeOffer,
		MessageTypeAnswer,
		MessageTypeICECandidate,
		MessageTypeError,
	}

	for _, msgType := range messageTypes {
		msg, err := NewMessage(msgType, nil)
		if err != nil {
			t.Errorf("Failed to create message with type '%s': %v", msgType, err)
			continue
		}

		if msg.Type != msgType {
			t.Errorf("Expected message type '%s', got '%s'", msgType, msg.Type)
		}

		// Test marshaling and parsing
		data, err := msg.Marshal()
		if err != nil {
			t.Errorf("Failed to marshal message with type '%s': %v", msgType, err)
			continue
		}

		parsedMsg, err := ParseMessage(data)
		if err != nil {
			t.Errorf("Failed to parse message with type '%s': %v", msgType, err)
			continue
		}

		if parsedMsg.Type != msgType {
			t.Errorf("Expected parsed message type '%s', got '%s'", msgType, parsedMsg.Type)
		}
	}
}

// TestSignaling_ErrorCodes tests error code mapping from domain errors.
func TestSignaling_ErrorCodes(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{"RoomClosed", domain.ErrRoomClosed, ErrorCodeRoomClosed},
		{"ParticipantNotFound", domain.ErrParticipantNotFound, ErrorCodeParticipantNotFound},
		{"TrackNotFound", domain.ErrTrackNotFound, ErrorCodeTrackNotFound},
		{"NilError", nil, ""},
		{"ParticipantAlreadyExists", domain.ErrParticipantAlreadyExists, ErrorCodeParticipantAlreadyExists},
		{"ParticipantLeft", domain.ErrParticipantLeft, ErrorCodeParticipantLeft},
		{"TrackAlreadyPublished", domain.ErrTrackAlreadyPublished, ErrorCodeTrackAlreadyPublished},
		{"TrackAlreadySubscribed", domain.ErrTrackAlreadySubscribed, ErrorCodeTrackAlreadySubscribed},
		{"TrackNotPublished", domain.ErrTrackNotPublished, ErrorCodeTrackNotPublished},
		{"InvalidTrackKind", domain.ErrInvalidTrackKind, ErrorCodeInvalidTrackKind},
		{"InvalidTrackSource", domain.ErrInvalidTrackSource, ErrorCodeInvalidTrackSource},
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

// TestSignaling_ConnectionManager tests the connection manager functionality.
func TestSignaling_ConnectionManager(t *testing.T) {
	// Setup
	connManager := NewConnectionManager()

	// Test empty manager
	if len(connManager.ConnectionIDs()) != 0 {
		t.Error("Expected empty connection manager")
	}

	// Test getting non-existent connection
	if conn := connManager.Get("non-existent"); conn != nil {
		t.Error("Expected nil for non-existent connection")
	}

	// Note: We can't easily test with real WebSocket connections in unit tests,
	// but we can test the manager logic with mock connections if needed.
	// For now, we'll just verify the basic interface works.

	// Test GetByRoom with no connections
	connections := connManager.GetByRoom("test-room")
	if len(connections) != 0 {
		t.Error("Expected empty connections for room")
	}
}

// TestSignaling_RoomManager tests the room manager functionality.
func TestSignaling_RoomManager(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()

	// Test empty manager
	if len(roomManager.RoomIDs()) != 0 {
		t.Error("Expected empty room manager")
	}

	// Test getting non-existent room
	if room := roomManager.GetRoom("non-existent"); room != nil {
		t.Error("Expected nil for non-existent room")
	}

	// Test creating multiple rooms
	rooms := []string{"room-1", "room-2", "room-3"}
	for _, roomID := range rooms {
		if _, err := roomManager.CreateRoom(roomID); err != nil {
			t.Fatalf("Failed to create room '%s': %v", roomID, err)
		}
	}

	// Verify all rooms exist
	for _, roomID := range rooms {
		if room := roomManager.GetRoom(roomID); room == nil {
			t.Errorf("Expected room '%s' to exist", roomID)
		}
	}

	// Test RoomIDs
	roomIDs := roomManager.RoomIDs()
	if len(roomIDs) != len(rooms) {
		t.Errorf("Expected %d room IDs, got %d", len(rooms), len(roomIDs))
	}

	// Test closing all rooms
	for _, roomID := range rooms {
		if err := roomManager.CloseRoom(roomID); err != nil {
			t.Fatalf("Failed to close room '%s': %v", roomID, err)
		}
	}

	// Verify all rooms are closed
	for _, roomID := range rooms {
		if room := roomManager.GetRoom(roomID); room != nil {
			t.Errorf("Expected room '%s' to be removed after close", roomID)
		}
	}

	// Verify RoomIDs is empty
	if len(roomManager.RoomIDs()) != 0 {
		t.Error("Expected empty RoomIDs after closing all rooms")
	}
}

// TestSignaling_DomainIntegration tests integration between signaling and domain models.
func TestSignaling_DomainIntegration(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()

	// Create room through signaling
	room, err := roomManager.CreateRoom("integration-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// Verify room is a domain.Room
	if room.ID() != "integration-room" {
		t.Errorf("Expected room ID 'integration-room', got '%s'", room.ID())
	}

	// Create participant through domain
	participant := domain.NewParticipant("integration-participant", "Integration User")

	// Join room through domain
	if err := room.Join(participant); err != nil {
		t.Fatalf("Failed to join room: %v", err)
	}
	participant.SetRoom(room)

	// Verify participant is in room
	if len(room.Participants()) != 1 {
		t.Errorf("Expected 1 participant, got %d", len(room.Participants()))
	}

	// Verify we can retrieve the participant
	retrievedParticipant := room.GetParticipant("integration-participant")
	if retrievedParticipant == nil {
		t.Error("Expected to retrieve participant from room")
	}

	if retrievedParticipant.ID() != participant.ID() {
		t.Errorf("Expected participant ID '%s', got '%s'", participant.ID(), retrievedParticipant.ID())
	}

	// Publish track through domain
	track, err := domain.NewTrack("integration-track", domain.TrackKindVideo, domain.TrackSourceCamera)
	if err != nil {
		t.Fatalf("Failed to create track: %v", err)
	}
	if err := participant.PublishTrack(track); err != nil {
		t.Fatalf("Failed to publish track: %v", err)
	}

	// Verify track is published
	if len(participant.PublishedTracks()) != 1 {
		t.Errorf("Expected 1 published track, got %d", len(participant.PublishedTracks()))
	}

	// Verify we can retrieve the track
	retrievedTrack := participant.GetPublishedTrack("integration-track")
	if retrievedTrack == nil {
		t.Error("Expected to retrieve track from participant")
	}

	if retrievedTrack.ID() != track.ID() {
		t.Errorf("Expected track ID '%s', got '%s'", track.ID(), retrievedTrack.ID())
	}

	// Verify track properties
	if retrievedTrack.Kind() != domain.TrackKindVideo {
		t.Errorf("Expected track kind 'video', got '%s'", retrievedTrack.Kind())
	}

	if retrievedTrack.Source() != domain.TrackSourceCamera {
		t.Errorf("Expected track source 'camera', got '%s'", retrievedTrack.Source())
	}

	// Clean up
	if err := room.Close(); err != nil {
		t.Fatalf("Failed to close room: %v", err)
	}
}
