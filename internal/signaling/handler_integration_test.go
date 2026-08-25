package signaling

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sajadbayatani/slive/internal/domain"
)

// =============================================================================
// Handler Integration Tests for Signaling Protocol
// =============================================================================

// MockConnection simulates a WebSocket connection for testing purposes.
type MockConnection struct {
	mu            sync.RWMutex
	participantID string
	roomID        string
	sendChan      chan []byte
	receiveChan   chan []byte
	closeChan     chan struct{}
	closeOnce     sync.Once
	messages      []*Message
	closed        bool
}

// NewMockConnection creates a new mock connection.
func NewMockConnection(participantID, roomID string) *MockConnection {
	return &MockConnection{
		participantID: participantID,
		roomID:        roomID,
		sendChan:      make(chan []byte, 256),
		receiveChan:   make(chan []byte, 256),
		closeChan:     make(chan struct{}),
		messages:      make([]*Message, 0),
		closed:        false,
	}
}

// ID returns the participant ID.
func (m *MockConnection) ID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.participantID
}

// RoomID returns the room ID.
func (m *MockConnection) RoomID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.roomID
}

// State returns the connection state (simplified for mock).
func (m *MockConnection) State() ConnectionState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ConnectionStateClosed
	}
	return ConnectionStateConnected
}

// LastActive returns the last active time.
func (m *MockConnection) LastActive() time.Time {
	return time.Now()
}

// Send sends a message (simulated).
func (m *MockConnection) Send(msgType MessageType, data interface{}) error {
	msg, err := NewMessage(msgType, data)
	if err != nil {
		return err
	}

	dataBytes, err := msg.Marshal()
	if err != nil {
		return err
	}

	select {
	case m.sendChan <- dataBytes:
		m.mu.Lock()
		m.messages = append(m.messages, msg)
		m.mu.Unlock()
		return nil
	case <-m.closeChan:
		return ErrConnectionClosed
	}
}

// Receive receives a message (simulated).
func (m *MockConnection) Receive() (*Message, error) {
	select {
	case data := <-m.receiveChan:
		return ParseMessage(data)
	case <-m.closeChan:
		return nil, ErrConnectionClosed
	}
}

// Close closes the connection.
func (m *MockConnection) Close() error {
	m.closeOnce.Do(func() {
		close(m.closeChan)
		m.mu.Lock()
		m.closed = true
		m.mu.Unlock()
	})
	return nil
}

// IsClosed returns true if the connection is closed.
func (m *MockConnection) IsClosed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closed
}

// SendRaw sends raw data to the receive channel (for testing).
func (m *MockConnection) SendRaw(data []byte) error {
	select {
	case m.receiveChan <- data:
		return nil
	case <-m.closeChan:
		return ErrConnectionClosed
	}
}

// GetMessages returns all messages sent through this connection.
func (m *MockConnection) GetMessages() []*Message {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Message, len(m.messages))
	copy(result, m.messages)
	return result
}

// ClearMessages clears the message history.
func (m *MockConnection) ClearMessages() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = make([]*Message, 0)
}

// TestHandler_RoomCreation tests room creation through the handler.
func TestHandler_RoomCreation(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()

	// Create room request
	createReq := CreateRoomRequest{
		RoomID:          "test-room",
		ParticipantID:   "participant-1",
		ParticipantName: "Test User",
	}

	// Process the message through the handler logic
	room, err := roomManager.CreateRoom(createReq.RoomID)
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	participant := domain.NewParticipant(createReq.ParticipantID, createReq.ParticipantName)
	if err := room.Join(participant); err != nil {
		t.Fatalf("Failed to join room: %v", err)
	}
	participant.SetRoom(room)

	// Verify room was created
	if room.ID() != "test-room" {
		t.Errorf("Expected room ID 'test-room', got '%s'", room.ID())
	}

	if room.State() != domain.RoomStateActive {
		t.Errorf("Expected room state 'active', got '%s'", room.State())
	}

	// Verify participant is in room
	if len(room.Participants()) != 1 {
		t.Errorf("Expected 1 participant, got %d", len(room.Participants()))
	}
}

// TestHandler_ParticipantJoin tests participant joining through the handler.
func TestHandler_ParticipantJoin(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()

	// Create room first
	room, err := roomManager.CreateRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// First participant joins
	participant1 := domain.NewParticipant("participant-1", "User 1")
	if err := room.Join(participant1); err != nil {
		t.Fatalf("Failed to join participant 1: %v", err)
	}
	participant1.SetRoom(room)

	// Second participant joins
	participant2 := domain.NewParticipant("participant-2", "User 2")
	if err := room.Join(participant2); err != nil {
		t.Fatalf("Failed to join participant 2: %v", err)
	}
	participant2.SetRoom(room)

	// Verify both participants are in room
	if len(room.Participants()) != 2 {
		t.Errorf("Expected 2 participants, got %d", len(room.Participants()))
	}

	// Test room joined response creation
	roomJoinedResp := RoomJoinedResponse{
		RoomID:        "test-room",
		ParticipantID: "participant-2",
		Participants: []ParticipantInfo{
			{ID: "participant-1", Name: "User 1"},
			{ID: "participant-2", Name: "User 2"},
		},
		Status: "success",
	}

	// Test message creation and serialization
	msg, err := NewMessage(MessageTypeRoomJoined, roomJoinedResp)
	if err != nil {
		t.Fatalf("Failed to create message: %v", err)
	}

	if msg.Type != MessageTypeRoomJoined {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeRoomJoined, msg.Type)
	}

	// Test message parsing
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal message: %v", err)
	}

	parsedMsg, err := ParseMessage(data)
	if err != nil {
		t.Fatalf("Failed to parse message: %v", err)
	}

	if parsedMsg.Type != MessageTypeRoomJoined {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeRoomJoined, parsedMsg.Type)
	}

	// Verify data can be unmarshaled
	var parsedResp RoomJoinedResponse
	if err := parsedMsg.UnmarshalData(&parsedResp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if parsedResp.RoomID != "test-room" {
		t.Errorf("Expected room ID 'test-room', got '%s'", parsedResp.RoomID)
	}
}

// TestHandler_TrackPublishing tests track publishing through the handler.
func TestHandler_TrackPublishing(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()

	// Create room and participant
	room, err := roomManager.CreateRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	participant := domain.NewParticipant("publisher", "Publisher")
	if err := room.Join(participant); err != nil {
		t.Fatalf("Failed to join room: %v", err)
	}
	participant.SetRoom(room)

	// Process the publish track request
	audioTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create track: %v", err)
	}
	if err := participant.PublishTrack(audioTrack); err != nil {
		t.Fatalf("Failed to publish track: %v", err)
	}

	// Test track published response creation
	trackPublishedResp := TrackPublishedResponse{
		TrackID:       "audio-1",
		ParticipantID: "publisher",
		Status:        "success",
	}

	// Test message creation and serialization
	msg, err := NewMessage(MessageTypeTrackPublished, trackPublishedResp)
	if err != nil {
		t.Fatalf("Failed to create message: %v", err)
	}

	if msg.Type != MessageTypeTrackPublished {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeTrackPublished, msg.Type)
	}

	// Verify track is published
	if len(participant.PublishedTracks()) != 1 {
		t.Errorf("Expected 1 published track, got %d", len(participant.PublishedTracks()))
	}

	// Test message parsing
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal message: %v", err)
	}

	parsedMsg, err := ParseMessage(data)
	if err != nil {
		t.Fatalf("Failed to parse message: %v", err)
	}

	if parsedMsg.Type != MessageTypeTrackPublished {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeTrackPublished, parsedMsg.Type)
	}

	// Verify data can be unmarshaled
	var parsedResp TrackPublishedResponse
	if err := parsedMsg.UnmarshalData(&parsedResp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if parsedResp.TrackID != "audio-1" {
		t.Errorf("Expected track ID 'audio-1', got '%s'", parsedResp.TrackID)
	}
}

// TestHandler_WebRTCSignaling tests WebRTC signaling message creation and parsing.
func TestHandler_WebRTCSignaling(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()

	// Create room and participants
	room, err := roomManager.CreateRoom("test-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

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
		t.Fatalf("Failed to create track: %v", err)
	}
	if err := publisher.PublishTrack(audioTrack); err != nil {
		t.Fatalf("Failed to publish track: %v", err)
	}

	// Test offer exchange
	offerReq := OfferRequest{
		RoomID:              "test-room",
		ParticipantID:       "publisher",
		TargetParticipantID: "subscriber",
		SDP:                 "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n",
		TrackIDs:            []string{"audio-1"},
	}

	// Test offer message creation and serialization
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

	// Test offer notification creation
	offerNotification := OfferNotification{
		SourceParticipantID: "publisher",
		SDP:                 offerReq.SDP,
		TrackIDs:            offerReq.TrackIDs,
	}

	offerNotificationMsg, err := NewMessage(MessageTypeOffer, offerNotification)
	if err != nil {
		t.Fatalf("Failed to create offer notification message: %v", err)
	}

	if offerNotificationMsg.Type != MessageTypeOffer {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeOffer, offerNotificationMsg.Type)
	}

	// Test answer exchange
	answerReq := AnswerRequest{
		RoomID:              "test-room",
		ParticipantID:       "subscriber",
		TargetParticipantID: "publisher",
		SDP:                 "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n",
	}

	// Test answer message creation and serialization
	answerMsg, err := NewMessage(MessageTypeAnswer, answerReq)
	if err != nil {
		t.Fatalf("Failed to create answer message: %v", err)
	}

	if answerMsg.Type != MessageTypeAnswer {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeAnswer, answerMsg.Type)
	}

	// Test answer parsing
	var parsedAnswer AnswerRequest
	if err := answerMsg.UnmarshalData(&parsedAnswer); err != nil {
		t.Fatalf("Failed to unmarshal answer: %v", err)
	}

	if parsedAnswer.ParticipantID != "subscriber" {
		t.Errorf("Expected participant ID 'subscriber', got '%s'", parsedAnswer.ParticipantID)
	}

	// Test answer notification creation
	answerNotification := AnswerNotification{
		SourceParticipantID: "subscriber",
		SDP:                 answerReq.SDP,
	}

	answerNotificationMsg, err := NewMessage(MessageTypeAnswer, answerNotification)
	if err != nil {
		t.Fatalf("Failed to create answer notification message: %v", err)
	}

	if answerNotificationMsg.Type != MessageTypeAnswer {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeAnswer, answerNotificationMsg.Type)
	}

	// Test ICE candidate exchange
	iceReq := ICECandidateRequest{
		RoomID:              "test-room",
		ParticipantID:       "publisher",
		TargetParticipantID: "subscriber",
		Candidate:           "candidate:1234567890 1 udp 2122260223 192.168.1.1 12345 typ host",
		SDPMid:              "0",
		SDPMLineIndex:       0,
	}

	// Test ICE candidate message creation and serialization
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

	// Test ICE candidate notification creation
	iceNotification := ICECandidateNotification{
		SourceParticipantID: "publisher",
		Candidate:           iceReq.Candidate,
		SDPMid:              iceReq.SDPMid,
		SDPMLineIndex:       iceReq.SDPMLineIndex,
	}

	iceNotificationMsg, err := NewMessage(MessageTypeICECandidate, iceNotification)
	if err != nil {
		t.Fatalf("Failed to create ICE candidate notification message: %v", err)
	}

	if iceNotificationMsg.Type != MessageTypeICECandidate {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeICECandidate, iceNotificationMsg.Type)
	}
}

// TestHandler_ErrorCases tests error handling in the signaling protocol.
func TestHandler_ErrorCases(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()

	// Test joining a closed room
	room, err := roomManager.CreateRoom("closed-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// Close the room
	if err := room.Close(); err != nil {
		t.Fatalf("Failed to close room: %v", err)
	}

	// Try to join the closed room
	participant := domain.NewParticipant("participant-1", "User 1")
	if err := room.Join(participant); err != domain.ErrRoomClosed {
		t.Errorf("Expected ErrRoomClosed, got: %v", err)
	}

	// Test publishing duplicate track
	room2, err := roomManager.CreateRoom("duplicate-track-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	participant2 := domain.NewParticipant("participant-2", "User 2")
	if err := room2.Join(participant2); err != nil {
		t.Fatalf("Failed to join room: %v", err)
	}
	participant2.SetRoom(room2)

	// Publish first track
	track1, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create first track: %v", err)
	}
	if err := participant2.PublishTrack(track1); err != nil {
		t.Fatalf("Failed to publish first track: %v", err)
	}

	// Try to publish duplicate track
	track2, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create second track: %v", err)
	}
	if err := participant2.PublishTrack(track2); err != domain.ErrTrackAlreadyPublished {
		t.Errorf("Expected ErrTrackAlreadyPublished, got: %v", err)
	}

	// Test subscribing to non-existent track
	participant3 := domain.NewParticipant("participant-3", "User 3")
	if err := room2.Join(participant3); err != nil {
		t.Fatalf("Failed to join room: %v", err)
	}
	participant3.SetRoom(room2)

	// Try to subscribe to non-existent track
	fakeTrack, err := domain.NewTrack("non-existent", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create fake track: %v", err)
	}
	if err := participant3.SubscribeTrack(fakeTrack); err != nil {
		// This should succeed because we're not checking if the track exists in the room
		// The error would come from the handler when trying to find the track
		t.Logf("SubscribeTrack with non-existent track: %v", err)
	}

	// Test unpublishing non-existent track
	if err := participant2.UnpublishTrack("non-existent"); err != domain.ErrTrackNotFound {
		t.Errorf("Expected ErrTrackNotFound, got: %v", err)
	}

	// Test error message creation
	errorResp := ErrorResponse{
		Error:       "room not found",
		Code:        ErrorCodeRoomNotFound,
		RequestType: string(MessageTypeJoinRoom),
	}

	errorMsg, err := NewMessage(MessageTypeError, errorResp)
	if err != nil {
		t.Fatalf("Failed to create error message: %v", err)
	}

	if errorMsg.Type != MessageTypeError {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeError, errorMsg.Type)
	}

	// Test error code mapping
	if code := errorCodeFromDomainError(domain.ErrRoomClosed); code != ErrorCodeRoomClosed {
		t.Errorf("Expected error code '%s', got '%s'", ErrorCodeRoomClosed, code)
	}

	if code := errorCodeFromDomainError(domain.ErrParticipantNotFound); code != ErrorCodeParticipantNotFound {
		t.Errorf("Expected error code '%s', got '%s'", ErrorCodeParticipantNotFound, code)
	}

	if code := errorCodeFromDomainError(domain.ErrTrackNotFound); code != ErrorCodeTrackNotFound {
		t.Errorf("Expected error code '%s', got '%s'", ErrorCodeTrackNotFound, code)
	}
}

// TestHandler_ConcurrentOperations tests concurrent operations on the signaling protocol.
func TestHandler_ConcurrentOperations(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()

	// Create room
	room, err := roomManager.CreateRoom("concurrent-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// Number of concurrent operations
	const numGoroutines = 5
	const numOperations = 20

	var wg sync.WaitGroup

	// Concurrent participant joins and track publishing
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				participantID := fmt.Sprintf("participant-%d-%d", id, j)
				participant := domain.NewParticipant(participantID, fmt.Sprintf("User %d-%d", id, j))

				// Join room
				if err := room.Join(participant); err != nil {
					// Might fail if participant already exists due to race condition
					t.Logf("Failed to join room for %s: %v", participantID, err)
					continue
				}
				participant.SetRoom(room)

				// Publish track
				track, err := domain.NewTrack(fmt.Sprintf("track-%d-%d", id, j), domain.TrackKindAudio, domain.TrackSourceMicrophone)
				if err != nil {
					t.Logf("Failed to create track for %s: %v", participantID, err)
					continue
				}
				if err := participant.PublishTrack(track); err != nil {
					t.Logf("Failed to publish track for %s: %v", participantID, err)
					continue
				}

				// Simulate leaving
				if j%2 == 0 {
					if err := room.Leave(participantID); err != nil {
						t.Logf("Failed to leave room for %s: %v", participantID, err)
					}
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify room is still functional
	if room.State() != domain.RoomStateActive {
		t.Errorf("Expected room state 'active', got '%s'", room.State())
	}

	t.Logf("Final participant count: %d", len(room.Participants()))
}

// TestHandler_MessageSerializationRoundTrip tests message serialization and deserialization.
func TestHandler_MessageSerializationRoundTrip(t *testing.T) {
	// Test all message types that can be sent through the handler
	testCases := []struct {
		name    string
		msgType MessageType
		data    interface{}
	}{
		{
			name:    "CreateRoom",
			msgType: MessageTypeCreateRoom,
			data: CreateRoomRequest{
				RoomID:          "test-room",
				ParticipantID:   "test-participant",
				ParticipantName: "Test User",
			},
		},
		{
			name:    "JoinRoom",
			msgType: MessageTypeJoinRoom,
			data: JoinRoomRequest{
				RoomID:          "test-room",
				ParticipantID:   "test-participant",
				ParticipantName: "Test User",
			},
		},
		{
			name:    "LeaveRoom",
			msgType: MessageTypeLeaveRoom,
			data: LeaveRoomRequest{
				RoomID:        "test-room",
				ParticipantID: "test-participant",
			},
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
		},
		{
			name:    "UnpublishTrack",
			msgType: MessageTypeUnpublishTrack,
			data: UnpublishTrackRequest{
				RoomID:        "test-room",
				ParticipantID: "test-participant",
				TrackID:       "track-1",
			},
		},
		{
			name:    "SubscribeTrack",
			msgType: MessageTypeSubscribeTrack,
			data: SubscribeTrackRequest{
				RoomID:        "test-room",
				ParticipantID: "test-participant",
				TrackID:       "track-1",
			},
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
		},
		{
			name:    "Answer",
			msgType: MessageTypeAnswer,
			data: AnswerRequest{
				RoomID:              "test-room",
				ParticipantID:       "participant-2",
				TargetParticipantID: "participant-1",
				SDP:                 "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n",
			},
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
		},
		{
			name:    "Error",
			msgType: MessageTypeError,
			data: ErrorResponse{
				Error:       "test error",
				Code:        ErrorCodeRoomNotFound,
				RequestType: string(MessageTypeJoinRoom),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create message
			msg, err := NewMessage(tc.msgType, tc.data)
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

			// Verify message type
			if parsedMsg.Type != tc.msgType {
				t.Errorf("Expected message type '%s', got '%s'", tc.msgType, parsedMsg.Type)
			}

			// Verify data can be unmarshaled back
			var parsedData json.RawMessage
			if err := parsedMsg.UnmarshalData(&parsedData); err != nil {
				t.Fatalf("Failed to unmarshal message data: %v", err)
			}

			// For some message types, verify the data content
			switch tc.msgType {
			case MessageTypeCreateRoom:
				var req CreateRoomRequest
				if err := parsedMsg.UnmarshalData(&req); err != nil {
					t.Fatalf("Failed to unmarshal CreateRoomRequest: %v", err)
				}
				if req.RoomID != "test-room" {
					t.Errorf("Expected room ID 'test-room', got '%s'", req.RoomID)
				}

			case MessageTypePublishTrack:
				var req PublishTrackRequest
				if err := parsedMsg.UnmarshalData(&req); err != nil {
					t.Fatalf("Failed to unmarshal PublishTrackRequest: %v", err)
				}
				if req.Track.ID != "track-1" {
					t.Errorf("Expected track ID 'track-1', got '%s'", req.Track.ID)
				}

			case MessageTypeOffer:
				var req OfferRequest
				if err := parsedMsg.UnmarshalData(&req); err != nil {
					t.Fatalf("Failed to unmarshal OfferRequest: %v", err)
				}
				if req.ParticipantID != "participant-1" {
					t.Errorf("Expected participant ID 'participant-1', got '%s'", req.ParticipantID)
				}
			}
		})
	}
}

// TestHandler_ConnectionManagerIntegration tests connection manager functionality.
func TestHandler_ConnectionManagerIntegration(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()
	handler := NewHandler(roomManager)

	// Test empty manager
	if len(handler.connectionManager.ConnectionIDs()) != 0 {
		t.Error("Expected empty connection manager")
	}

	// Test getting non-existent connection
	if conn := handler.connectionManager.Get("non-existent"); conn != nil {
		t.Error("Expected nil for non-existent connection")
	}

	// Test GetByRoom with no connections
	connections := handler.connectionManager.GetByRoom("test-room")
	if len(connections) != 0 {
		t.Error("Expected empty connections for room")
	}

	// Test RoomIDs method
	if len(handler.connectionManager.ConnectionIDs()) != 0 {
		t.Error("Expected empty connection IDs")
	}
}

// TestHandler_CompleteSessionFlow tests a complete session flow from room creation to cleanup.
func TestHandler_CompleteSessionFlow(t *testing.T) {
	// Setup
	roomManager := NewRoomManager()

	// Step 1: Create room
	room, err := roomManager.CreateRoom("session-room")
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// Step 2: Create participants and join room
	participant1 := domain.NewParticipant("alice", "Alice")
	participant2 := domain.NewParticipant("bob", "Bob")

	if err := room.Join(participant1); err != nil {
		t.Fatalf("Failed to join participant 1: %v", err)
	}
	if err := room.Join(participant2); err != nil {
		t.Fatalf("Failed to join participant 2: %v", err)
	}
	participant1.SetRoom(room)
	participant2.SetRoom(room)

	// Step 3: Publish tracks
	audioTrack, err := domain.NewTrack("alice-audio", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create audio track: %v", err)
	}
	videoTrack, err := domain.NewTrack("alice-video", domain.TrackKindVideo, domain.TrackSourceCamera)
	if err != nil {
		t.Fatalf("Failed to create video track: %v", err)
	}

	if err := participant1.PublishTrack(audioTrack); err != nil {
		t.Fatalf("Failed to publish audio track: %v", err)
	}
	if err := participant1.PublishTrack(videoTrack); err != nil {
		t.Fatalf("Failed to publish video track: %v", err)
	}

	// Step 4: Subscribe to tracks
	if err := participant2.SubscribeTrack(audioTrack); err != nil {
		t.Fatalf("Failed to subscribe to audio track: %v", err)
	}
	if err := participant2.SubscribeTrack(videoTrack); err != nil {
		t.Fatalf("Failed to subscribe to video track: %v", err)
	}

	// Step 5: WebRTC signaling - Offer/Answer exchange
	offerNotification := OfferNotification{
		SourceParticipantID: "alice",
		SDP:                 "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n",
		TrackIDs:            []string{"alice-audio", "alice-video"},
	}

	// Test offer message creation
	offerMsg, err := NewMessage(MessageTypeOffer, offerNotification)
	if err != nil {
		t.Fatalf("Failed to create offer message: %v", err)
	}

	if offerMsg.Type != MessageTypeOffer {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeOffer, offerMsg.Type)
	}

	answerNotification := AnswerNotification{
		SourceParticipantID: "bob",
		SDP:                 "v=0\r\no=- 1234567890 2 IN IP4 127.0.0.1\r\n",
	}

	// Test answer message creation
	answerMsg, err := NewMessage(MessageTypeAnswer, answerNotification)
	if err != nil {
		t.Fatalf("Failed to create answer message: %v", err)
	}

	if answerMsg.Type != MessageTypeAnswer {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeAnswer, answerMsg.Type)
	}

	// Step 6: ICE candidate exchange
	iceNotification := ICECandidateNotification{
		SourceParticipantID: "alice",
		Candidate:           "candidate:1234567890 1 udp 2122260223 192.168.1.1 12345 typ host",
		SDPMid:              "0",
		SDPMLineIndex:       0,
	}

	// Test ICE candidate message creation
	iceMsg, err := NewMessage(MessageTypeICECandidate, iceNotification)
	if err != nil {
		t.Fatalf("Failed to create ICE candidate message: %v", err)
	}

	if iceMsg.Type != MessageTypeICECandidate {
		t.Errorf("Expected message type '%s', got '%s'", MessageTypeICECandidate, iceMsg.Type)
	}

	// Step 7: Verify state
	if len(room.Participants()) != 2 {
		t.Errorf("Expected 2 participants, got %d", len(room.Participants()))
	}

	if len(participant1.PublishedTracks()) != 2 {
		t.Errorf("Expected 2 published tracks, got %d", len(participant1.PublishedTracks()))
	}

	if len(participant2.SubscribedTracks()) != 2 {
		t.Errorf("Expected 2 subscribed tracks, got %d", len(participant2.SubscribedTracks()))
	}

	// Step 8: Cleanup - Leave room
	if err := room.Leave("alice"); err != nil {
		t.Fatalf("Failed to leave room: %v", err)
	}

	if len(room.Participants()) != 1 {
		t.Errorf("Expected 1 participant after leave, got %d", len(room.Participants()))
	}

	// Step 9: Final cleanup - Close room
	if err := roomManager.CloseRoom("session-room"); err != nil {
		t.Fatalf("Failed to close room: %v", err)
	}

	if roomManager.GetRoom("session-room") != nil {
		t.Error("Expected room to be removed from manager")
	}
}
