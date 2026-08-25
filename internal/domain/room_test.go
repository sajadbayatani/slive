package domain

import (
	"testing"
)

func TestNewRoom(t *testing.T) {
	room := NewRoom("test-room")

	if room.ID() != "test-room" {
		t.Errorf("Expected room ID to be 'test-room', got '%s'", room.ID())
	}

	if room.State() != RoomStateCreated {
		t.Errorf("Expected room state to be 'created', got '%s'", room.State())
	}

	if len(room.Participants()) != 0 {
		t.Errorf("Expected no participants, got %d", len(room.Participants()))
	}
}

func TestRoomCreate(t *testing.T) {
	room := NewRoom("test-room")

	// Test creating a room
	err := room.Create()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if room.State() != RoomStateActive {
		t.Errorf("Expected room state to be 'active', got '%s'", room.State())
	}

	// Test idempotency
	err = room.Create()
	if err != nil {
		t.Errorf("Unexpected error on second Create: %v", err)
	}

	if room.State() != RoomStateActive {
		t.Errorf("Expected room state to remain 'active', got '%s'", room.State())
	}
}

func TestRoomJoin(t *testing.T) {
	room := NewRoom("test-room")
	_ = room.Create()

	participant := NewParticipant("participant-1", "Alice")

	// Test joining a room
	err := room.Join(participant)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(room.Participants()) != 1 {
		t.Errorf("Expected 1 participant, got %d", len(room.Participants()))
	}

	// Test joining with duplicate participant ID
	duplicateParticipant := NewParticipant("participant-1", "Bob")
	err = room.Join(duplicateParticipant)
	if err != ErrParticipantAlreadyExists {
		t.Errorf("Expected ErrParticipantAlreadyExists, got %v", err)
	}

	// Test joining a closed room
	_ = room.Close()
	err = room.Join(participant)
	if err != ErrRoomClosed {
		t.Errorf("Expected ErrRoomClosed, got %v", err)
	}
}

func TestRoomLeave(t *testing.T) {
	room := NewRoom("test-room")
	_ = room.Create()

	participant := NewParticipant("participant-1", "Alice")
	_ = room.Join(participant)

	// Test leaving a room
	err := room.Leave("participant-1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(room.Participants()) != 0 {
		t.Errorf("Expected 0 participants, got %d", len(room.Participants()))
	}

	// Test leaving a non-existent participant
	err = room.Leave("non-existent")
	if err != ErrParticipantNotFound {
		t.Errorf("Expected ErrParticipantNotFound, got %v", err)
	}

	// Test leaving a closed room
	_ = room.Close()
	err = room.Leave("participant-1")
	if err != ErrRoomClosed {
		t.Errorf("Expected ErrRoomClosed, got %v", err)
	}
}

func TestRoomClose(t *testing.T) {
	room := NewRoom("test-room")
	_ = room.Create()

	participant1 := NewParticipant("participant-1", "Alice")
	participant2 := NewParticipant("participant-2", "Bob")
	_ = room.Join(participant1)
	_ = room.Join(participant2)

	// Test closing a room
	err := room.Close()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if room.State() != RoomStateClosed {
		t.Errorf("Expected room state to be 'closed', got '%s'", room.State())
	}

	if len(room.Participants()) != 0 {
		t.Errorf("Expected 0 participants after close, got %d", len(room.Participants()))
	}

	// Test idempotency
	err = room.Close()
	if err != nil {
		t.Errorf("Unexpected error on second Close: %v", err)
	}

	if room.State() != RoomStateClosed {
		t.Errorf("Expected room state to remain 'closed', got '%s'", room.State())
	}
}

func TestRoomGetParticipant(t *testing.T) {
	room := NewRoom("test-room")
	_ = room.Create()

	participant := NewParticipant("participant-1", "Alice")
	_ = room.Join(participant)

	// Test getting a participant
	retrievedParticipant := room.GetParticipant("participant-1")
	if retrievedParticipant == nil {
		t.Error("Expected participant to be found, got nil")
	}

	if retrievedParticipant.ID() != "participant-1" {
		t.Errorf("Expected participant ID to be 'participant-1', got '%s'", retrievedParticipant.ID())
	}

	// Test getting a non-existent participant
	retrievedParticipant = room.GetParticipant("non-existent")
	if retrievedParticipant != nil {
		t.Error("Expected nil for non-existent participant")
	}
}

func TestRoomStateString(t *testing.T) {
	tests := []struct {
		state    RoomState
		expected string
	}{
		{RoomStateCreated, "created"},
		{RoomStateActive, "active"},
		{RoomStateClosed, "closed"},
		{RoomState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("RoomState(%d).String() = %s, want %s", tt.state, got, tt.expected)
		}
	}
}

func TestRoomJoinBeforeCreate(t *testing.T) {
	room := NewRoom("test-room")
	participant := NewParticipant("participant-1", "Alice")

	// Test joining a room that hasn't been created yet
	err := room.Join(participant)
	if err != nil {
		t.Errorf("Expected no error when joining a created room, got %v", err)
	}

	if len(room.Participants()) != 1 {
		t.Errorf("Expected 1 participant, got %d", len(room.Participants()))
	}
}

func TestRoomOperationsAfterClose(t *testing.T) {
	room := NewRoom("test-room")
	_ = room.Create()

	participant := NewParticipant("participant-1", "Alice")
	_ = room.Join(participant)

	_ = room.Close()

	// Test joining after close
	err := room.Join(participant)
	if err != ErrRoomClosed {
		t.Errorf("Expected ErrRoomClosed when joining closed room, got %v", err)
	}

	// Test leaving after close
	err = room.Leave("participant-1")
	if err != ErrRoomClosed {
		t.Errorf("Expected ErrRoomClosed when leaving closed room, got %v", err)
	}

	// Test creating after close
	err = room.Create()
	if err != ErrRoomClosed {
		t.Errorf("Expected ErrRoomClosed when creating closed room, got %v", err)
	}
}

func TestRoomMultipleParticipants(t *testing.T) {
	room := NewRoom("test-room")
	_ = room.Create()

	// Add multiple participants
	participant1 := NewParticipant("participant-1", "Alice")
	participant2 := NewParticipant("participant-2", "Bob")
	participant3 := NewParticipant("participant-3", "Charlie")

	_ = room.Join(participant1)
	_ = room.Join(participant2)
	_ = room.Join(participant3)

	if len(room.Participants()) != 3 {
		t.Errorf("Expected 3 participants, got %d", len(room.Participants()))
	}

	// Test leaving one participant
	_ = room.Leave("participant-2")
	if len(room.Participants()) != 2 {
		t.Errorf("Expected 2 participants after one leaves, got %d", len(room.Participants()))
	}

	// Test that the remaining participants are still there
	if room.GetParticipant("participant-1") == nil {
		t.Error("Expected participant-1 to still be in room")
	}
	if room.GetParticipant("participant-3") == nil {
		t.Error("Expected participant-3 to still be in room")
	}
}

// Integration test: Room-Participant-Track lifecycle
func TestRoomParticipantTrackIntegration(t *testing.T) {
	room := NewRoom("test-room")
	_ = room.Create()

	// Create participants
	publisher := NewParticipant("publisher-1", "Alice")
	subscriber := NewParticipant("subscriber-1", "Bob")

	// Join room
	_ = room.Join(publisher)
	_ = room.Join(subscriber)

	// Publisher creates and publishes a track
	audioTrack := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)
	_ = publisher.PublishTrack(audioTrack)

	// Verify track is published
	if len(publisher.PublishedTracks()) != 1 {
		t.Errorf("Expected 1 published track, got %d", len(publisher.PublishedTracks()))
	}

	// Subscriber subscribes to the track
	_ = subscriber.SubscribeTrack(audioTrack)
	if len(subscriber.SubscribedTracks()) != 1 {
		t.Errorf("Expected 1 subscribed track, got %d", len(subscriber.SubscribedTracks()))
	}

	// Verify track publisher is set
	if audioTrack.Publisher() != publisher {
		t.Error("Expected track publisher to be set")
	}

	// Publisher leaves the room
	publisher.Leave()
	_ = room.Leave("publisher-1")

	// Verify publisher's tracks are cleaned up
	if len(publisher.PublishedTracks()) != 0 {
		t.Errorf("Expected 0 published tracks after leaving, got %d", len(publisher.PublishedTracks()))
	}

	// Subscriber should still have the track subscribed (but it's now dangling)
	if len(subscriber.SubscribedTracks()) != 1 {
		t.Errorf("Expected 1 subscribed track after publisher leaves, got %d", len(subscriber.SubscribedTracks()))
	}
}

// Test error handling for operations on closed room
func TestRoomClosedOperations(t *testing.T) {
	room := NewRoom("test-room")
	_ = room.Create()

	participant := NewParticipant("participant-1", "Alice")
	_ = room.Join(participant)

	// Close the room
	_ = room.Close()

	// Test joining a closed room
	newParticipant := NewParticipant("participant-2", "Bob")
	err := room.Join(newParticipant)
	if err != ErrRoomClosed {
		t.Errorf("Expected ErrRoomClosed when joining closed room, got %v", err)
	}

	// Test leaving a closed room
	err = room.Leave("participant-1")
	if err != ErrRoomClosed {
		t.Errorf("Expected ErrRoomClosed when leaving closed room, got %v", err)
	}

	// Test creating a closed room
	err = room.Create()
	if err != ErrRoomClosed {
		t.Errorf("Expected ErrRoomClosed when creating closed room, got %v", err)
	}
}

// Test Room ID uniqueness and edge cases
func TestRoomEdgeCases(t *testing.T) {
	// Test empty room ID
	room := NewRoom("")
	if room.ID() != "" {
		t.Errorf("Expected empty room ID, got '%s'", room.ID())
	}

	// Test room with special characters in ID
	room = NewRoom("test-room_123!@#")
	if room.ID() != "test-room_123!@#" {
		t.Errorf("Expected room ID to be 'test-room_123!@#', got '%s'", room.ID())
	}
}
