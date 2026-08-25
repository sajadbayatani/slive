package domain

import (
	"testing"
)

func TestNewParticipant(t *testing.T) {
	participant := NewParticipant("participant-1", "Alice")

	if participant.ID() != "participant-1" {
		t.Errorf("Expected participant ID to be 'participant-1', got '%s'", participant.ID())
	}

	if participant.Name() != "Alice" {
		t.Errorf("Expected participant name to be 'Alice', got '%s'", participant.Name())
	}

	if participant.State() != ParticipantStateJoined {
		t.Errorf("Expected participant state to be 'joined', got '%s'", participant.State())
	}

	if participant.Room() != nil {
		t.Error("Expected room to be nil")
	}

	if len(participant.PublishedTracks()) != 0 {
		t.Errorf("Expected no published tracks, got %d", len(participant.PublishedTracks()))
	}

	if len(participant.SubscribedTracks()) != 0 {
		t.Errorf("Expected no subscribed tracks, got %d", len(participant.SubscribedTracks()))
	}
}

func TestParticipantActivate(t *testing.T) {
	participant := NewParticipant("participant-1", "Alice")

	participant.Activate()

	if participant.State() != ParticipantStateActive {
		t.Errorf("Expected participant state to be 'active', got '%s'", participant.State())
	}
}

func TestParticipantLeave(t *testing.T) {
	participant := NewParticipant("participant-1", "Alice")
	room := NewRoom("test-room")
	participant.SetRoom(room)

	// Add some tracks
	audioTrack := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)
	_ = participant.PublishTrack(audioTrack)

	videoTrack := newValidTrack(t, "video-1", TrackKindVideo, TrackSourceCamera)
	videoTrack.Publish()
	_ = participant.SubscribeTrack(videoTrack)

	participant.Leave()

	if participant.State() != ParticipantStateLeft {
		t.Errorf("Expected participant state to be 'left', got '%s'", participant.State())
	}

	if participant.Room() != nil {
		t.Error("Expected room to be nil after leaving")
	}

	if len(participant.PublishedTracks()) != 0 {
		t.Errorf("Expected no published tracks after leaving, got %d", len(participant.PublishedTracks()))
	}

	if len(participant.SubscribedTracks()) != 0 {
		t.Errorf("Expected no subscribed tracks after leaving, got %d", len(participant.SubscribedTracks()))
	}
}

func TestParticipantSetRoom(t *testing.T) {
	participant := NewParticipant("participant-1", "Alice")
	room := NewRoom("test-room")

	participant.SetRoom(room)

	if participant.Room() != room {
		t.Error("Expected participant's room to be set")
	}
}

func TestParticipantPublishTrack(t *testing.T) {
	participant := NewParticipant("participant-1", "Alice")
	audioTrack := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)

	// Test publishing a track
	err := participant.PublishTrack(audioTrack)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(participant.PublishedTracks()) != 1 {
		t.Errorf("Expected 1 published track, got %d", len(participant.PublishedTracks()))
	}

	if participant.GetPublishedTrack("audio-1") == nil {
		t.Error("Expected to retrieve published track")
	}

	// Test publishing a duplicate track
	err = participant.PublishTrack(audioTrack)
	if err != ErrTrackAlreadyPublished {
		t.Errorf("Expected ErrTrackAlreadyPublished, got %v", err)
	}

	// Test publishing after leaving
	participant.Leave()
	err = participant.PublishTrack(audioTrack)
	if err != ErrParticipantLeft {
		t.Errorf("Expected ErrParticipantLeft, got %v", err)
	}
}

func TestParticipantUnpublishTrack(t *testing.T) {
	participant := NewParticipant("participant-1", "Alice")
	audioTrack := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)
	_ = participant.PublishTrack(audioTrack)

	// Test unpublishing a track
	err := participant.UnpublishTrack("audio-1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(participant.PublishedTracks()) != 0 {
		t.Errorf("Expected 0 published tracks, got %d", len(participant.PublishedTracks()))
	}

	// Test unpublishing a non-existent track
	err = participant.UnpublishTrack("non-existent")
	if err != ErrTrackNotFound {
		t.Errorf("Expected ErrTrackNotFound, got %v", err)
	}

	// Test unpublishing after leaving
	participant.Leave()
	err = participant.UnpublishTrack("audio-1")
	if err != ErrParticipantLeft {
		t.Errorf("Expected ErrParticipantLeft, got %v", err)
	}
}

func TestParticipantSubscribeTrack(t *testing.T) {
	participant := NewParticipant("participant-1", "Alice")
	videoTrack := newValidTrack(t, "video-1", TrackKindVideo, TrackSourceCamera)
	videoTrack.Publish()

	// Test subscribing to a track
	err := participant.SubscribeTrack(videoTrack)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(participant.SubscribedTracks()) != 1 {
		t.Errorf("Expected 1 subscribed track, got %d", len(participant.SubscribedTracks()))
	}

	if participant.GetSubscribedTrack("video-1") == nil {
		t.Error("Expected to retrieve subscribed track")
	}

	// Test subscribing to a duplicate track
	err = participant.SubscribeTrack(videoTrack)
	if err != ErrTrackAlreadySubscribed {
		t.Errorf("Expected ErrTrackAlreadySubscribed, got %v", err)
	}

	// Test subscribing after leaving
	participant.Leave()
	err = participant.SubscribeTrack(videoTrack)
	if err != ErrParticipantLeft {
		t.Errorf("Expected ErrParticipantLeft, got %v", err)
	}
}

func TestParticipantUnsubscribeTrack(t *testing.T) {
	participant := NewParticipant("participant-1", "Alice")
	videoTrack := newValidTrack(t, "video-1", TrackKindVideo, TrackSourceCamera)
	videoTrack.Publish()
	_ = participant.SubscribeTrack(videoTrack)

	// Test unsubscribing from a track
	err := participant.UnsubscribeTrack("video-1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(participant.SubscribedTracks()) != 0 {
		t.Errorf("Expected 0 subscribed tracks, got %d", len(participant.SubscribedTracks()))
	}

	// Test unsubscribing from a non-existent track
	err = participant.UnsubscribeTrack("non-existent")
	if err != ErrTrackNotFound {
		t.Errorf("Expected ErrTrackNotFound, got %v", err)
	}

	// Test unsubscribing after leaving
	participant.Leave()
	err = participant.UnsubscribeTrack("video-1")
	if err != ErrParticipantLeft {
		t.Errorf("Expected ErrParticipantLeft, got %v", err)
	}
}

func TestParticipantStateString(t *testing.T) {
	tests := []struct {
		state    ParticipantState
		expected string
	}{
		{ParticipantStateJoined, "joined"},
		{ParticipantStateActive, "active"},
		{ParticipantStateLeft, "left"},
		{ParticipantState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("ParticipantState(%d).String() = %s, want %s", tt.state, got, tt.expected)
		}
	}
}

// Integration test: Participant-Track lifecycle
func TestParticipantTrackLifecycle(t *testing.T) {
	participant := NewParticipant("participant-1", "Alice")

	// Create tracks
	audioTrack := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)
	videoTrack := newValidTrack(t, "video-1", TrackKindVideo, TrackSourceCamera)

	// Publish tracks
	_ = participant.PublishTrack(audioTrack)
	_ = participant.PublishTrack(videoTrack)

	// Verify tracks are published
	if len(participant.PublishedTracks()) != 2 {
		t.Errorf("Expected 2 published tracks, got %d", len(participant.PublishedTracks()))
	}

	// Subscribe to tracks
	_ = participant.SubscribeTrack(audioTrack)
	_ = participant.SubscribeTrack(videoTrack)

	// Verify tracks are subscribed
	if len(participant.SubscribedTracks()) != 2 {
		t.Errorf("Expected 2 subscribed tracks, got %d", len(participant.SubscribedTracks()))
	}

	// Unpublish one track
	_ = participant.UnpublishTrack("audio-1")
	if len(participant.PublishedTracks()) != 1 {
		t.Errorf("Expected 1 published track after unpublishing, got %d", len(participant.PublishedTracks()))
	}

	// Unsubscribe from one track
	_ = participant.UnsubscribeTrack("video-1")
	if len(participant.SubscribedTracks()) != 1 {
		t.Errorf("Expected 1 subscribed track after unsubscribing, got %d", len(participant.SubscribedTracks()))
	}

	// Leave and verify cleanup
	participant.Leave()
	if len(participant.PublishedTracks()) != 0 {
		t.Errorf("Expected 0 published tracks after leaving, got %d", len(participant.PublishedTracks()))
	}
	if len(participant.SubscribedTracks()) != 0 {
		t.Errorf("Expected 0 subscribed tracks after leaving, got %d", len(participant.SubscribedTracks()))
	}
}

// Test error handling for track operations
func TestParticipantTrackErrors(t *testing.T) {
	participant := NewParticipant("participant-1", "Alice")

	// Test publishing a track after leaving
	participant.Leave()
	audioTrack := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)
	err := participant.PublishTrack(audioTrack)
	if err != ErrParticipantLeft {
		t.Errorf("Expected ErrParticipantLeft when publishing after leaving, got %v", err)
	}

	// Test unpublishing a track after leaving
	err = participant.UnpublishTrack("audio-1")
	if err != ErrParticipantLeft {
		t.Errorf("Expected ErrParticipantLeft when unpublishing after leaving, got %v", err)
	}

	// Test subscribing to a track after leaving
	err = participant.SubscribeTrack(audioTrack)
	if err != ErrParticipantLeft {
		t.Errorf("Expected ErrParticipantLeft when subscribing after leaving, got %v", err)
	}

	// Test unsubscribing from a track after leaving
	err = participant.UnsubscribeTrack("audio-1")
	if err != ErrParticipantLeft {
		t.Errorf("Expected ErrParticipantLeft when unsubscribing after leaving, got %v", err)
	}
}

// Test Participant edge cases
func TestParticipantEdgeCases(t *testing.T) {
	// Test empty participant ID
	participant := NewParticipant("", "")
	if participant.ID() != "" {
		t.Errorf("Expected empty participant ID, got '%s'", participant.ID())
	}

	// Test participant with special characters in name
	participant = NewParticipant("participant-1", "Alice!@#")
	if participant.Name() != "Alice!@#" {
		t.Errorf("Expected participant name to be 'Alice!@#', got '%s'", participant.Name())
	}

	// Test publishing duplicate tracks
	participant = NewParticipant("participant-1", "Alice")
	audioTrack := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)
	_ = participant.PublishTrack(audioTrack)
	err := participant.PublishTrack(audioTrack)
	if err != ErrTrackAlreadyPublished {
		t.Errorf("Expected ErrTrackAlreadyPublished, got %v", err)
	}

	// Test subscribing to duplicate tracks
	videoTrack := newValidTrack(t, "video-1", TrackKindVideo, TrackSourceCamera)
	videoTrack.Publish()
	_ = participant.SubscribeTrack(videoTrack)
	err = participant.SubscribeTrack(videoTrack)
	if err != ErrTrackAlreadySubscribed {
		t.Errorf("Expected ErrTrackAlreadySubscribed, got %v", err)
	}

	// Test unpublishing non-existent track
	err = participant.UnpublishTrack("non-existent")
	if err != ErrTrackNotFound {
		t.Errorf("Expected ErrTrackNotFound, got %v", err)
	}

	// Test unsubscribing from non-existent track
	err = participant.UnsubscribeTrack("non-existent")
	if err != ErrTrackNotFound {
		t.Errorf("Expected ErrTrackNotFound, got %v", err)
	}
}
