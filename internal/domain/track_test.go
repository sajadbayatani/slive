package domain

import (
	"testing"
)

func newValidTrack(t *testing.T, id string, kind TrackKind, source TrackSource) *Track {
	t.Helper()

	track, err := NewTrack(id, kind, source)
	if err != nil {
		t.Fatalf("NewTrack(%q, %s, %s): %v", id, kind, source, err)
	}
	return track
}

func TestNewTrack(t *testing.T) {
	audioTrack, err := NewTrack("audio-1", TrackKindAudio, TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if audioTrack.ID() != "audio-1" {
		t.Errorf("Expected track ID to be 'audio-1', got '%s'", audioTrack.ID())
	}

	if audioTrack.Kind() != TrackKindAudio {
		t.Errorf("Expected track kind to be 'audio', got '%s'", audioTrack.Kind())
	}

	if audioTrack.Source() != TrackSourceMicrophone {
		t.Errorf("Expected track source to be 'microphone', got '%s'", audioTrack.Source())
	}

	if audioTrack.State() != TrackStateCreated {
		t.Errorf("Expected track state to be 'created', got '%s'", audioTrack.State())
	}

	if audioTrack.Publisher() != nil {
		t.Error("Expected publisher to be nil")
	}
}

func TestTrackPublish(t *testing.T) {
	audioTrack, err := NewTrack("audio-1", TrackKindAudio, TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	audioTrack.Publish()

	if audioTrack.State() != TrackStatePublished {
		t.Errorf("Expected track state to be 'published', got '%s'", audioTrack.State())
	}
}

func TestTrackUnpublish(t *testing.T) {
	audioTrack, err := NewTrack("audio-1", TrackKindAudio, TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	audioTrack.Publish()

	audioTrack.Unpublish()

	if audioTrack.State() != TrackStateUnpublished {
		t.Errorf("Expected track state to be 'unpublished', got '%s'", audioTrack.State())
	}
}

func TestTrackSetPublisher(t *testing.T) {
	audioTrack := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)
	participant := NewParticipant("participant-1", "Alice")

	audioTrack.SetPublisher(participant)

	if audioTrack.Publisher() != participant {
		t.Error("Expected track's publisher to be set")
	}
}

func TestTrackKindString(t *testing.T) {
	tests := []struct {
		kind     TrackKind
		expected string
	}{
		{TrackKindAudio, "audio"},
		{TrackKindVideo, "video"},
		{TrackKind(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.kind.String(); got != tt.expected {
			t.Errorf("TrackKind(%d).String() = %s, want %s", tt.kind, got, tt.expected)
		}
	}
}

func TestTrackSourceString(t *testing.T) {
	tests := []struct {
		source   TrackSource
		expected string
	}{
		{TrackSourceMicrophone, "microphone"},
		{TrackSourceCamera, "camera"},
		{TrackSourceScreenShare, "screen_share"},
		{TrackSource(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.source.String(); got != tt.expected {
			t.Errorf("TrackSource(%d).String() = %s, want %s", tt.source, got, tt.expected)
		}
	}
}

func TestTrackStateString(t *testing.T) {
	tests := []struct {
		state    TrackState
		expected string
	}{
		{TrackStateCreated, "created"},
		{TrackStatePublished, "published"},
		{TrackStateUnpublished, "unpublished"},
		{TrackState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("TrackState(%d).String() = %s, want %s", tt.state, got, tt.expected)
		}
	}
}

// Integration test: Track-Participant lifecycle
func TestTrackParticipantIntegration(t *testing.T) {
	participant := NewParticipant("participant-1", "Alice")
	audioTrack := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)

	// Set publisher
	audioTrack.SetPublisher(participant)
	if audioTrack.Publisher() != participant {
		t.Error("Expected track publisher to be set")
	}

	// Publish the track
	audioTrack.Publish()
	if audioTrack.State() != TrackStatePublished {
		t.Errorf("Expected track state to be 'published', got '%s'", audioTrack.State())
	}

	// Unpublish the track
	audioTrack.Unpublish()
	if audioTrack.State() != TrackStateUnpublished {
		t.Errorf("Expected track state to be 'unpublished', got '%s'", audioTrack.State())
	}

	// Verify publisher is still set after unpublishing
	if audioTrack.Publisher() != participant {
		t.Error("Expected track publisher to remain set after unpublishing")
	}
}

// Test Track edge cases
func TestTrackEdgeCases(t *testing.T) {
	// Test empty track ID
	track := newValidTrack(t, "", TrackKindAudio, TrackSourceMicrophone)
	if track.ID() != "" {
		t.Errorf("Expected empty track ID, got '%s'", track.ID())
	}

	// Test track with all enum values
	track = newValidTrack(t, "track-1", TrackKindVideo, TrackSourceScreenShare)
	if track.Kind() != TrackKindVideo {
		t.Errorf("Expected track kind to be 'video', got '%s'", track.Kind())
	}
	if track.Source() != TrackSourceScreenShare {
		t.Errorf("Expected track source to be 'screen_share', got '%s'", track.Source())
	}

	// Test track state transitions
	track = newValidTrack(t, "track-1", TrackKindAudio, TrackSourceMicrophone)
	if track.State() != TrackStateCreated {
		t.Errorf("Expected track state to be 'created', got '%s'", track.State())
	}

	track.Publish()
	if track.State() != TrackStatePublished {
		t.Errorf("Expected track state to be 'published', got '%s'", track.State())
	}

	track.Unpublish()
	if track.State() != TrackStateUnpublished {
		t.Errorf("Expected track state to be 'unpublished', got '%s'", track.State())
	}

	// Test setting publisher to nil
	track.SetPublisher(nil)
	if track.Publisher() != nil {
		t.Error("Expected track publisher to be nil")
	}
}

// Test Track state transitions are idempotent
func TestTrackStateTransitions(t *testing.T) {
	track := newValidTrack(t, "track-1", TrackKindAudio, TrackSourceMicrophone)

	// Test publishing multiple times
	track.Publish()
	track.Publish()
	if track.State() != TrackStatePublished {
		t.Errorf("Expected track state to remain 'published', got '%s'", track.State())
	}

	// Test unpublishing multiple times
	track.Unpublish()
	track.Unpublish()
	if track.State() != TrackStateUnpublished {
		t.Errorf("Expected track state to remain 'unpublished', got '%s'", track.State())
	}

	// Test publishing after unpublishing
	track.Publish()
	if track.State() != TrackStatePublished {
		t.Errorf("Expected track state to be 'published' after re-publishing, got '%s'", track.State())
	}
}
