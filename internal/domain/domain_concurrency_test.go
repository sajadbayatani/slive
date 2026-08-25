package domain

import (
	"sync"
	"testing"
)

// TestRoomConcurrency tests concurrent access to Room methods.
func TestRoomConcurrency(t *testing.T) {
	room := NewRoom("concurrent-room")
	_ = room.Create()

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrently join participants
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			participant := NewParticipant("participant-"+string(rune(id)), "User")
			_ = room.Join(participant)
		}(i)
	}

	// Concurrently leave participants
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = room.Leave("participant-" + string(rune(id)))
		}(i)
	}

	wg.Wait()

	// Verify room state is consistent
	if room.State() != RoomStateActive {
		t.Errorf("Expected room state to be 'active', got '%s'", room.State())
	}
}

// TestParticipantConcurrency tests concurrent access to Participant methods.
func TestParticipantConcurrency(t *testing.T) {
	participant := NewParticipant("concurrent-participant", "Concurrent User")

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrently publish tracks
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			track := newValidTrack(t, "track-"+string(rune(id)), TrackKindAudio, TrackSourceMicrophone)
			_ = participant.PublishTrack(track)
		}(i)
	}

	// Concurrently subscribe to tracks
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			track := newValidTrack(t, "track-"+string(rune(id)), TrackKindVideo, TrackSourceCamera)
			_ = participant.SubscribeTrack(track)
		}(i)
	}

	wg.Wait()

	// Verify participant state is consistent
	if participant.State() != ParticipantStateJoined {
		t.Errorf("Expected participant state to be 'joined', got '%s'", participant.State())
	}
}

// TestTrackConcurrency tests concurrent access to Track methods.
func TestTrackConcurrency(t *testing.T) {
	track := newValidTrack(t, "concurrent-track", TrackKindAudio, TrackSourceMicrophone)

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrently publish and unpublish
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			track.Publish()
			track.Unpublish()
		}()
	}

	wg.Wait()

	// Verify track state is consistent
	if track.State() != TrackStateUnpublished {
		t.Errorf("Expected track state to be 'unpublished', got '%s'", track.State())
	}
}

// TestRoomParticipantConcurrency tests concurrent Room and Participant operations.
func TestRoomParticipantConcurrency(t *testing.T) {
	room := NewRoom("concurrent-room")
	_ = room.Create()

	var wg sync.WaitGroup
	numGoroutines := 50

	// Concurrently join and leave participants
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			participant := NewParticipant("participant-"+string(rune(id)), "User")
			_ = room.Join(participant)
			_ = room.Leave("participant-" + string(rune(id)))
		}(i)
	}

	// Concurrently close and reopen the room (should fail gracefully)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = room.Close()
			_ = room.Create()
		}()
	}

	wg.Wait()

	// Verify room state is consistent
	if room.State() != RoomStateClosed {
		t.Errorf("Expected room state to be 'closed', got '%s'", room.State())
	}
}

// TestParticipantTrackConcurrency tests concurrent Participant and Track operations.
func TestParticipantTrackConcurrency(t *testing.T) {
	participant := NewParticipant("concurrent-participant", "Concurrent User")

	var wg sync.WaitGroup
	numGoroutines := 50

	// Concurrently publish and unpublish tracks
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			track := newValidTrack(t, "track-"+string(rune(id)), TrackKindAudio, TrackSourceMicrophone)
			_ = participant.PublishTrack(track)
			_ = participant.UnpublishTrack("track-" + string(rune(id)))
		}(i)
	}

	// Concurrently subscribe and unsubscribe from tracks
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			track := newValidTrack(t, "track-"+string(rune(id)), TrackKindVideo, TrackSourceCamera)
			_ = participant.SubscribeTrack(track)
			_ = participant.UnsubscribeTrack("track-" + string(rune(id)))
		}(i)
	}

	wg.Wait()

	// Verify participant state is consistent
	if participant.State() != ParticipantStateJoined {
		t.Errorf("Expected participant state to be 'joined', got '%s'", participant.State())
	}
}
