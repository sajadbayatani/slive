package domain

import (
	"errors"
	"testing"
)

// --- Track construction sentinels ---

func TestNewTrackInvalidKindAndSource(t *testing.T) {
	if _, err := NewTrack("track-1", TrackKind(99), TrackSourceMicrophone); !errors.Is(err, ErrInvalidTrackKind) {
		t.Errorf("Expected ErrInvalidTrackKind, got %v", err)
	}

	if _, err := NewTrack("track-1", TrackKindAudio, TrackSource(99)); !errors.Is(err, ErrInvalidTrackSource) {
		t.Errorf("Expected ErrInvalidTrackSource, got %v", err)
	}
}

// --- Room track registry lifecycle ---

func TestRoomTrackRegistryLifecycle(t *testing.T) {
	room := NewRoom("registry-room")
	_ = room.Create()

	publisher := NewParticipant("publisher", "Alice")
	_ = room.Join(publisher)

	track := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)

	// Empty registry lookups.
	if got := room.GetTrack("audio-1"); got != nil {
		t.Error("Expected nil for missing track before publication")
	}
	if len(room.Tracks()) != 0 {
		t.Errorf("Expected empty registry, got %d entries", len(room.Tracks()))
	}
	if err := room.UnpublishTrack("audio-1"); !errors.Is(err, ErrTrackNotFound) {
		t.Errorf("Expected ErrTrackNotFound when unpublishing missing track, got %v", err)
	}

	// Publication registers the track object.
	if err := publisher.PublishTrack(track); err != nil {
		t.Fatalf("PublishTrack: %v", err)
	}
	if err := room.PublishTrack(track); err != nil {
		t.Fatalf("Room.PublishTrack: %v", err)
	}

	if len(room.Tracks()) != 1 {
		t.Errorf("Expected 1 registered track, got %d", len(room.Tracks()))
	}
	if room.GetTrack("audio-1") != track {
		t.Error("Expected GetTrack to return the registered track instance")
	}

	// Duplicate registration is rejected...
	duplicate := newValidTrack(t, "audio-1", TrackKindVideo, TrackSourceCamera)
	if err := room.PublishTrack(duplicate); !errors.Is(err, ErrTrackAlreadyPublished) {
		t.Errorf("Expected ErrTrackAlreadyPublished, got %v", err)
	}

	// ...and removal works.
	if err := room.UnpublishTrack("audio-1"); err != nil {
		t.Fatalf("UnpublishTrack: %v", err)
	}
	if got := room.GetTrack("audio-1"); got != nil {
		t.Error("Expected nil after unpublishing from registry")
	}

	// Closed rooms reject registry operations.
	_ = room.Close()
	closed := newValidTrack(t, "closed-track", TrackKindAudio, TrackSourceMicrophone)
	if err := room.PublishTrack(closed); !errors.Is(err, ErrRoomClosed) {
		t.Errorf("Expected ErrRoomClosed on PublishTrack, got %v", err)
	}
	if err := room.UnpublishTrack("closed-track"); !errors.Is(err, ErrRoomClosed) {
		t.Errorf("Expected ErrRoomClosed on UnpublishTrack, got %v", err)
	}
}

// --- Subscription resolution through the room ---

func TestRoomSubscribeResolvesMissingRegistryEntry(t *testing.T) {
	room := NewRoom("resolve-room")
	_ = room.Create()

	publisher := NewParticipant("publisher", "Alice")
	subscriber := NewParticipant("subscriber", "Bob")
	_ = room.Join(publisher)
	_ = room.Join(subscriber)

	track := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)
	if err := publisher.PublishTrack(track); err != nil {
		t.Fatalf("PublishTrack: %v", err)
	}
	// Deliberately no room.PublishTrack: resolution must scan participants.

	if err := room.SubscribeToTrack(subscriber, "audio-1"); err != nil {
		t.Fatalf("SubscribeToTrack with missing registry entry: %v", err)
	}
	if len(subscriber.SubscribedTracks()) != 1 {
		t.Errorf("Expected 1 subscribed track, got %d", len(subscriber.SubscribedTracks()))
	}
	if !track.HasSubscribers() {
		t.Error("Expected subscriber to be attached to the track")
	}

	// Unknown IDs still fail with the domain sentinel.
	if err := room.SubscribeToTrack(subscriber, "missing"); !errors.Is(err, ErrTrackNotFound) {
		t.Errorf("Expected ErrTrackNotFound, got %v", err)
	}

	// UnsubscribeFromTrack resolves as well and detaches both sides.
	if err := room.UnsubscribeFromTrack(subscriber, "audio-1"); err != nil {
		t.Fatalf("UnsubscribeFromTrack with missing registry entry: %v", err)
	}
	if len(subscriber.SubscribedTracks()) != 0 || track.HasSubscribers() {
		t.Error("Expected subscription to be fully detached")
	}

	if err := room.UnsubscribeFromTrack(subscriber, "missing"); !errors.Is(err, ErrTrackNotFound) {
		t.Errorf("Expected ErrTrackNotFound on unknown unsubscribe, got %v", err)
	}

	// Closed rooms reject both operations.
	_ = room.Close()
	if err := room.SubscribeToTrack(subscriber, "audio-1"); !errors.Is(err, ErrRoomClosed) {
		t.Errorf("Expected ErrRoomClosed on SubscribeToTrack, got %v", err)
	}
	if err := room.UnsubscribeFromTrack(subscriber, "audio-1"); !errors.Is(err, ErrRoomClosed) {
		t.Errorf("Expected ErrRoomClosed on UnsubscribeFromTrack, got %v", err)
	}
}

// --- Orphan cleanup on publisher leave ---

func TestRoomOrphanCleanupOnPublisherLeave(t *testing.T) {
	room := NewRoom("orphan-room")
	_ = room.Create()

	publisher := NewParticipant("publisher", "Alice")
	subscriberA := NewParticipant("subscriber-a", "Bob")
	subscriberB := NewParticipant("subscriber-b", "Carol")
	_ = room.Join(publisher)
	_ = room.Join(subscriberA)
	_ = room.Join(subscriberB)

	track := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)
	_ = publisher.PublishTrack(track)
	_ = room.PublishTrack(track)
	_ = room.SubscribeToTrack(subscriberA, "audio-1")
	_ = room.SubscribeToTrack(subscriberB, "audio-1")

	// Publisher leaves: the track is destroyed and everyone is detached.
	if err := room.Leave("publisher"); err != nil {
		t.Fatalf("Leave(publisher): %v", err)
	}

	if len(room.Tracks()) != 0 {
		t.Errorf("Expected empty registry after publisher left, got %d", len(room.Tracks()))
	}
	if state := track.State(); state != TrackStateUnpublished {
		t.Errorf("Expected track unpublished after publisher left, got %s", state)
	}
	if track.Publisher() != nil {
		t.Error("Expected publisher reference cleared after publisher left")
	}
	for _, p := range []*Participant{subscriberA, subscriberB} {
		if len(p.SubscribedTracks()) != 0 {
			t.Errorf("Expected %s to be detached, still subscribed to %v", p.ID(), p.SubscribedTracks())
		}
	}
	if track.HasSubscribers() {
		t.Errorf("Expected track subscribers to be emptied, got %v", track.Subscribers())
	}
}

// --- Last subscriber leaving keeps the track while the publisher stays ---

func TestRoomLastSubscriberLeaveKeepsPublishedTrack(t *testing.T) {
	room := NewRoom("keep-track-room")
	_ = room.Create()

	publisher := NewParticipant("publisher", "Alice")
	subscriberA := NewParticipant("subscriber-a", "Bob")
	subscriberB := NewParticipant("subscriber-b", "Carol")
	_ = room.Join(publisher)
	_ = room.Join(subscriberA)
	_ = room.Join(subscriberB)

	track := newValidTrack(t, "audio-1", TrackKindAudio, TrackSourceMicrophone)
	_ = publisher.PublishTrack(track)
	_ = room.PublishTrack(track)
	_ = room.SubscribeToTrack(subscriberA, "audio-1")
	_ = room.SubscribeToTrack(subscriberB, "audio-1")

	// First subscriber leaves; the track survives untouched.
	if err := room.Leave("subscriber-a"); err != nil {
		t.Fatalf("Leave(subscriber-a): %v", err)
	}
	if got := room.GetTrack("audio-1"); got != track {
		t.Fatal("Expected track to remain in registry while publisher present")
	}
	if state := track.State(); state != TrackStatePublished {
		t.Errorf("Expected track still published, got %s", state)
	}
	if remaining := track.Subscribers(); len(remaining) != 1 || remaining[0] != "subscriber-b" {
		t.Errorf("Expected only subscriber-b attached, got %v", remaining)
	}

	// The LAST subscriber leaves; the track survives because its publisher
	// is still in the room.
	if err := room.Leave("subscriber-b"); err != nil {
		t.Fatalf("Leave(subscriber-b): %v", err)
	}
	if got := room.GetTrack("audio-1"); got != track {
		t.Fatal("Expected published track to stay registered even with zero subscribers")
	}
	if track.HasSubscribers() {
		t.Errorf("Expected no subscribers left, got %v", track.Subscribers())
	}
	if track.Publisher() != publisher {
		t.Error("Expected publisher to remain set")
	}
}

// --- Close detaches everyone ---

func TestRoomCloseDetachesEveryone(t *testing.T) {
	room := NewRoom("close-room")
	_ = room.Create()

	publisher := NewParticipant("publisher", "Alice")
	subscriber := NewParticipant("subscriber", "Bob")
	_ = room.Join(publisher)
	_ = room.Join(subscriber)

	publishedTrack := newValidTrack(t, "publisher-audio", TrackKindAudio, TrackSourceMicrophone)
	foreignTrack := newValidTrack(t, "foreign-audio", TrackKindVideo, TrackSourceCamera)

	_ = publisher.PublishTrack(publishedTrack)
	_ = room.PublishTrack(publishedTrack)
	_ = room.SubscribeToTrack(subscriber, "publisher-audio")

	// Subscriber also subscribes to a track that never touches this room's
	// registry (simulating an out-of-band attachment).
	_ = publisher.PublishTrack(foreignTrack)
	_ = subscriber.SubscribeTrack(foreignTrack)

	if err := room.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Everyone transitioned to Left and lost their bookkeeping.
	for _, p := range []*Participant{publisher, subscriber} {
		if state := p.State(); state != ParticipantStateLeft {
			t.Errorf("Expected %s to be left after close, got %s", p.ID(), state)
		}
		if len(p.PublishedTracks()) != 0 {
			t.Errorf("Expected %s publications cleared, got %v", p.ID(), p.PublishedTracks())
		}
		if len(p.SubscribedTracks()) != 0 {
			t.Errorf("Expected %s subscriptions cleared, got %v", p.ID(), p.SubscribedTracks())
		}
		if p.Room() != nil {
			t.Errorf("Expected %s room association cleared", p.ID())
		}
	}

	// Tracks are detached and unowned.
	for _, track := range []*Track{publishedTrack, foreignTrack} {
		if track.HasSubscribers() {
			t.Errorf("Expected no subscribers on %s after close", track.ID())
		}
		if track.State() != TrackStateUnpublished {
			t.Errorf("Expected %s unpublished after close, got %s", track.ID(), track.State())
		}
		if track.Publisher() != nil {
			t.Errorf("Expected publisher cleared on %s after close", track.ID())
		}
	}

	// Registry wiped.
	if len(room.Tracks()) != 0 || len(room.Participants()) != 0 {
		t.Errorf("Expected wiped maps, got tracks=%v participants=%v", room.Tracks(), room.Participants())
	}
}
