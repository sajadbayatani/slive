package domain

import (
	"errors"
	"testing"
)

// TestTrackAddSubscriberBeforePublish pins the gate that subscriptions can
// only attach to published tracks (TASK-013).
func TestTrackAddSubscriberBeforePublish(t *testing.T) {
	track := newValidTrack(t, "unpublished-track", TrackKindAudio, TrackSourceMicrophone)
	subscriber := NewParticipant("subscriber", "Bob")

	if err := track.AddSubscriber(subscriber); !errors.Is(err, ErrTrackNotPublished) {
		t.Errorf("Expected ErrTrackNotPublished before publication, got %v", err)
	}
	if track.SubscriberCount() != 0 {
		t.Errorf("Expected no subscribers after rejected add, got %d", track.SubscriberCount())
	}

	// After unpublishing, previously valid subscriptions are rejected again.
	track.Publish()
	if err := track.AddSubscriber(subscriber); err != nil {
		t.Fatalf("AddSubscriber after publish: %v", err)
	}
	track.Unpublish()
	other := NewParticipant("late-subscriber", "Carol")
	if err := track.AddSubscriber(other); !errors.Is(err, ErrTrackNotPublished) {
		t.Errorf("Expected ErrTrackNotPublished after unpublish, got %v", err)
	}
	if track.SubscriberCount() != 1 {
		t.Errorf("Existing subscriber must survive unpublish, got %d", track.SubscriberCount())
	}
}

// TestTrackAddSubscriberDuplicateIsNoOp verifies re-subscribing is accepted
// as nil and keeps exactly one entry keyed by the participant ID.
func TestTrackAddSubscriberDuplicateIsNoOp(t *testing.T) {
	track := newValidTrack(t, "dup-track", TrackKindAudio, TrackSourceMicrophone)
	publisher := NewParticipant("publisher", "Alice")
	subscriber := NewParticipant("subscriber", "Bob")

	_ = publisher.PublishTrack(track)

	if err := track.AddSubscriber(subscriber); err != nil {
		t.Fatalf("first AddSubscriber: %v", err)
	}
	if err := track.AddSubscriber(subscriber); err != nil {
		t.Fatalf("duplicate AddSubscriber must be a nil no-op, got %v", err)
	}
	if track.SubscriberCount() != 1 {
		t.Errorf("Expected single subscriber entry, got %d", track.SubscriberCount())
	}
	if got := track.Subscribers(); len(got) != 1 || got[0] != "subscriber" {
		t.Errorf("Subscribers = %v, want [subscriber]", got)
	}
	if track.GetSubscriber("subscriber") != subscriber {
		t.Error("Expected GetSubscriber to return the registered participant")
	}
	if track.GetSubscriber("missing") != nil {
		t.Error("Expected GetSubscriber to return nil for unknown IDs")
	}
}

// TestTrackRemoveSubscriberUnknownIsSafe verifies removal of unknown IDs is
// a silent no-op rather than an error or panic.
func TestTrackRemoveSubscriberUnknownIsSafe(t *testing.T) {
	track := newValidTrack(t, "remove-track", TrackKindAudio, TrackSourceMicrophone)

	// Removing from an empty subscriber set.
	track.RemoveSubscriber("ghost")

	subscriber := NewParticipant("subscriber", "Bob")
	_ = NewParticipant("publisher", "Alice").PublishTrack(track)
	if err := track.AddSubscriber(subscriber); err != nil {
		t.Fatalf("AddSubscriber: %v", err)
	}

	track.RemoveSubscriber("ghost")
	if track.SubscriberCount() != 1 {
		t.Errorf("Unknown-ID removal must not touch real subscribers, got %d", track.SubscriberCount())
	}

	track.RemoveSubscriber("subscriber")
	if track.HasSubscribers() {
		t.Errorf("Expected subscriber removed, got %v", track.Subscribers())
	}

	// Double removal stays safe.
	track.RemoveSubscriber("subscriber")
	if track.SubscriberCount() != 0 {
		t.Errorf("Expected empty subscriber set after double removal, got %d", track.SubscriberCount())
	}
}

// TestTrackSubscribersSnapshotIsACopy proves callers cannot mutate the
// internal subscriber bookkeeping through the returned slice.
func TestTrackSubscribersSnapshotIsACopy(t *testing.T) {
	track := newValidTrack(t, "snapshot-track", TrackKindAudio, TrackSourceMicrophone)
	_ = NewParticipant("publisher", "Alice").PublishTrack(track)

	for _, id := range []string{"sub-a", "sub-b"} {
		if err := track.AddSubscriber(NewParticipant(id, id)); err != nil {
			t.Fatalf("AddSubscriber(%s): %v", id, err)
		}
	}

	snapshot := track.Subscribers()
	for i := range snapshot {
		snapshot[i] = "tampered"
	}
	snapshot = append(snapshot, "injected")

	live := track.Subscribers()
	if len(live) != 2 {
		t.Fatalf("Internal subscriber set mutated through snapshot copy: %v", live)
	}
	want := map[string]bool{"sub-a": true, "sub-b": true}
	got := map[string]bool{}
	for _, id := range live {
		got[id] = true
	}
	for id := range want {
		if !got[id] {
			t.Errorf("Expected %s to remain subscribed, got %v", id, live)
		}
	}
	for _, id := range live {
		if !want[id] {
			t.Errorf("Unexpected subscriber %q after snapshot tampering: %v", id, live)
		}
	}

	// The same independence holds in reverse: removing a subscriber after a
	// snapshot must not shrink the already-taken snapshot.
	before := track.Subscribers()
	track.RemoveSubscriber("sub-a")
	if len(before) != 2 {
		t.Errorf("Earlier snapshot aliased with live state: %v", before)
	}
}
