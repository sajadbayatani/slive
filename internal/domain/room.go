package domain

import (
	"sync"
)

// RoomState represents the current state of a room.
type RoomState int

const (
	RoomStateCreated RoomState = iota
	RoomStateActive
	RoomStateClosed
)

func (s RoomState) String() string {
	switch s {
	case RoomStateCreated:
		return "created"
	case RoomStateActive:
		return "active"
	case RoomStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Room represents an isolated real-time communication session.
type Room struct {
	mu           sync.RWMutex
	id           string
	state        RoomState
	participants map[string]*Participant
	tracks       map[string]*Track // All tracks published in this room
}

// NewRoom creates a new Room with the given ID.
func NewRoom(id string) *Room {
	return &Room{
		id:           id,
		state:        RoomStateCreated,
		participants: make(map[string]*Participant),
		tracks:       make(map[string]*Track),
	}
}

// ID returns the room's unique identifier.
func (r *Room) ID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.id
}

// State returns the current state of the room.
func (r *Room) State() RoomState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// Participants returns a copy of the participant IDs in the room.
func (r *Room) Participants() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.participants))
	for id := range r.participants {
		ids = append(ids, id)
	}
	return ids
}

// Create initializes the room and transitions it to the active state.
// This method is idempotent; calling it on an already active room has no effect.
func (r *Room) Create() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == RoomStateClosed {
		return ErrRoomClosed
	}

	if r.state == RoomStateCreated {
		r.state = RoomStateActive
	}

	return nil
}

// Join adds a participant to the room.
// Returns an error if the room is closed or the participant ID is already in use.
func (r *Room) Join(participant *Participant) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == RoomStateClosed {
		return ErrRoomClosed
	}

	if _, exists := r.participants[participant.ID()]; exists {
		return ErrParticipantAlreadyExists
	}

	r.participants[participant.ID()] = participant
	return nil
}

// Leave removes a participant from the room.
// Returns an error if the room is closed or the participant is not found.
//
// Cleanup follows the canonical rule "loss of publisher destroys the track":
// every track the leaver published is unpublished, its publisher reference is
// cleared, it is removed from the room registry, and all remaining
// subscribers are detached from it. Tracks the leaver subscribed to keep
// living as long as their publisher is present; they are destroyed too if
// this departure orphans them.
func (r *Room) Leave(participantID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == RoomStateClosed {
		return ErrRoomClosed
	}

	participant, exists := r.participants[participantID]
	if !exists {
		return ErrParticipantNotFound
	}

	// Destroy tracks published by the leaver and detach their subscribers.
	for _, trackID := range participant.PublishedTracks() {
		// Fetch the track object first; all track mutations happen on the
		// object so a stale registry entry cannot be dereferenced.
		if track := participant.GetPublishedTrack(trackID); track != nil {
			r.destroyTrackLocked(track, participantID)
		}
	}

	// Registry hygiene: purge orphaned entries. These appear when
	// Participant.Leave already ran for the leaver (it unpublishes and clears
	// publisher links, so the attribution loop above cannot see them) and
	// cover any other inconsistent registry state. The registry must only
	// ever hold live tracks.
	for _, track := range r.tracks {
		if track.State() == TrackStateUnpublished && track.Publisher() == nil {
			r.destroyTrackLocked(track, participantID)
		}
	}

	// Drop the leaver's own subscriptions; destroy tracks that are now both
	// unpublishable and unwatched.
	for _, trackID := range participant.SubscribedTracks() {
		_ = participant.UnsubscribeTrack(trackID)

		if track := r.tracks[trackID]; track != nil && !track.HasSubscribers() && track.Publisher() == nil {
			delete(r.tracks, trackID)
		}
	}

	delete(r.participants, participantID)
	return nil
}

// Close transitions the room to the closed state and removes all participants.
// This method is idempotent; calling it on an already closed room has no effect.
// Participants are detached via Participant.Leave before the maps are wiped,
// so publication and subscription bookkeeping stays consistent everywhere.
func (r *Room) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == RoomStateClosed {
		return nil
	}

	r.state = RoomStateClosed

	// Detach everyone first: Participant.Leave unpublishes own tracks,
	// clears subscription bookkeeping, and detaches from subscribed tracks.
	// It never fans out across participants, so no ABBA lock hazard exists.
	for _, participant := range r.participants {
		participant.Leave()
	}

	r.participants = make(map[string]*Participant)
	r.tracks = make(map[string]*Track)
	return nil
}

// GetParticipant retrieves a participant by ID.
// Returns nil if the participant is not found.
func (r *Room) GetParticipant(participantID string) *Participant {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.participants[participantID]
}

// PublishTrack adds a track to the room's track registry.
// This is called when a participant publishes a track.
func (r *Room) PublishTrack(track *Track) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == RoomStateClosed {
		return ErrRoomClosed
	}

	if _, exists := r.tracks[track.ID()]; exists {
		return ErrTrackAlreadyPublished
	}

	r.tracks[track.ID()] = track
	return nil
}

// UnpublishTrack removes a track from the room's track registry.
// This is called when a participant unpublishes a track.
func (r *Room) UnpublishTrack(trackID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == RoomStateClosed {
		return ErrRoomClosed
	}

	if _, exists := r.tracks[trackID]; !exists {
		return ErrTrackNotFound
	}

	delete(r.tracks, trackID)
	return nil
}

// GetTrack retrieves a track by ID.
// Returns nil if the track is not found.
func (r *Room) GetTrack(trackID string) *Track {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.tracks[trackID]
}

// Tracks returns a copy of all track IDs in the room.
func (r *Room) Tracks() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.tracks))
	for id := range r.tracks {
		ids = append(ids, id)
	}
	return ids
}

// SubscribeToTrack allows a participant to subscribe to a track in the room.
// Tracks missing from the room registry are resolved by scanning the
// published tracks of the room's participants, so direct publication via
// Participant.PublishTrack remains subscribable.
func (r *Room) SubscribeToTrack(participant *Participant, trackID string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.state == RoomStateClosed {
		return ErrRoomClosed
	}

	track := r.tracks[trackID]
	if track == nil {
		track = r.resolvePublishedTrackLocked(trackID)
	}
	if track == nil {
		return ErrTrackNotFound
	}

	return participant.SubscribeTrack(track)
}

// UnsubscribeFromTrack allows a participant to unsubscribe from a track in
// the room. The removal itself is owned by Participant.UnsubscribeTrack
// (semantics unchanged post-TASK-015); this method only validates that the
// track belongs to the room, resolving through participants when the registry
// entry is absent.
func (r *Room) UnsubscribeFromTrack(participant *Participant, trackID string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.state == RoomStateClosed {
		return ErrRoomClosed
	}

	if r.tracks[trackID] == nil && r.resolvePublishedTrackLocked(trackID) == nil {
		return ErrTrackNotFound
	}

	return participant.UnsubscribeTrack(trackID)
}

// resolvePublishedTrackLocked scans the published tracks of all room
// participants looking for trackID. Callers must hold r.mu (read or write);
// locking follows the room > participant > track hierarchy.
func (r *Room) resolvePublishedTrackLocked(trackID string) *Track {
	for _, participant := range r.participants {
		if track := participant.GetPublishedTrack(trackID); track != nil {
			return track
		}
	}
	return nil
}

// destroyTrackLocked implements the canonical "loss of publisher destroys the
// track" teardown: unpublish, clear ownership, drop from the registry, and
// detach every remaining subscriber. Detachment removes the track from each
// subscriber's bookkeeping (via UnsubscribeTrack) and, as a safety net for
// subscribers that can no longer run it themselves (e.g. already Left),
// directly from the track's subscriber set.
//
// Callers must hold r.mu for writing; locking follows the
// room > participant > track hierarchy.
func (r *Room) destroyTrackLocked(track *Track, excludeParticipantID string) {
	track.Unpublish()
	track.SetPublisher(nil)
	delete(r.tracks, track.ID())

	for _, subscriberID := range track.Subscribers() {
		if subscriberID == excludeParticipantID {
			continue
		}
		if subscriber, ok := r.participants[subscriberID]; ok {
			_ = subscriber.UnsubscribeTrack(track.ID())
		}
		track.RemoveSubscriber(subscriberID)
	}
}
