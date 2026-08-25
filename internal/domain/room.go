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
// Also cleans up tracks published by the participant and removes orphaned tracks.
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

	// Clean up tracks published by this participant
	for _, track := range participant.PublishedTracks() {
		delete(r.tracks, track)
	}

	// Clean up subscriptions from this participant
	for _, track := range participant.SubscribedTracks() {
		if t := r.tracks[track]; t != nil {
			t.RemoveSubscriber(participantID)
			// If track has no more subscribers and no publisher, remove it
			if !t.HasSubscribers() && t.Publisher() == nil {
				delete(r.tracks, track)
			}
		}
	}

	delete(r.participants, participantID)
	return nil
}

// Close transitions the room to the closed state and removes all participants.
// This method is idempotent; calling it on an already closed room has no effect.
// Also cleans up all tracks in the room.
func (r *Room) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == RoomStateClosed {
		return nil
	}

	r.state = RoomStateClosed
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
func (r *Room) SubscribeToTrack(participant *Participant, trackID string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.state == RoomStateClosed {
		return ErrRoomClosed
	}

	track := r.tracks[trackID]
	if track == nil {
		return ErrTrackNotFound
	}

	return participant.SubscribeTrack(track)
}

// UnsubscribeFromTrack allows a participant to unsubscribe from a track in the room.
func (r *Room) UnsubscribeFromTrack(participant *Participant, trackID string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.state == RoomStateClosed {
		return ErrRoomClosed
	}

	return participant.UnsubscribeTrack(trackID)
}
