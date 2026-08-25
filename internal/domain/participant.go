package domain

import (
	"sync"
)

// ParticipantState represents the current state of a participant.
type ParticipantState int

const (
	ParticipantStateJoined ParticipantState = iota
	ParticipantStateActive
	ParticipantStateLeft
)

func (s ParticipantState) String() string {
	switch s {
	case ParticipantStateJoined:
		return "joined"
	case ParticipantStateActive:
		return "active"
	case ParticipantStateLeft:
		return "left"
	default:
		return "unknown"
	}
}

// Participant represents a client connected to a room.
type Participant struct {
	mu        sync.RWMutex
	id        string
	name      string
	state     ParticipantState
	room      *Room
	pubTracks map[string]*Track // Tracks published by this participant
	subTracks map[string]*Track // Tracks subscribed to by this participant
}

// NewParticipant creates a new Participant with the given ID and name.
func NewParticipant(id, name string) *Participant {
	return &Participant{
		id:        id,
		name:      name,
		state:     ParticipantStateJoined,
		pubTracks: make(map[string]*Track),
		subTracks: make(map[string]*Track),
	}
}

// ID returns the participant's unique identifier.
func (p *Participant) ID() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.id
}

// Name returns the participant's display name.
func (p *Participant) Name() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.name
}

// State returns the current state of the participant.
func (p *Participant) State() ParticipantState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

// Room returns the room this participant belongs to.
func (p *Participant) Room() *Room {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.room
}

// SetRoom associates this participant with a room.
func (p *Participant) SetRoom(room *Room) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.room = room
}

// Activate transitions the participant to the active state.
func (p *Participant) Activate() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = ParticipantStateActive
}

// Leave transitions the participant to the left state and cleans up resources.
// It removes this participant from all track subscriptions and unpublishes all tracks.
func (p *Participant) Leave() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.state = ParticipantStateLeft
	participantID := p.id

	// Clean up published tracks - remove this participant as publisher
	for trackID, track := range p.pubTracks {
		track.RemoveSubscriber(participantID)
		delete(p.pubTracks, trackID)
	}

	// Clean up subscribed tracks - remove this participant from each track's subscribers
	for trackID, track := range p.subTracks {
		track.RemoveSubscriber(participantID)
		delete(p.subTracks, trackID)
	}

	p.pubTracks = make(map[string]*Track)
	p.subTracks = make(map[string]*Track)
	p.room = nil
}

// PublishTrack adds a track published by this participant.
func (p *Participant) PublishTrack(track *Track) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == ParticipantStateLeft {
		return ErrParticipantLeft
	}

	if _, exists := p.pubTracks[track.ID()]; exists {
		return ErrTrackAlreadyPublished
	}

	p.pubTracks[track.ID()] = track
	track.SetPublisher(p)
	track.Publish()
	return nil
}

// UnpublishTrack removes a track published by this participant.
func (p *Participant) UnpublishTrack(trackID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == ParticipantStateLeft {
		return ErrParticipantLeft
	}

	if _, exists := p.pubTracks[trackID]; !exists {
		return ErrTrackNotFound
	}

	if track, exists := p.pubTracks[trackID]; exists {
		track.Unpublish()
	}
	delete(p.pubTracks, trackID)
	return nil
}

// SubscribeTrack adds a track subscribed to by this participant.
// It also adds this participant as a subscriber to the track.
func (p *Participant) SubscribeTrack(track *Track) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == ParticipantStateLeft {
		return ErrParticipantLeft
	}

	if _, exists := p.subTracks[track.ID()]; exists {
		return ErrTrackAlreadySubscribed
	}

	// Add to participant's subscribed tracks
	p.subTracks[track.ID()] = track

	// Add this participant as a subscriber to the track.
	// p.mu is held, so the pre-read ID variant must be used; calling
	// track.AddSubscriber here would re-acquire p.mu and self-deadlock.
	if err := track.addSubscriber(p, p.id); err != nil {
		delete(p.subTracks, track.ID())
		return err
	}

	return nil
}

// UnsubscribeTrack removes a track subscribed to by this participant.
// It also removes this participant from the track's subscribers.
func (p *Participant) UnsubscribeTrack(trackID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == ParticipantStateLeft {
		return ErrParticipantLeft
	}

	track, exists := p.subTracks[trackID]
	if !exists {
		return ErrTrackNotFound
	}

	// p.mu is held: read the field directly, p.ID() would self-deadlock
	participantID := p.id

	// Remove from track's subscribers
	track.RemoveSubscriber(participantID)

	// Remove from participant's subscribed tracks
	delete(p.subTracks, trackID)
	return nil
}

// PublishedTracks returns a copy of the track IDs published by this participant.
func (p *Participant) PublishedTracks() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	ids := make([]string, 0, len(p.pubTracks))
	for id := range p.pubTracks {
		ids = append(ids, id)
	}
	return ids
}

// SubscribedTracks returns a copy of the track IDs subscribed to by this participant.
func (p *Participant) SubscribedTracks() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	ids := make([]string, 0, len(p.subTracks))
	for id := range p.subTracks {
		ids = append(ids, id)
	}
	return ids
}

// GetPublishedTrack retrieves a published track by ID.
func (p *Participant) GetPublishedTrack(trackID string) *Track {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pubTracks[trackID]
}

// GetSubscribedTrack retrieves a subscribed track by ID.
func (p *Participant) GetSubscribedTrack(trackID string) *Track {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.subTracks[trackID]
}
