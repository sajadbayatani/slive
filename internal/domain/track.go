package domain

import (
	"sync"
)

// TrackKind represents the type of media track.
type TrackKind int

const (
	TrackKindAudio TrackKind = iota
	TrackKindVideo
)

func (k TrackKind) String() string {
	switch k {
	case TrackKindAudio:
		return "audio"
	case TrackKindVideo:
		return "video"
	default:
		return "unknown"
	}
}

// TrackSource represents the source of the media track.
type TrackSource int

const (
	TrackSourceMicrophone TrackSource = iota
	TrackSourceCamera
	TrackSourceScreenShare
)

func (s TrackSource) String() string {
	switch s {
	case TrackSourceMicrophone:
		return "microphone"
	case TrackSourceCamera:
		return "camera"
	case TrackSourceScreenShare:
		return "screen_share"
	default:
		return "unknown"
	}
}

// TrackState represents the current state of a track.
type TrackState int

const (
	TrackStateCreated TrackState = iota
	TrackStatePublished
	TrackStateUnpublished
)

func (s TrackState) String() string {
	switch s {
	case TrackStateCreated:
		return "created"
	case TrackStatePublished:
		return "published"
	case TrackStateUnpublished:
		return "unpublished"
	default:
		return "unknown"
	}
}

// Track represents an audio or video media stream owned/published by a participant.
//
// Track is a pure domain object: it carries identity, kind, source, state,
// publisher, and subscriber bookkeeping only. Media transport binding lives
// in the webrtc package (webrtc.WebRTCTrack), which composes a domain.Track
// with a pion track object; this type deliberately keeps no transport
// reference so the domain stays independent of any WebRTC implementation.
type Track struct {
	mu          sync.RWMutex
	id          string
	kind        TrackKind
	source      TrackSource
	state       TrackState
	publisher   *Participant
	subscribers map[string]*Participant // Participants subscribed to this track
}

// NewTrack creates a new Track with the given ID, kind, and source.
// Returns an error if kind or source are invalid.
func NewTrack(id string, kind TrackKind, source TrackSource) (*Track, error) {
	if !isValidTrackKind(kind) {
		return nil, ErrInvalidTrackKind
	}
	if !isValidTrackSource(source) {
		return nil, ErrInvalidTrackSource
	}
	return &Track{
		id:          id,
		kind:        kind,
		source:      source,
		state:       TrackStateCreated,
		subscribers: make(map[string]*Participant),
	}, nil
}

// NewTrackUnvalidated creates a new Track without validation.
// This is useful for internal operations where validation has already been performed.
func NewTrackUnvalidated(id string, kind TrackKind, source TrackSource) *Track {
	return &Track{
		id:          id,
		kind:        kind,
		source:      source,
		state:       TrackStateCreated,
		subscribers: make(map[string]*Participant),
	}
}

// isValidTrackKind checks if the track kind is valid.
func isValidTrackKind(kind TrackKind) bool {
	return kind == TrackKindAudio || kind == TrackKindVideo
}

// isValidTrackSource checks if the track source is valid.
func isValidTrackSource(source TrackSource) bool {
	return source == TrackSourceMicrophone || source == TrackSourceCamera || source == TrackSourceScreenShare
}

// ID returns the track's unique identifier.
func (t *Track) ID() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.id
}

// Kind returns the type of the track (audio or video).
func (t *Track) Kind() TrackKind {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.kind
}

// Source returns the source of the track.
func (t *Track) Source() TrackSource {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.source
}

// State returns the current state of the track.
func (t *Track) State() TrackState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

// Publisher returns the participant who published this track.
func (t *Track) Publisher() *Participant {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.publisher
}

// SetPublisher sets the participant who published this track.
func (t *Track) SetPublisher(p *Participant) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.publisher = p
}

// Publish transitions the track to the published state.
func (t *Track) Publish() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = TrackStatePublished
}

// Unpublish transitions the track to the unpublished state.
func (t *Track) Unpublish() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = TrackStateUnpublished
}

// AddSubscriber adds a participant to the list of subscribers for this track.
func (t *Track) AddSubscriber(p *Participant) error {
	// Read the ID before locking: no code path may acquire p.mu while
	// holding t.mu (lock hierarchy room > participant > track).
	return t.addSubscriber(p, p.ID())
}

// addSubscriber is AddSubscriber for callers that already hold the participant
// lock and therefore must pass a pre-read participant ID.
func (t *Track) addSubscriber(p *Participant, participantID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != TrackStatePublished {
		return ErrTrackNotPublished
	}

	if _, exists := t.subscribers[participantID]; exists {
		return nil // Already subscribed
	}

	t.subscribers[participantID] = p
	return nil
}

// RemoveSubscriber removes a participant from the list of subscribers for this track.
func (t *Track) RemoveSubscriber(participantID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.subscribers, participantID)
}

// Subscribers returns a copy of the subscriber IDs for this track.
func (t *Track) Subscribers() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	ids := make([]string, 0, len(t.subscribers))
	for id := range t.subscribers {
		ids = append(ids, id)
	}
	return ids
}

// GetSubscriber returns a subscriber by ID.
func (t *Track) GetSubscriber(participantID string) *Participant {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.subscribers[participantID]
}

// HasSubscribers returns true if the track has any subscribers.
func (t *Track) HasSubscribers() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.subscribers) > 0
}

// SubscriberCount returns the number of subscribers to this track.
func (t *Track) SubscriberCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.subscribers)
}
