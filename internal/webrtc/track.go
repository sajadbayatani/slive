package webrtc

import (
	"sync"

	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// WebRTCTrack wraps a domain.Track with WebRTC-specific functionality.
// It provides thread-safe access to the underlying WebRTC track and its metadata.
type WebRTCTrack struct {
	mu     sync.RWMutex
	domain *domain.Track
	track  interface{} // Can be TrackLocal or TrackRemote
	codec  webrtc.RTPCodecParameters
}

// NewWebRTCTrack creates a new WebRTCTrack from a domain.Track and a Pion WebRTC track.
func NewWebRTCTrack(domainTrack *domain.Track, pionTrack interface{}, codec webrtc.RTPCodecParameters) *WebRTCTrack {
	return &WebRTCTrack{
		domain: domainTrack,
		track:  pionTrack,
		codec:  codec,
	}
}

// DomainTrack returns the underlying domain.Track.
func (t *WebRTCTrack) DomainTrack() *domain.Track {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.domain
}

// PionTrack returns the underlying Pion WebRTC track.
func (t *WebRTCTrack) PionTrack() interface{} {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.track
}

// Codec returns the codec parameters for this track.
func (t *WebRTCTrack) Codec() webrtc.RTPCodecParameters {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.codec
}

// SetCodec updates the codec parameters for this track.
func (t *WebRTCTrack) SetCodec(codec webrtc.RTPCodecParameters) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.codec = codec
}

// ID returns the track's unique identifier.
func (t *WebRTCTrack) ID() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.domain.ID()
}

// Kind returns the type of the track (audio or video).
func (t *WebRTCTrack) Kind() domain.TrackKind {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.domain.Kind()
}

// Source returns the source of the track.
func (t *WebRTCTrack) Source() domain.TrackSource {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.domain.Source()
}

// State returns the current state of the track.
func (t *WebRTCTrack) State() domain.TrackState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.domain.State()
}

// Publish transitions the track to the published state.
func (t *WebRTCTrack) Publish() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.domain.Publish()
}

// Unpublish transitions the track to the unpublished state.
func (t *WebRTCTrack) Unpublish() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.domain.Unpublish()
}

// Publisher returns the participant who published this track.
func (t *WebRTCTrack) Publisher() *domain.Participant {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.domain.Publisher()
}

// SetPublisher sets the participant who published this track.
func (t *WebRTCTrack) SetPublisher(p *domain.Participant) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.domain.SetPublisher(p)
}

// Read reads RTP packets from the track.
//
// The underlying reader reference is copied while holding the lock and the
// lock is released before any blocking I/O: TrackRemote.Read blocks until the
// next packet arrives, and holding t.mu across that window would deadlock a
// concurrent Close (which needs the write lock).
func (t *WebRTCTrack) Read(b []byte) (int, error) {
	t.mu.RLock()
	remote, ok := t.track.(*webrtc.TrackRemote)
	t.mu.RUnlock()

	if !ok || remote == nil {
		return 0, ErrTrackNotReady
	}
	n, _, err := remote.Read(b)
	return n, err
}

// Close closes the underlying WebRTC track.
func (t *WebRTCTrack) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	// In Pion v3, tracks don't have a Close method directly
	// We just clear the reference
	t.track = nil
	return nil
}
