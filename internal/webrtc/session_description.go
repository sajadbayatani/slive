package webrtc

import (
	"encoding/json"
	"sync"

	"github.com/pion/webrtc/v3"
)

// SessionDescription represents a WebRTC session description (SDP offer/answer).
// It provides thread-safe access to the underlying Pion WebRTC session description.
type SessionDescription struct {
	mu     sync.RWMutex
	sdp    *webrtc.SessionDescription
	type_  webrtc.SDPType
}

// NewSessionDescription creates a new SessionDescription from a Pion WebRTC session description.
func NewSessionDescription(sdp *webrtc.SessionDescription) *SessionDescription {
	return &SessionDescription{
		sdp:   sdp,
		type_: sdp.Type,
	}
}

// NewSessionDescriptionFromString creates a new SessionDescription from an SDP string and type.
func NewSessionDescriptionFromString(sdp string, sdpType webrtc.SDPType) (*SessionDescription, error) {
	return &SessionDescription{
		sdp: &webrtc.SessionDescription{
			Type:  sdpType,
			SDP:   sdp,
		},
		type_: sdpType,
	}, nil
}

// Type returns the SDP type (offer, answer, etc.).
func (s *SessionDescription) Type() webrtc.SDPType {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.type_
}

// SDP returns the SDP string.
func (s *SessionDescription) SDP() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sdp.SDP
}

// PionSessionDescription returns the underlying Pion WebRTC session description.
func (s *SessionDescription) PionSessionDescription() *webrtc.SessionDescription {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sdp
}

// SetSDP updates the SDP string.
func (s *SessionDescription) SetSDP(sdp string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sdp.SDP = sdp
}

// SetType updates the SDP type.
func (s *SessionDescription) SetType(sdpType webrtc.SDPType) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.type_ = sdpType
	s.sdp.Type = sdpType
}

// MarshalJSON implements json.Marshaler for SessionDescription.
func (s *SessionDescription) MarshalJSON() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(struct {
		Type string `json:"type"`
		SDP  string `json:"sdp"`
	}{
		Type: s.type_.String(),
		SDP:  s.sdp.SDP,
	})
}

// UnmarshalJSON implements json.Unmarshaler for SessionDescription.
func (s *SessionDescription) UnmarshalJSON(data []byte) error {
	var raw struct {
		Type string `json:"type"`
		SDP  string `json:"sdp"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.type_ = webrtc.NewSDPType(raw.Type)
	s.sdp = &webrtc.SessionDescription{
		Type: s.type_,
		SDP:  raw.SDP,
	}
	return nil
}

// String returns a string representation of the SessionDescription.
func (s *SessionDescription) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.type_.String() + "\n" + s.sdp.SDP
}
