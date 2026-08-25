package webrtc

import (
	"encoding/json"
	"sync"

	"github.com/pion/webrtc/v3"
)

// ICECandidate represents a WebRTC ICE candidate.
// It provides thread-safe access to the underlying Pion WebRTC ICE candidate.
type ICECandidate struct {
	mu        sync.RWMutex
	candidate webrtc.ICECandidateInit
}

// NewICECandidate creates a new ICECandidate from a Pion WebRTC ICE candidate.
func NewICECandidate(candidate *webrtc.ICECandidate) *ICECandidate {
	init := candidate.ToJSON()
	return &ICECandidate{
		candidate: webrtc.ICECandidateInit{
			Candidate:     init.Candidate,
			SDPMid:        init.SDPMid,
			SDPMLineIndex: init.SDPMLineIndex,
		},
	}
}

// NewICECandidateFromString creates a new ICECandidate from a candidate string.
func NewICECandidateFromString(candidate string) (*ICECandidate, error) {
	return &ICECandidate{
		candidate: webrtc.ICECandidateInit{
			Candidate: candidate,
		},
	}, nil
}

// Candidate returns the candidate string.
func (c *ICECandidate) Candidate() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.candidate.Candidate
}

// SDPMid returns the SDP mid attribute.
func (c *ICECandidate) SDPMid() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.candidate.SDPMid != nil {
		return *c.candidate.SDPMid
	}
	return ""
}

// SDPMLineIndex returns the SDP m-line index.
func (c *ICECandidate) SDPMLineIndex() uint16 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.candidate.SDPMLineIndex != nil {
		return *c.candidate.SDPMLineIndex
	}
	return 0
}

// PionICECandidateInit returns the underlying Pion WebRTC ICE candidate init.
func (c *ICECandidate) PionICECandidateInit() webrtc.ICECandidateInit {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.candidate
}

// SetCandidate updates the candidate string.
func (c *ICECandidate) SetCandidate(candidate string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.candidate.Candidate = candidate
	return nil
}

// SetSDPMid updates the SDP mid attribute.
func (c *ICECandidate) SetSDPMid(sdpMid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.candidate.SDPMid = &sdpMid
}

// SetSDPMLineIndex updates the SDP m-line index.
func (c *ICECandidate) SetSDPMLineIndex(index uint16) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.candidate.SDPMLineIndex = &index
}

// MarshalJSON implements json.Marshaler for ICECandidate.
func (c *ICECandidate) MarshalJSON() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return json.Marshal(c.candidate)
}

// UnmarshalJSON implements json.Unmarshaler for ICECandidate.
func (c *ICECandidate) UnmarshalJSON(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return json.Unmarshal(data, &c.candidate)
}

// String returns a string representation of the ICECandidate.
func (c *ICECandidate) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.candidate.Candidate
}
