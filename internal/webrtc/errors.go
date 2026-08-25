package webrtc

import "errors"

// Common errors for the webrtc package.
var (
	// ErrTrackNotReady is returned when a track is not ready for operations.
	ErrTrackNotReady = errors.New("track not ready")

	// ErrTrackNotFound is returned when a track with the given ID is not found.
	ErrTrackNotFound = errors.New("track not found")

	// ErrPeerConnectionClosed is returned when operations are attempted on a closed peer connection.
	ErrPeerConnectionClosed = errors.New("peer connection closed")

	// ErrInvalidSDP is returned when an SDP string is invalid.
	ErrInvalidSDP = errors.New("invalid SDP")

	// ErrInvalidICECandidate is returned when an ICE candidate string is invalid.
	ErrInvalidICECandidate = errors.New("invalid ICE candidate")

	// ErrNoPeerConnection is returned when no peer connection is available.
	ErrNoPeerConnection = errors.New("no peer connection")

	// ErrSignalingError is returned when there is an error during signaling.
	ErrSignalingError = errors.New("signaling error")

	// ErrNegotiationFailed is returned when SDP negotiation fails.
	ErrNegotiationFailed = errors.New("negotiation failed")
)
