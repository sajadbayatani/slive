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

	// ErrICEFailed is returned when adding a remote ICE candidate kept
	// failing until retries were exhausted; it wraps the last underlying
	// error so callers can inspect the root cause with errors.Is.
	ErrICEFailed = errors.New("ice candidate failed")

	// ErrNoPendingOffer is returned when a browser answer arrives but the
	// PC holds no outstanding server offer (not in have-local-offer).
	// Callers treat duplicate retransmits as idempotent and anything else
	// as a mis-targeted answer. It never wraps a pion
	// InvalidModificationError: the pion state is pre-checked, so reaching
	// pion with an incompatible state is impossible by construction.
	ErrNoPendingOffer = errors.New("answer with no outstanding server offer")

	// ErrUnexpectedOffer is returned when a browser offer arrives while a
	// browser negotiation is already in progress on the PC (e.g. a second
	// outstanding publisher offer). Each negotiation cycle has exactly one
	// offerer; a concurrent second browser offer is a client protocol
	// violation, reported without touching pion state.
	ErrUnexpectedOffer = errors.New("unexpected browser offer while negotiation in progress")
)
