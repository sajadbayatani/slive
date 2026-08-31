package slive

import (
	"errors"

	"github.com/sajadbayatani/slive/internal/domain"
	"github.com/sajadbayatani/slive/internal/webrtc"
)

// Stable error sentinels. Except for the two room sentinels below, they are var
// aliases for domain/webrtc errors so errors.Is(err, slive.ErrRoomClosed)
// remains true. These sentinels are frozen; new sentinels will be added as
// var ErrNew = errors.New(...) with errors.Is support in a MINOR, and removal
// requires a MAJOR bump.
//
// ErrRoomNotFound and ErrRoomAlreadyExists are the two package-owned
// exceptions: they are declared with errors.New in pkg/slive instead of
// aliasing an internal participant sentinel, so a missing room never matches
// ErrParticipantNotFound (DEF-01). Every room miss on the stable surface is
// raised inside pkg/slive, so no internal change is needed. Other room-level
// paths that still report the participant errors — signaling.RoomManager
// CreateRoom/CloseRoom — keep their internal identity.

// Room errors.
var (
	// ErrRoomClosed is returned when an operation targets a closed room.
	ErrRoomClosed = domain.ErrRoomClosed
	// ErrRoomAlreadyExists is returned when creating a room that already exists.
	// It is owned by pkg/slive and has its own identity: it does not match any
	// participant sentinel.
	ErrRoomAlreadyExists = errors.New("room already exists")
	// ErrRoomNotFound is returned when a room cannot be found. It is owned by
	// pkg/slive and has its own identity: a missing room does not match
	// ErrParticipantNotFound.
	ErrRoomNotFound = errors.New("room not found")
)

// Participant errors.
var (
	// ErrParticipantAlreadyExists is returned when a participant ID already exists in a room.
	ErrParticipantAlreadyExists = domain.ErrParticipantAlreadyExists
	// ErrParticipantNotFound is returned when a participant cannot be found.
	ErrParticipantNotFound = domain.ErrParticipantNotFound
	// ErrParticipantLeft is returned when an operation targets a left participant.
	ErrParticipantLeft = domain.ErrParticipantLeft
)

// Track errors.
var (
	// ErrTrackAlreadyPublished is returned when a track is already published by a participant.
	ErrTrackAlreadyPublished = domain.ErrTrackAlreadyPublished
	// ErrTrackAlreadySubscribed is returned when a track is already subscribed.
	ErrTrackAlreadySubscribed = domain.ErrTrackAlreadySubscribed
	// ErrTrackNotFound is returned when a track cannot be found.
	ErrTrackNotFound = domain.ErrTrackNotFound
	// ErrInvalidTrackKind is returned when a track kind is invalid.
	ErrInvalidTrackKind = domain.ErrInvalidTrackKind
	// ErrInvalidTrackSource is returned when a track source is invalid.
	ErrInvalidTrackSource = domain.ErrInvalidTrackSource
	// ErrTrackNotPublished is returned when a track is not in published state.
	ErrTrackNotPublished = domain.ErrTrackNotPublished
)

// WebRTC / signaling errors.
var (
	// ErrTrackNotReady is returned when a track is not ready for operations.
	ErrTrackNotReady = webrtc.ErrTrackNotReady
	// ErrPeerConnectionClosed is returned when operations target a closed peer connection.
	ErrPeerConnectionClosed = webrtc.ErrPeerConnectionClosed
	// ErrNoPeerConnection is returned when no peer connection is available.
	ErrNoPeerConnection = webrtc.ErrNoPeerConnection
	// ErrInvalidSDP is returned when an SDP string is invalid.
	ErrInvalidSDP = webrtc.ErrInvalidSDP
	// ErrInvalidICECandidate is returned when an ICE candidate string is invalid.
	ErrInvalidICECandidate = webrtc.ErrInvalidICECandidate
)
