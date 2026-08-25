package domain

import "errors"

// Room-related errors.
var (
	ErrRoomClosed               = errors.New("room is closed")
	ErrParticipantAlreadyExists = errors.New("participant already exists in the room")
	ErrParticipantNotFound      = errors.New("participant not found in the room")
)

// Participant-related errors.
var (
	ErrParticipantLeft = errors.New("participant has left the room")
)

// Track-related errors.
var (
	ErrTrackAlreadyPublished  = errors.New("track already published by participant")
	ErrTrackAlreadySubscribed = errors.New("track already subscribed by participant")
	ErrTrackNotFound          = errors.New("track not found")
	ErrInvalidTrackKind       = errors.New("invalid track kind")
	ErrInvalidTrackSource     = errors.New("invalid track source")
	ErrTrackNotPublished      = errors.New("track not published")
)
