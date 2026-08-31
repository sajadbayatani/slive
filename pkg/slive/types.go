package slive

import (
	"github.com/sajadbayatani/slive/internal/domain"
	"github.com/sajadbayatani/slive/internal/signaling"
	"github.com/sajadbayatani/slive/internal/webrtc"
)

// Domain lifecycle types. These are type aliases so their method sets are
// identical to the internal implementation, but pkg/slive is the stable
// import path. Private fields (sync.RWMutex, maps) remain unexported.

// Room is an isolated real-time communication session.
//
// The room owns participants and the track registry. The zero value is not
// usable; create rooms via signaling.RoomManager or Client.JoinRoom.
type Room = domain.Room

// Participant is a client connected to a room.
type Participant = domain.Participant

// Track is an audio or video media stream owned/published by a participant.
type Track = domain.Track

// TrackKind is the kind of a Track (audio or video).
type TrackKind = domain.TrackKind

const (
	// TrackKindAudio is an audio track.
	TrackKindAudio = domain.TrackKindAudio
	// TrackKindVideo is a video track.
	TrackKindVideo = domain.TrackKindVideo
)

// TrackSource is the source of a Track (microphone, camera, screen_share).
type TrackSource = domain.TrackSource

const (
	// TrackSourceMicrophone is a microphone audio source.
	TrackSourceMicrophone = domain.TrackSourceMicrophone
	// TrackSourceCamera is a camera video source.
	TrackSourceCamera = domain.TrackSourceCamera
	// TrackSourceScreenShare is a screen share source.
	TrackSourceScreenShare = domain.TrackSourceScreenShare
)

// RoomState is the lifecycle state of a Room.
type RoomState = domain.RoomState

const (
	// RoomStateCreated is the initial state before Create.
	RoomStateCreated = domain.RoomStateCreated
	// RoomStateActive is the active state after Create/Join.
	RoomStateActive = domain.RoomStateActive
	// RoomStateClosed is the terminal closed state.
	RoomStateClosed = domain.RoomStateClosed
)

// ParticipantState is the lifecycle state of a Participant.
type ParticipantState = domain.ParticipantState

const (
	// ParticipantStateJoined is the state after NewParticipant/Join.
	ParticipantStateJoined = domain.ParticipantStateJoined
	// ParticipantStateActive is the state after Activate.
	ParticipantStateActive = domain.ParticipantStateActive
	// ParticipantStateLeft is the terminal state after Leave.
	ParticipantStateLeft = domain.ParticipantStateLeft
)

// TrackState is the lifecycle state of a Track.
type TrackState = domain.TrackState

const (
	// TrackStateCreated is the initial state.
	TrackStateCreated = domain.TrackStateCreated
	// TrackStatePublished is the published state.
	TrackStatePublished = domain.TrackStatePublished
	// TrackStateUnpublished is the unpublished state.
	TrackStateUnpublished = domain.TrackStateUnpublished
)

// Signaling and SFU types.

// RoomManager manages the lifecycle of rooms.
type RoomManager = signaling.RoomManager

// Handler handles signaling connections and SFU forwarding.
type Handler = signaling.Handler

// HandlerOption customises a Handler at construction time.
type HandlerOption = signaling.HandlerOption

// ForwarderConfig holds tunable parameters for TrackForwarder.
type ForwarderConfig = webrtc.ForwarderConfig

// DefaultQueueSize is the default per-subscriber RTP queue capacity.
const DefaultQueueSize = webrtc.DefaultQueueSize

// MetricsSnapshot is a point-in-time copy of all observable counters and gauges.
// It is safe to encode without holding any handler locks.
type MetricsSnapshot = webrtc.MetricsSnapshot

// PeerConnectionConfig holds configuration for creating peer connections.
type PeerConnectionConfig = webrtc.PeerConnectionConfig
