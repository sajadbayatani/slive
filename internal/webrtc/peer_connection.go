// Package webrtc implements SFU forwarding and peer connection management.
//
// Lock hierarchy (must be respected to avoid deadlocks):
//
//	PeerConnection.mu > TrackForwarder.mu > WebRTCTrack.mu
//	Handler.trackForwardersMutex > TrackForwarder.lifecycleMu > TrackForwarder.mu
//
// TrackForwarder must never call Handler while holding mu, and callbacks
// registered via OnLocalTrackAdded / OnTrack must never acquire
// TrackForwarder.mu while holding PeerConnection.mu.
package webrtc

import (
	"context"
	"log/slog"
	"sync"

	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// PeerConnectionState represents the current state of a peer connection.
type PeerConnectionState int

const (
	PeerConnectionStateNew PeerConnectionState = iota
	PeerConnectionStateConnecting
	PeerConnectionStateConnected
	PeerConnectionStateDisconnected
	PeerConnectionStateFailed
	PeerConnectionStateClosed
)

func (s PeerConnectionState) String() string {
	switch s {
	case PeerConnectionStateNew:
		return "new"
	case PeerConnectionStateConnecting:
		return "connecting"
	case PeerConnectionStateConnected:
		return "connected"
	case PeerConnectionStateDisconnected:
		return "disconnected"
	case PeerConnectionStateFailed:
		return "failed"
	case PeerConnectionStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Usable reports whether a connection in this state can still carry media.
// Only the terminal Closed state and the unrecoverable Failed state are
// unusable; Disconnected is deliberately usable because ICE may self-heal
// (NeedsReconnect describes that recoverable-drop case instead). The
// signaling layer replaces unusable connections on reconnect and reuses
// usable ones.
func (s PeerConnectionState) Usable() bool {
	return s != PeerConnectionStateClosed && s != PeerConnectionStateFailed
}

// PeerConnectionConfig holds configuration for creating a new PeerConnection.
type PeerConnectionConfig struct {
	// ICEServers is a list of ICE servers to use for NAT traversal.
	ICEServers []webrtc.ICEServer
	// SDPSemantics is the SDP semantics to use (e.g., "unified-plan").
	SDPSemantics webrtc.SDPSemantics
	// Logger receives structured lifecycle events for connections created
	// with this config. A nil Logger resolves to slog.Default() inside
	// NewPeerConnection, so the zero value stays usable.
	Logger *slog.Logger
}

// DefaultPeerConnectionConfig returns a default PeerConnectionConfig.
func DefaultPeerConnectionConfig() PeerConnectionConfig {
	return PeerConnectionConfig{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback,
	}
}

// SignalingSender is a function type for sending signaling messages.
// It takes a message type and data, and sends it via the signaling connection.
type SignalingSender func(msgType string, data interface{}) error

// PeerConnection manages a WebRTC peer connection.
// It provides thread-safe access to the underlying Pion WebRTC peer connection.
type PeerConnection struct {
	mu            sync.RWMutex
	pionPC        *webrtc.PeerConnection
	config        PeerConnectionConfig
	state         PeerConnectionState
	localTracks   map[string]*WebRTCTrack
	remoteTracks  map[string]*WebRTCTrack
	participant   *domain.Participant
	sendSignaling SignalingSender
	// logger is resolved once at construction (config.Logger or
	// slog.Default()) and never mutated afterwards, so it is read without
	// holding mu.
	logger *slog.Logger
	// Event callbacks registered via OnNegotiationNeeded / OnICECandidate /
	// OnTrack. They are stored on the struct and invoked from the pion event
	// handlers below; a nil callback simply means "not interested".
	onNegotiationNeeded func()
	onICECandidate      func(*ICECandidate)
	onTrack             func(*WebRTCTrack)
	onLocalTrackAdded   func(*WebRTCTrack)
	ctx                 context.Context
	cancel              context.CancelFunc
}

// NewPeerConnection creates a new PeerConnection with the given configuration.
func NewPeerConnection(config PeerConnectionConfig, participant *domain.Participant, sendSignaling SignalingSender) (*PeerConnection, error) {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	pionConfig := webrtc.Configuration{
		ICEServers:   config.ICEServers,
		SDPSemantics: config.SDPSemantics,
	}

	pionPC, err := webrtc.NewPeerConnection(pionConfig)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	pc := &PeerConnection{
		pionPC:        pionPC,
		config:        config,
		state:         PeerConnectionStateNew,
		localTracks:   make(map[string]*WebRTCTrack),
		remoteTracks:  make(map[string]*WebRTCTrack),
		participant:   participant,
		sendSignaling: sendSignaling,
		logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
	}

	// Set up event handlers
	pionPC.OnNegotiationNeeded(func() {
		pc.handleNegotiationNeeded()
	})

	pionPC.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate != nil {
			pc.handleICECandidate(candidate)
		}
	})

	pionPC.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		pc.handleTrack(track, receiver)
	})

	pionPC.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		pc.handleConnectionStateChange(state)
	})

	return pc, nil
}

// PionPeerConnection returns the underlying Pion WebRTC peer connection.
func (pc *PeerConnection) PionPeerConnection() *webrtc.PeerConnection {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.pionPC
}

// Participant returns the participant associated with this peer connection.
func (pc *PeerConnection) Participant() *domain.Participant {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.participant
}

// UpdateSignalingSender updates the signaling sender function.
func (pc *PeerConnection) UpdateSignalingSender(sendSignaling SignalingSender) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.sendSignaling = sendSignaling
}

// State returns the current state of the peer connection.
func (pc *PeerConnection) State() PeerConnectionState {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.state
}

// Config returns the configuration of the peer connection.
func (pc *PeerConnection) Config() PeerConnectionConfig {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.config
}

// AddTrack adds a local track to the peer connection.
//
// Lock hierarchy: PeerConnection.mu is held only to update localTracks and
// snapshot the OnLocalTrackAdded callback; the callback is invoked outside
// mu via a goroutine with panic recovery so user code never runs on pion's
// dispatch path and does not invert PeerConnection.mu > TrackForwarder.mu.
// Callbacks must not acquire TrackForwarder.mu while holding pc.mu.
func (pc *PeerConnection) AddTrack(track *WebRTCTrack) error {
	pc.mu.Lock()

	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		pc.mu.Unlock()
		return ErrPeerConnectionClosed
	}

	pionTrack := track.PionTrack()
	if pionTrack == nil {
		pc.mu.Unlock()
		return ErrTrackNotReady
	}

	// In Pion v3, AddTrack requires a TrackLocal interface
	// We need to check if the track is a TrackLocal
	trackLocal, ok := pionTrack.(webrtc.TrackLocal)
	if !ok {
		pc.mu.Unlock()
		return ErrTrackNotReady
	}

	sender, err := pc.pionPC.AddTrack(trackLocal)
	if err != nil {
		pc.mu.Unlock()
		return err
	}

	// Store the track and its sender
	pc.localTracks[track.ID()] = track
	callback := pc.onLocalTrackAdded
	ctx := pc.ctx
	logger := pc.logger
	pc.mu.Unlock()

	// Set up RTCP handling if needed (outside lock)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if _, _, err := sender.ReadRTCP(); err != nil {
					return
				}
			}
		}
	}()

	if callback != nil {
		go func(cb func(*WebRTCTrack), t *WebRTCTrack) {
			defer func() {
				if r := recover(); r != nil {
					logger.Warn("OnLocalTrackAdded callback panicked",
						"event", "on_local_track_added_panic",
						"error", r,
					)
				}
			}()
			cb(t)
		}(callback, track)
	}

	return nil
}

// RemoveTrack removes a local track from the peer connection.
func (pc *PeerConnection) RemoveTrack(trackID string) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		return ErrPeerConnectionClosed
	}

	_, exists := pc.localTracks[trackID]
	if !exists {
		return ErrTrackNotFound
	}

	senders := pc.pionPC.GetSenders()
	for _, sender := range senders {
		if sender.Track() != nil && sender.Track().ID() == trackID {
			if err := pc.pionPC.RemoveTrack(sender); err != nil {
				return err
			}
			break
		}
	}

	delete(pc.localTracks, trackID)
	return nil
}

// GetLocalTrack retrieves a local track by ID.
func (pc *PeerConnection) GetLocalTrack(trackID string) *WebRTCTrack {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.localTracks[trackID]
}

// GetRemoteTrack retrieves a remote track by ID.
func (pc *PeerConnection) GetRemoteTrack(trackID string) *WebRTCTrack {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.remoteTracks[trackID]
}

// CreateOffer creates an SDP offer.
func (pc *PeerConnection) CreateOffer() (*SessionDescription, error) {
	pc.mu.RLock()
	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		pc.mu.RUnlock()
		return nil, ErrPeerConnectionClosed
	}
	pionPC := pc.pionPC
	pc.mu.RUnlock()

	ConnectionMetrics.IncrementAttempts()

	gatherComplete := webrtc.GatheringCompletePromise(pionPC)
	offer, err := pionPC.CreateOffer(nil)
	if err != nil {
		ConnectionMetrics.IncrementFailures()
		return nil, err
	}

	if err := pionPC.SetLocalDescription(offer); err != nil {
		ConnectionMetrics.IncrementFailures()
		return nil, err
	}

	<-gatherComplete

	localDesc := pionPC.LocalDescription()
	if localDesc == nil {
		ConnectionMetrics.IncrementFailures()
		return nil, ErrInvalidSDP
	}

	return NewSessionDescription(localDesc), nil
}

// CreateAnswer creates an SDP answer for the given offer.
func (pc *PeerConnection) CreateAnswer(offer *SessionDescription) (*SessionDescription, error) {
	pc.mu.RLock()
	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		pc.mu.RUnlock()
		return nil, ErrPeerConnectionClosed
	}
	pionPC := pc.pionPC
	pc.mu.RUnlock()

	ConnectionMetrics.IncrementAttempts()

	if err := pionPC.SetRemoteDescription(*offer.PionSessionDescription()); err != nil {
		ConnectionMetrics.IncrementFailures()
		return nil, err
	}

	gatherComplete := webrtc.GatheringCompletePromise(pionPC)
	answer, err := pionPC.CreateAnswer(nil)
	if err != nil {
		ConnectionMetrics.IncrementFailures()
		return nil, err
	}

	if err := pionPC.SetLocalDescription(answer); err != nil {
		ConnectionMetrics.IncrementFailures()
		return nil, err
	}

	<-gatherComplete

	localDesc := pionPC.LocalDescription()
	if localDesc == nil {
		ConnectionMetrics.IncrementFailures()
		return nil, ErrInvalidSDP
	}

	return NewSessionDescription(localDesc), nil
}

// SetRemoteDescription sets the remote session description.
func (pc *PeerConnection) SetRemoteDescription(sdp *SessionDescription) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		return ErrPeerConnectionClosed
	}

	return pc.pionPC.SetRemoteDescription(*sdp.PionSessionDescription())
}

// AddICECandidate adds a remote ICE candidate.
func (pc *PeerConnection) AddICECandidate(candidate *ICECandidate) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		return ErrPeerConnectionClosed
	}

	return pc.pionPC.AddICECandidate(candidate.PionICECandidateInit())
}

// Close closes the peer connection.
func (pc *PeerConnection) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.state == PeerConnectionStateClosed {
		return nil
	}

	// Cancel all goroutines
	if pc.cancel != nil {
		pc.cancel()
	}

	// Close all local tracks
	for _, track := range pc.localTracks {
		if err := track.Close(); err != nil {
			// Log error but continue closing
			_ = err
		}
	}

	// Close the underlying peer connection
	if err := pc.pionPC.Close(); err != nil {
		return err
	}

	pc.state = PeerConnectionStateClosed
	return nil
}

// OnNegotiationNeeded registers a callback invoked whenever the underlying
// peer connection signals that renegotiation is needed. Registering a
// callback does not disable the automatic offer push performed when a
// signaling sender is configured (see handleNegotiationNeeded). Passing nil
// clears a previously registered callback.
func (pc *PeerConnection) OnNegotiationNeeded(callback func()) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.onNegotiationNeeded = callback
}

// OnICECandidate registers a callback invoked for every local ICE candidate
// produced during gathering. The trailing nil sentinel emitted at the end of
// gathering by the WebRTC spec is not forwarded. Passing nil clears a
// previously registered callback.
func (pc *PeerConnection) OnICECandidate(callback func(*ICECandidate)) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.onICECandidate = callback
}

// OnTrack registers a callback invoked when the remote peer starts streaming
// a media track towards us. The wrapper WebRTCTrack passed to the callback is
// the same instance stored in the remote-track registry. Passing nil clears a
// previously registered callback.
func (pc *PeerConnection) OnTrack(callback func(*WebRTCTrack)) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.onTrack = callback
}

// OnLocalTrackAdded registers a callback invoked when a local track is added
// via AddTrack. The callback receives the WebRTCTrack that was just added.
// Passing nil clears a previously registered callback. Callbacks are invoked
// outside PeerConnection.mu via a goroutine with panic recovery; they must
// not acquire TrackForwarder.mu while holding pc.mu (see lock hierarchy).
func (pc *PeerConnection) OnLocalTrackAdded(callback func(*WebRTCTrack)) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.onLocalTrackAdded = callback
}

// handleNegotiationNeeded is called when the peer connection needs to renegotiate.
// It invokes a registered callback (asynchronously, so user code never runs on
// pion's dispatch goroutine) and, when a signaling sender is configured,
// automatically creates an offer and pushes it over the signaling channel.
// Callback panics are recovered and logged.
func (pc *PeerConnection) handleNegotiationNeeded() {
	pc.mu.RLock()
	callback := pc.onNegotiationNeeded
	sendSignaling := pc.sendSignaling
	logger := pc.logger
	pc.mu.RUnlock()

	if callback != nil {
		go func(cb func()) {
			defer func() {
				if r := recover(); r != nil {
					logger.Warn("OnNegotiationNeeded callback panicked",
						"event", "on_negotiation_needed_panic",
						"error", r,
					)
				}
			}()
			cb()
		}(callback)
	}

	if sendSignaling == nil {
		return
	}

	go func() {
		participantID := ""
		if p := pc.Participant(); p != nil {
			participantID = p.ID()
		}

		offer, err := pc.CreateOffer()
		if err != nil {
			pc.logger.Warn("automatic offer push failed",
				"event", "offer_create_failed",
				"participant_id", participantID,
				"error", err,
			)
			return
		}
		if err := sendSignaling("webrtc:offer", offer); err != nil {
			pc.logger.Warn("failed to send offer over signaling channel",
				"event", "signaling_send_failed",
				"participant_id", participantID,
				"msg_type", "webrtc:offer",
				"error", err,
			)
		}
	}()
}

// handleICECandidate is called when a new ICE candidate is generated.
func (pc *PeerConnection) handleICECandidate(candidate *webrtc.ICECandidate) {
	if candidate == nil {
		// Gathering-complete sentinel; nothing to forward.
		return
	}

	pc.dispatchICECandidate(NewICECandidate(candidate))
}

// dispatchICECandidate fans a gathered local candidate out to the registered
// callback and the configured signaling sender.
func (pc *PeerConnection) dispatchICECandidate(wrapped *ICECandidate) {
	pc.mu.RLock()
	callback := pc.onICECandidate
	sendSignaling := pc.sendSignaling
	pc.mu.RUnlock()

	if callback != nil {
		callback(wrapped)
	}

	if sendSignaling != nil {
		// Send the ICE candidate via signaling
		if err := sendSignaling("webrtc:ice-candidate", wrapped); err != nil {
			participantID := ""
			if p := pc.Participant(); p != nil {
				participantID = p.ID()
			}
			pc.logger.Warn("failed to send ICE candidate over signaling channel",
				"event", "signaling_send_failed",
				"participant_id", participantID,
				"msg_type", "webrtc:ice-candidate",
				"error", err,
			)
		}
	}
}

// handleTrack is called when a new remote track is added.
func (pc *PeerConnection) handleTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	_ = receiver

	participantID := ""
	roomID := ""
	if p := pc.Participant(); p != nil {
		participantID = p.ID()
		if r := p.Room(); r != nil {
			roomID = r.ID()
		}
	}

	// Create a domain track for the remote track
	kind := webrtcTrackKindToDomain(track.Kind())
	source := webrtcTrackSource(track.Kind())

	domainTrack, err := domain.NewTrack(track.ID(), kind, source)
	if err != nil {
		pc.logger.Error("failed to wrap remote track in a domain track",
			"event", "remote_track_rejected",
			"participant_id", participantID,
			"room_id", roomID,
			"track_id", track.ID(),
			"error", err,
		)
		return
	}

	// Create a WebRTCTrack wrapper
	webRTCTrack := NewWebRTCTrack(domainTrack, track, track.Codec())

	pc.mu.Lock()
	pc.remoteTracks[track.ID()] = webRTCTrack
	callback := pc.onTrack
	pc.mu.Unlock()

	trackID := track.ID()
	trackKind := kind.String()
	pc.logger.Info("track available",
		"event", "track_available",
		"room_id", roomID,
		"participant_id", participantID,
		"track_id", trackID,
		"kind", trackKind,
	)

	// Invoke outside the lock so callbacks may re-enter PeerConnection
	// methods without deadlocking.
	if callback != nil {
		callback(webRTCTrack)
	}
}

// handleConnectionStateChange is called when the connection state changes.
func (pc *PeerConnection) handleConnectionStateChange(state webrtc.PeerConnectionState) {
	// Snapshot IDs under lock, then log outside to avoid holding mu while logging.
	var participantID string
	var roomID string
	pc.mu.Lock()
	if pc.participant != nil {
		participantID = pc.participant.ID()
		if r := pc.participant.Room(); r != nil {
			roomID = r.ID()
		}
	}
	switch state {
	case webrtc.PeerConnectionStateNew:
		pc.state = PeerConnectionStateNew
	case webrtc.PeerConnectionStateConnecting:
		pc.state = PeerConnectionStateConnecting
	case webrtc.PeerConnectionStateConnected:
		pc.state = PeerConnectionStateConnected
	case webrtc.PeerConnectionStateDisconnected:
		pc.state = PeerConnectionStateDisconnected
	case webrtc.PeerConnectionStateFailed:
		pc.state = PeerConnectionStateFailed
		ConnectionMetrics.IncrementFailures()
	case webrtc.PeerConnectionStateClosed:
		pc.state = PeerConnectionStateClosed
	}
	pc.mu.Unlock()

	switch state {
	case webrtc.PeerConnectionStateConnecting:
		pc.logger.Debug("webrtc connection state changed",
			"event", "connection_state",
			"room_id", roomID,
			"participant_id", participantID,
			"state", "connecting",
		)
	case webrtc.PeerConnectionStateConnected:
		pc.logger.Info("webrtc connection established",
			"event", "peer_connected",
			"room_id", roomID,
			"participant_id", participantID,
			"state", "connected",
		)
	case webrtc.PeerConnectionStateDisconnected:
		pc.logger.Warn("webrtc connection disconnected",
			"event", "peer_disconnected",
			"room_id", roomID,
			"participant_id", participantID,
			"state", "disconnected",
		)
	case webrtc.PeerConnectionStateFailed:
		failures := ConnectionMetrics.FailuresTotal()
		pc.logger.Warn("webrtc connection failed",
			"event", "peer_failed",
			"room_id", roomID,
			"participant_id", participantID,
			"state", "failed",
			"failures_total", failures,
			"error", "peer connection entered failed state",
		)
	case webrtc.PeerConnectionStateClosed:
		pc.logger.Debug("webrtc connection closed",
			"event", "connection_state",
			"room_id", roomID,
			"participant_id", participantID,
			"state", "closed",
		)
	}
}

// NeedsReconnect returns true when the peer connection is in a recoverable disconnected state.
func (pc *PeerConnection) NeedsReconnect() bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.state == PeerConnectionStateDisconnected || pc.state == PeerConnectionStateFailed
}

// webrtcTrackKindToDomain converts a Pion WebRTC track kind to a domain track kind.
func webrtcTrackKindToDomain(kind webrtc.RTPCodecType) domain.TrackKind {
	switch kind {
	case webrtc.RTPCodecTypeAudio:
		return domain.TrackKindAudio
	case webrtc.RTPCodecTypeVideo:
		return domain.TrackKindVideo
	default:
		return domain.TrackKindAudio // Default to audio
	}
}

// webrtcTrackSource determines the track source based on the track kind.
// This is a simple heuristic; in a real implementation, this might be more sophisticated.
func webrtcTrackSource(kind webrtc.RTPCodecType) domain.TrackSource {
	switch kind {
	case webrtc.RTPCodecTypeAudio:
		return domain.TrackSourceMicrophone
	case webrtc.RTPCodecTypeVideo:
		return domain.TrackSourceCamera
	default:
		return domain.TrackSourceMicrophone // Default to microphone
	}
}
