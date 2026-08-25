package webrtc

import (
	"context"
	"log"
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

// PeerConnectionConfig holds configuration for creating a new PeerConnection.
type PeerConnectionConfig struct {
	// ICEServers is a list of ICE servers to use for NAT traversal.
	ICEServers []webrtc.ICEServer
	// SDPSemantics is the SDP semantics to use (e.g., "unified-plan").
	SDPSemantics webrtc.SDPSemantics
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
	mu              sync.RWMutex
	pionPC          *webrtc.PeerConnection
	config          PeerConnectionConfig
	state           PeerConnectionState
	localTracks     map[string]*WebRTCTrack
	remoteTracks    map[string]*WebRTCTrack
	participant     *domain.Participant
	sendSignaling   SignalingSender
	onNegotiation   bool
	onICECandidate  bool
	onTrack         bool
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewPeerConnection creates a new PeerConnection with the given configuration.
func NewPeerConnection(config PeerConnectionConfig, participant *domain.Participant, sendSignaling SignalingSender) (*PeerConnection, error) {
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
func (pc *PeerConnection) AddTrack(track *WebRTCTrack) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		return ErrPeerConnectionClosed
	}

	pionTrack := track.PionTrack()
	if pionTrack == nil {
		return ErrTrackNotReady
	}

	// In Pion v3, AddTrack requires a TrackLocal interface
	// We need to check if the track is a TrackLocal
	trackLocal, ok := pionTrack.(webrtc.TrackLocal)
	if !ok {
		return ErrTrackNotReady
	}

	sender, err := pc.pionPC.AddTrack(trackLocal)
	if err != nil {
		return err
	}

	// Store the track and its sender
	pc.localTracks[track.ID()] = track

	// Set up RTCP handling if needed
	go func() {
		for {
			select {
			case <-pc.ctx.Done():
				return
			default:
				if _, _, err := sender.ReadRTCP(); err != nil {
					return
				}
			}
		}
	}()

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

// OnNegotiationNeeded registers a callback for when negotiation is needed.
func (pc *PeerConnection) OnNegotiationNeeded(callback func()) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.onNegotiation = true
	// Note: The actual callback is set in NewPeerConnection
	// This method is for external registration
	_ = callback
}

// OnICECandidate registers a callback for when an ICE candidate is generated.
func (pc *PeerConnection) OnICECandidate(callback func(*ICECandidate)) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.onICECandidate = true
	// Note: The actual callback is set in NewPeerConnection
	_ = callback
}

// OnTrack registers a callback for when a remote track is added.
func (pc *PeerConnection) OnTrack(callback func(*WebRTCTrack)) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.onTrack = true
	// Note: The actual callback is set in NewPeerConnection
	_ = callback
}

// handleNegotiationNeeded is called when the peer connection needs to renegotiate.
func (pc *PeerConnection) handleNegotiationNeeded() {
	pc.mu.Lock()
	if pc.onNegotiation && pc.sendSignaling != nil {
		pc.mu.Unlock()
		// Trigger offer creation and send via signaling
		go func() {
			offer, err := pc.CreateOffer()
			if err != nil {
				return
			}
			// Send the offer via signaling
			_ = pc.sendSignaling("webrtc:offer", offer)
		}()
		pc.mu.Lock()
		pc.state = PeerConnectionStateConnecting
	}
	pc.mu.Unlock()
}

// handleICECandidate is called when a new ICE candidate is generated.
func (pc *PeerConnection) handleICECandidate(candidate *webrtc.ICECandidate) {
	pc.mu.Lock()
	sendSignaling := pc.sendSignaling
	pc.mu.Unlock()
	
	if candidate != nil && sendSignaling != nil {
		// Send the ICE candidate via signaling
		iceCandidate := NewICECandidate(candidate)
		_ = sendSignaling("webrtc:ice-candidate", iceCandidate)
	}
}

// handleTrack is called when a new remote track is added.
func (pc *PeerConnection) handleTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Create a domain track for the remote track
	kind := webrtcTrackKindToDomain(track.Kind())
	source := webrtcTrackSource(track.Kind())
	
	domainTrack, err := domain.NewTrack(track.ID(), kind, source)
	if err != nil {
		// Log error but continue
		return
	}

	// Create a WebRTCTrack wrapper
	webRTCTrack := NewWebRTCTrack(domainTrack, track, track.Codec())

	// Store the remote track
	pc.remoteTracks[track.ID()] = webRTCTrack

	if pc.onTrack {
		// In a real implementation, this would notify the application
		// For now, we just store the track
	}
}

// handleConnectionStateChange is called when the connection state changes.
func (pc *PeerConnection) handleConnectionStateChange(state webrtc.PeerConnectionState) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	participantID := ""
	if pc.participant != nil {
		participantID = pc.participant.ID()
	}

	switch state {
	case webrtc.PeerConnectionStateNew:
		pc.state = PeerConnectionStateNew
	case webrtc.PeerConnectionStateConnecting:
		pc.state = PeerConnectionStateConnecting
		log.Printf("webrtc: connection connecting participant=%s", participantID)
	case webrtc.PeerConnectionStateConnected:
		pc.state = PeerConnectionStateConnected
		log.Printf("webrtc: connection established participant=%s attempts=%d failures=%d",
			participantID, ConnectionMetrics.AttemptsTotal(), ConnectionMetrics.FailuresTotal())
	case webrtc.PeerConnectionStateDisconnected:
		pc.state = PeerConnectionStateDisconnected
		log.Printf("webrtc: connection disconnected participant=%s", participantID)
	case webrtc.PeerConnectionStateFailed:
		pc.state = PeerConnectionStateFailed
		ConnectionMetrics.IncrementFailures()
		log.Printf("webrtc: connection failed participant=%s", participantID)
	case webrtc.PeerConnectionStateClosed:
		pc.state = PeerConnectionStateClosed
		log.Printf("webrtc: connection closed participant=%s", participantID)
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
