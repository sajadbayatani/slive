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
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/sdp/v3"
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

// egressMIDBase is the first MID value Slive's server-initiated (egress)
// transceivers may claim. Browser-minted publish MIDs count up from 0, so an
// egress MID >= egressMIDBase can never collide with a publish media section
// on this unified PC no matter which negotiation role runs first. Pion honors
// pre-set MIDs at offer generation and sequences auto-assigned MIDs past them.
const egressMIDBase = 100

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
	// iceState is written only by handleICEConnectionStateChange and read via
	// ICEState().
	iceState string
	// rtcpObserver (see RTCPObserver) feeds the keyframe-feedback relay in
	// the signaling layer. Set via SetRTCPObserver; guarded by mu like the
	// other callbacks.
	rtcpObserver RTCPObserver
	// DIAG-PCID: stable per-object identity (pc-<n>) assigned at construction.
	// There is exactly one live PeerConnection per participant (see Handler
	// peerConnections map), serving BOTH publisher and subscriber flows, so
	// pc + participant jointly identify every signaling/media operation.
	instanceID string
	ctx        context.Context
	cancel     context.CancelFunc
	// Negotiation coordinator: exactly one SDP offerer per cycle on this
	// PeerConnection. Browser-initiated publish offers are answered via
	// ProcessBrowserOffer; server-initiated subscriber offers are driven by
	// handleNegotiationNeeded. The two flows must never run concurrently:
	// negMu serializes every check-and-act unit (state inspection plus the
	// pion calls), so a subscriber offer can never strand the PC in
	// have-local-offer when the browser's publisher offer arrives, and an
	// inbound offer can never land mid-offer. The flags below live under
	// pc.mu. Lock order is ALWAYS negMu -> pc.mu; pc.mu is never held
	// across pion calls or signaling sends (existing rule, unchanged).
	negMu sync.Mutex
	// isNegotiating marks an outstanding server offer (have-local-offer,
	// answer pending). While true, further server triggers coalesce into
	// pendingNegotiation. Protected by mu.
	isNegotiating bool
	// pendingNegotiation coalesces server-initiated negotiations deferred
	// while the PC was not stable (or a publish was expected). Flushed as
	// exactly ONE offer when the PC returns to stable. Protected by mu.
	pendingNegotiation bool
	// pendingInboundOffer holds a browser offer that arrived while our own
	// subscriber offer was outstanding (have-local-offer). It is never
	// dropped: it is processed with priority once the PC is stable again,
	// and its answer is delivered via the signaling sender. Protected by mu.
	//
	// A browser owns exactly ONE signaling state machine, so each new offer
	// it sends replaces its previous local offer — the superseded offer's
	// transaction is dead and will never expect an answer. A second inbound
	// browser offer therefore SUPERSEDES the stored one (loudly logged),
	// keeping exactly one deferred lifecycle; answering the stale offer
	// would only be rejected browser-side and re-wedge negotiation.
	pendingInboundOffer *SessionDescription
	// negotiationsQueued counts the triggers coalesced into the current
	// pendingNegotiation epoch — the queued depth behind the ONE pending
	// offer. Incremented on every deferral, reset when the pending need is
	// consumed (offered or skipped). Diagnostic only. Protected by mu.
	negotiationsQueued int
	// nextEgressMID is the next MID to hand out from Slive's server-initiated
	// partition (see egressMIDBase). It never decreases; it is primed to
	// egressMIDBase on first use. Protected by mu.
	nextEgressMID uint64
	// publishSlots are the pre-created publish audio/video transceivers
	// (canonical layout): egress must never stay attached to them even
	// while their MID is still empty, or the publish section is corrupted
	// into an egress section. Set once at construction; guarded by mu.
	publishSlots map[*webrtc.RTPTransceiver]bool
	// negotiationGeneration counts SDP-affecting sender changes on this PC
	// (AddTrack / successful RemoveTrack). It advances even while an offer
	// is outstanding, so deferred work can tell "the outstanding offer
	// already covers this state" from "a new change happened after that
	// offer was created". Protected by mu.
	negotiationGeneration uint64
	// lastOfferGeneration is the negotiationGeneration snapshot that the
	// most recently CREATED server offer was built from (captured before
	// CreateOffer, recorded after SetLocalDescription). A coalesced flush
	// whose generation still equals this value carries no change the
	// outstanding/last offer does not already cover, so it is skipped
	// instead of producing a redundant identical offer. Protected by mu.
	lastOfferGeneration uint64
	// ICE queuing: candidates received before remote description are queued
	// (LiveKit-style trickle ICE) and flushed after SetRemoteDescription.
	// Protected by mu.
	pendingICECandidates []*ICECandidate
}

// RTCPObserver is a hook invoked for every RTCP packet read from a
// subscriber sender (see AddTrack). The signaling layer uses it to relay
// subscriber PLI/FIR upstream to the publisher. It runs on the RTCP reader
// goroutine without holding PeerConnection.mu; implementations must not
// block and must not call back into PeerConnection methods that take mu
// while holding their own locks in conflicting order. A nil observer is a
// no-op. The packet is still discarded afterwards.
type RTCPObserver func(trackID string, packet rtcp.Packet)

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

	// Pion's default answer always declares a=setup:active (DTLSRoleClient),
	// even when a prior subscriber exchange already made the BROWSER the
	// DTLS client (it answered our subscriber offer with setup:active). The
	// answer to a later publish offer would then try to flip the established
	// role and Chrome rejects it: "Failed to set SSL role for the
	// transport". Pinning the server to DTLSRoleServer makes every answer
	// say setup:passive, which matches the subscriber-exchange role and is
	// always acceptable to a browser offerer (actpass).
	settingEngine := webrtc.SettingEngine{}
	settingEngine.SetAnsweringDTLSRole(webrtc.DTLSRoleServer)
	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	// BUNDLE extmap ID alignment (media-delivery root cause, Sep 2026):
	// pion assigns RTP header-extension IDs lowest-free starting at 1 when
	// Slive creates an offer before any remote description exists. With
	// only TWCC registered that yields id=1 for transport-CC, but
	// Chrome/Edge/Safari use id=1 for ssrc-audio-level. Any browser that
	// must then CREATE an offer bundling a Slive-mapped m-line with its
	// own m-lines (the normal subscribe-before-publish order) gets
	// "A BUNDLE group contains a codec collision for header extension
	// id=1", can never complete negotiation, and delivers zero media in
	// both directions. Publish-first PCs survived only because pion adopts
	// the browser's IDs (TWCC=3) from the publish offer.
	// Pre-registering the same extensions with the same IDs Chrome uses
	// (audio-level=1, abs-send-time=2, TWCC=3, sdes:mid=4) makes Slive's
	// first-offer SDP extmap-identical to Chrome's, so mixed offers are
	// consistent by construction. Registration ORDER determines the IDs,
	// so TWCC is registered here explicitly (RegisterDefaultInterceptors
	// below re-registers the same URI, which keeps its position).
	for _, ext := range []struct {
		uri   string
		kinds []webrtc.RTPCodecType
	}{
		{sdp.AudioLevelURI, []webrtc.RTPCodecType{webrtc.RTPCodecTypeAudio}},
		{sdp.ABSSendTimeURI, []webrtc.RTPCodecType{webrtc.RTPCodecTypeAudio, webrtc.RTPCodecTypeVideo}},
		{sdp.TransportCCURI, []webrtc.RTPCodecType{webrtc.RTPCodecTypeAudio, webrtc.RTPCodecTypeVideo}},
		{sdp.SDESMidURI, []webrtc.RTPCodecType{webrtc.RTPCodecTypeAudio, webrtc.RTPCodecTypeVideo}},
	} {
		for _, kind := range ext.kinds {
			if err := mediaEngine.RegisterHeaderExtension(
				webrtc.RTPHeaderExtensionCapability{URI: ext.uri}, kind); err != nil {
				return nil, err
			}
		}
	}
	interceptors := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(mediaEngine, interceptors); err != nil {
		return nil, err
	}
	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(mediaEngine),
		webrtc.WithInterceptorRegistry(interceptors),
		webrtc.WithSettingEngine(settingEngine),
	)
	pionPC, err := api.NewPeerConnection(pionConfig)
	if err != nil {
		return nil, err
	}

	var publishSlots map[*webrtc.RTPTransceiver]bool
	// Canonical m-line layout (media-delivery root cause, Sep 2026):
	// pre-create the publish audio/video transceivers (recvonly, MID left
	// empty) so every PC on every interleaving has the same transceiver
	// order [publish-audio, publish-video, egress...] from birth. Without
	// this, a subscribe-first PC emits offer#1 as [100(,101)] and only
	// appends the publish m-lines later; pion's re-offers then follow the
	// last remote offer's order ([0,1,100,101]) instead of extending the
	// session order ([100...]), and strict browsers reject both the
	// deferred answer ("order of m-lines in answer doesn't match") and
	// the re-offer ("order of m-lines in subsequent offer doesn't
	// match") — zero media in both directions. With the layout fixed,
	// offer#1 already carries the publish sections first.
	// MIDs are deliberately NOT pinned: pion's SetRemote adopts the
	// browser's mids onto reused mid-less transceivers by kind (so a
	// publish-first or video-only offer can never be hijacked by a
	// preoccupied MID), while a subscribe-first browser reuses these same
	// sections via the answer and never mints its own — each flow has
	// exactly one minter. Egress AddTrack still re-homes off these (see
	// ensureEgressTransceiverLocked) exactly as in the publish-first flow.
	for _, kind := range []webrtc.RTPCodecType{
		webrtc.RTPCodecTypeAudio,
		webrtc.RTPCodecTypeVideo,
	} {
		t, err := pionPC.AddTransceiverFromKind(kind, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionRecvonly,
		})
		if err != nil {
			return nil, err
		}
		if publishSlots == nil {
			publishSlots = make(map[*webrtc.RTPTransceiver]bool)
		}
		publishSlots[t] = true
	}

	ctx, cancel := context.WithCancel(context.Background())

	pc := &PeerConnection{
		pionPC:        pionPC,
		instanceID:    fmt.Sprintf("pc-%d", pcInstanceCounter.Add(1)),
		config:        config,
		state:         PeerConnectionStateNew,
		localTracks:   make(map[string]*WebRTCTrack),
		remoteTracks:  make(map[string]*WebRTCTrack),
		participant:   participant,
		sendSignaling: sendSignaling,
		logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
		publishSlots:  publishSlots,
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

	// DIAG-E2E: ICE state is the signal that separates ICE failure from
	// DTLS/SRTP failure (pion v3 exposes no DTLS-state callback).
	pionPC.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		pc.handleICEConnectionStateChange(state)
	})

	// DIAG-PCID: creation log binds instance ID to participant once.
	// DIAG-TURN: include the ICE server summary (TURN URLs + username,
	// never the credential) so a live test can prove which relay this PC
	// gathers from. No SDP/forwarding behavior changes.
	participantID := ""
	if participant != nil {
		participantID = participant.ID()
	}
	logger.Info("peer connection created",
		"event", "pc_created",
		"pc", pc.instanceID,
		"participant_id", participantID,
		"ice_servers", iceServerSummary(config.ICEServers),
	)

	return pc, nil
}

// pcInstanceCounter mints stable per-object PeerConnection IDs.
var pcInstanceCounter atomic.Uint64

// iceServerSummary renders ICE servers for diagnostics as
// "urls=[...] username=... {...}" entries. Credentials are never included.
func iceServerSummary(servers []webrtc.ICEServer) string {
	parts := make([]string, 0, len(servers))
	for _, s := range servers {
		user := ""
		if s.Username != "" {
			user = " username=" + s.Username
		}
		parts = append(parts, fmt.Sprintf("urls=%v%s", s.URLs, user))
	}
	return strings.Join(parts, " | ")
}

// iceCandidateType parses the "typ X" token from a candidate string
// (e.g. "candidate:... typ relay ...") for diagnostics. Returns "unknown"
// when the token is absent.
func iceCandidateType(candidate string) string {
	const marker = " typ "
	idx := strings.Index(candidate, marker)
	if idx < 0 {
		return "unknown"
	}
	rest := candidate[idx+len(marker):]
	if end := strings.IndexByte(rest, ' '); end >= 0 {
		rest = rest[:end]
	}
	if rest == "" {
		return "unknown"
	}
	return rest
}

// InstanceID returns the stable per-object ID (pc-<n>) of this PeerConnection.
// Immutable after construction; safe without locking.
func (pc *PeerConnection) InstanceID() string {
	return pc.instanceID
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
	sender, err = pc.ensureEgressTransceiverLocked(sender, trackLocal)
	if err != nil {
		pc.mu.Unlock()
		return err
	}

	// Store the track and its sender. The generation bump must happen while
	// still under mu and before any offer can observe the new sender, so a
	// mid-offer AddTrack is never mistaken for "already negotiated" by the
	// flush guard. The MID reservation pins the newborn transceiver into the
	// egress partition before pion can auto-assign a low MID that a later
	// browser publish offer would collide with.
	pc.localTracks[track.ID()] = track
	pc.negotiationGeneration++
	pc.reserveEgressMIDLocked(sender)
	egressMid := ""
	for _, t := range pc.pionPC.GetTransceivers() {
		if t.Sender() == sender {
			egressMid = t.Mid()
			break
		}
	}
	pc.logger.Info("egress track added",
		"event", "egress_track_added",
		"pc", pc.instanceID,
		"track_id", track.ID(),
		"mid", egressMid,
		"transceivers", len(pc.pionPC.GetTransceivers()),
	)
	callback := pc.onLocalTrackAdded
	ctx := pc.ctx
	logger := pc.logger
	pc.mu.Unlock()

	// Set up RTCP handling if needed (outside lock).
	// NOTE: subscriber RTCP (NACK/PLI/FIR/REMB) is read here and discarded —
	// it is never forwarded to the publisher by pion. The optional
	// RTCPObserver (if set) sees every packet so the signaling layer can
	// relay keyframe requests upstream. RTCP is still discarded afterwards:
	// no behavior change.
	trackID := track.ID()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				raw, _, err := sender.ReadRTCP()
				if err != nil {
					return
				}
				pc.mu.RLock()
				observer := pc.rtcpObserver
				pc.mu.RUnlock()
				for _, p := range raw {
					if observer != nil {
						observer(trackID, p)
					}
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
			// Removing a sender deactivates its m-line: SDP-affecting.
			pc.negotiationGeneration++
			pc.logger.Info("egress track removed",
				"event", "egress_track_removed",
				"pc", pc.instanceID,
				"track_id", trackID,
			)
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
// LiveKit-style: after setting remote offer, flush any queued ICE candidates.
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

	// Flush queued ICE candidates that arrived before remote offer (LiveKit trickle ICE)
	pc.flushICECandidates()

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
// On success, if a negotiation was queued while an offer was pending, it
// flushes the coalesced request after moving to stable. LiveKit-style ICE
// queuing: any candidates queued before remote description are flushed here.
func (pc *PeerConnection) SetRemoteDescription(sdp *SessionDescription) error {
	pc.mu.Lock()
	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		pc.mu.Unlock()
		return ErrPeerConnectionClosed
	}

	err := pc.pionPC.SetRemoteDescription(*sdp.PionSessionDescription())
	// DIAG-PCID: bind every remote-description install to the exact object.
	// NOTE: pc.mu is write-locked here, so read pc.participant directly —
	// calling Participant() (RLock) would self-deadlock.
	if err == nil {
		participantID := ""
		if pc.participant != nil {
			participantID = pc.participant.ID()
		}
		pc.logger.Info("remote description set",
			"event", "remote_description_set",
			"pc", pc.instanceID,
			"participant_id", participantID,
			"type", sdp.Type().String(),
		)
	}
	shouldFlushNegotiation := false
	var queued []*ICECandidate
	if err == nil {
		if pc.pendingNegotiation && pc.pionPC.SignalingState() == webrtc.SignalingStateStable {
			pc.pendingNegotiation = false
			pc.negotiationsQueued = 0
			pc.isNegotiating = false
			shouldFlushNegotiation = true
		} else if pc.pionPC.SignalingState() == webrtc.SignalingStateStable {
			pc.isNegotiating = false
		}
		// Flush queued ICE candidates after remote description is set.
		if len(pc.pendingICECandidates) > 0 {
			queued = pc.pendingICECandidates
			pc.pendingICECandidates = nil
		}
	}
	pc.mu.Unlock()

	if len(queued) > 0 {
		participantID := ""
		if p := pc.Participant(); p != nil {
			participantID = p.ID()
		}
		pc.logger.Info("Flushing queued ICE candidates",
			"event", "ice_candidates_flushed",
			"participant_id", participantID,
			"count", len(queued),
		)
		for _, c := range queued {
			// Use direct AddICECandidate to avoid re-queuing.
			if err2 := pc.AddICECandidate(c); err2 != nil {
				pc.logger.Warn("flushed ICE candidate failed",
					"event", "ice_flush_failed",
					"participant_id", participantID,
					"error", err2,
				)
			}
		}
	}

	if shouldFlushNegotiation {
		go pc.handleNegotiationNeeded()
	}
	return err
}

// ensureEgressTransceiverLocked keeps egress media off the browser's publish
// m-lines. Pion's AddTrack silently reuses any compatible receive-only
// transceiver; after a publisher exchange those are the browser's own publish
// sections (their MIDs are below egressMIDBase). Egress attached there makes
// the server re-offer the publisher's own m-lines as sendrecv, which the
// browser answers sendonly — the senders have no receiver and downstream
// media never flows (the live two-browser test showed inbound=[] on the
// publisher). Reused egress-partition transceivers (codec reconcile swaps)
// are kept. Callers hold pc.mu; returns the sender that actually carries the
// track.
func (pc *PeerConnection) ensureEgressTransceiverLocked(sender *webrtc.RTPSender, trackLocal webrtc.TrackLocal) (*webrtc.RTPSender, error) {
	if sender == nil {
		return sender, nil
	}
	for _, t := range pc.pionPC.GetTransceivers() {
		if t.Sender() != sender {
			continue
		}
		if mid := t.Mid(); (mid == "" || isEgressMID(mid)) && !pc.publishSlots[t] {
			return sender, nil
		}
		// Pion attached the track to a negotiated publish transceiver — or
		// to a pre-created publish placeholder (canonical layout, still
		// mid-less): detach it (direction reverts to the pre-reuse value)
		// and re-home it on a fresh send-only transceiver that
		// reserveEgressMIDLocked can pin into the egress partition.
		if err := pc.pionPC.RemoveTrack(sender); err != nil {
			return nil, err
		}
		t2, err := pc.pionPC.AddTransceiverFromTrack(trackLocal)
		if err != nil {
			return nil, err
		}
		pc.logger.Info("egress sender re-homed off publish transceiver",
			"event", "egress_transceiver_rehomed",
			"pc", pc.instanceID,
			"reused_mid", t.Mid(),
		)
		return t2.Sender(), nil
	}
	return sender, nil
}

// isEgressMID reports whether the MID belongs to Slive's server-initiated
// egress partition (see egressMIDBase).
func isEgressMID(mid string) bool {
	n, err := strconv.ParseUint(mid, 10, 64)
	return err == nil && n >= egressMIDBase
}

// SwapEgressTrack replaces one egress sender (codec reconcile: remove the
// stale TrackLocal, attach the fresh one) under the negotiation gate. The
// churn must never interleave with an in-flight exchange: a sender attached
// after CreateOffer but before SetRemoteDescription(answer) is started by
// pion against the stale negotiated m-line, and if the browser answered that
// m-line without the new codec the whole answer fails with
// ErrUnsupportedCodec (observed live: a reconciled H264 egress bound against
// a VP8-era answer, deadlocking the subscriber's video). Holding negMu means
// the swap happens either fully before CreateOffer or fully after the answer
// is applied; the generation bump from AddTrack then drives exactly one
// fresh subscriber offer that carries the new codec. Caller must NOT hold
// f.mu-style forwarder locks that conflict with PC callbacks (same contract
// as RemoveTrack/AddTrack).
func (pc *PeerConnection) SwapEgressTrack(trackID string, next *WebRTCTrack) (removedErr error, err error) {
	pc.negMu.Lock()
	defer pc.negMu.Unlock()
	if err := pc.RemoveTrack(trackID); err != nil && !errors.Is(err, ErrTrackNotFound) {
		removedErr = err
	}
	err = pc.AddTrack(next)
	return removedErr, err
}

// reserveEgressMIDLocked assigns the newborn sender's transceiver a MID from
// Slive's server-initiated partition (egressMIDBase upward), keeping it
// disjoint from browser-minted publish MIDs (0 upward) across negotiation
// roles on this unified PC. Pion honors pre-set MIDs at offer generation and
// sequences later ones past them. Best-effort: when pion reused a transceiver
// that already carries a MID, or the offer generator raced ahead, the
// existing assignment stands. Caller holds pc.mu.
func (pc *PeerConnection) reserveEgressMIDLocked(sender *webrtc.RTPSender) {
	if sender == nil || pc.pionPC == nil {
		return
	}
	for _, t := range pc.pionPC.GetTransceivers() {
		if t.Sender() != sender || t.Mid() != "" {
			continue
		}
		if pc.nextEgressMID < egressMIDBase {
			pc.nextEgressMID = egressMIDBase
		}
		mid := pc.nextEgressMID
		pc.nextEgressMID++
		if err := t.SetMid(strconv.FormatUint(mid, 10)); err != nil {
			pc.logger.Debug("egress MID reservation failed, falling back to pion assignment",
				"event", "egress_mid_reserve_failed",
				"pc", pc.instanceID,
				"error", err,
			)
			return
		}
		pc.logger.Info("egress MID reserved",
			"event", "egress_mid_reserved",
			"pc", pc.instanceID,
			"mid", strconv.FormatUint(mid, 10),
		)
		return
	}
}

// bumpNegotiationGeneration records an SDP-affecting sender change on paths
// that do not already hold pc.mu (AddTrack and RemoveTrack bump inline while
// they hold it). The offer-generation guard in flushPendingLocked uses the
// counter to skip re-offers that would carry no new changes. Lock order:
// takes pc.mu briefly; never call with pc.mu held.
func (pc *PeerConnection) bumpNegotiationGeneration() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.negotiationGeneration++
}

// RequestSubscriberOffer drives one server subscriber offer synchronously
// through the gate (used by handleSubscribeTrack right after AddTrack, so a
// first subscription never depends solely on pion's asynchronous
// negotiation-needed callback). If the gate is closed the need stays
// coalesced in pendingNegotiation for the async callback and completion
// flushes; returns true when an offer was sent.
func (pc *PeerConnection) RequestSubscriberOffer() bool {
	pc.negMu.Lock()
	defer pc.negMu.Unlock()
	deferrable := func(reason string) bool {
		pc.mu.Lock()
		firstDefer := !pc.pendingNegotiation
		pc.pendingNegotiation = true
		pc.negotiationsQueued++
		queued := pc.negotiationsQueued
		participantID := ""
		if pc.participant != nil {
			participantID = pc.participant.ID()
		}
		pc.mu.Unlock()
		if firstDefer {
			pc.logger.Info("subscriber offer deferred behind in-flight negotiation",
				"event", "negotiation_queued",
				"pc", pc.instanceID,
				"participant_id", participantID,
				"reason", reason,
				"signaling_state", pc.pionPC.SignalingState().String(),
				"negotiations_queued", queued,
			)
		}
		return false
	}
	pc.mu.Lock()
	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		pc.mu.Unlock()
		return false
	}
	if pc.sendSignaling == nil {
		// No signaling path (tests, detached PCs): match the async gate,
		// which silently skips without arming a pending retry.
		pc.mu.Unlock()
		return false
	}
	if pc.isNegotiating {
		pc.mu.Unlock()
		return deferrable("have-local-offer")
	}
	pc.mu.Unlock()
	if pc.pionPC.SignalingState() != webrtc.SignalingStateStable {
		return deferrable("not-stable")
	}
	if err := pc.serverOfferLocked(); err != nil {
		return false
	}
	return true
}

// ProcessBrowserOffer answers a browser publisher offer (Flow A: browser is
// the offerer, Slive the answerer) through the negotiation gate.
//
//   - stable: the offer is answered inline and the answer is returned.
//     The caller must deliver the answer and then call
//     FlushDeferredNegotiation, which flushes any coalesced server
//     negotiation as exactly one subscriber offer after the answer is
//     enqueued. Returns (answer, false, nil).
//   - have-local-offer (our subscriber offer outstanding): the inbound offer
//     is stored, never dropped, and answered with priority once the PC is
//     stable again. Returns (nil, true, nil); the caller must not report an
//     error — the answer is delivered later via the signaling sender. This
//     replaces the old InvalidModificationError failure. A second inbound
//     offer while one is stored does not error and does not open a second
//     lifecycle: the browser's newest offer supersedes the stored one (its
//     older transaction is dead — a browser has one signaling state
//     machine), so the slot is replaced with a loud supersession log.
//   - any other state: returns ErrUnexpectedOffer without touching pion
//     state (unreachable through the gate; guards legacy callers).
func (pc *PeerConnection) ProcessBrowserOffer(offer *SessionDescription) (*SessionDescription, bool, error) {
	pc.negMu.Lock()
	defer pc.negMu.Unlock()
	pc.mu.Lock()
	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		pc.mu.Unlock()
		return nil, false, ErrPeerConnectionClosed
	}
	pc.mu.Unlock()
	before := pc.pionPC.SignalingState()
	switch before {
	case webrtc.SignalingStateStable:
		answer, err := pc.answerExchangeLocked(offer)
		pc.logNegotiationTransition("browser_offer_answered_inline", "browser", before)
		if err != nil {
			return nil, false, err
		}
		return answer, false, nil
	case webrtc.SignalingStateHaveLocalOffer:
		pc.mu.Lock()
		participantID := ""
		if pc.participant != nil {
			participantID = pc.participant.ID()
		}
		stale := pc.pendingInboundOffer
		pc.pendingInboundOffer = offer
		pc.mu.Unlock()
		op := "browser_offer_deferred"
		if stale != nil {
			op = "browser_offer_superseded"
		}
		pc.logNegotiationTransition(op, "browser", before)
		if stale != nil {
			// Supersession, not an error: the newer offer is the only one
			// whose answer the browser can still apply. The stale offer is
			// dropped from the single deferred slot — never answered, never
			// queued in parallel.
			pc.logger.Warn("browser offer supersedes deferred browser offer",
				"event", "negotiation_inbound_superseded",
				"pc", pc.instanceID,
				"participant_id", participantID,
				"superseded_sdp", SDPSummary(stale.SDP()),
				"incoming_sdp", SDPSummary(offer.SDP()),
			)
		} else {
			pc.logger.Info("inbound browser offer deferred behind outstanding server offer",
				"event", "negotiation_inbound_deferred",
				"pc", pc.instanceID,
				"participant_id", participantID,
				"DIAGX_stored_offer", SDPSummary(offer.SDP()),
			)
		}
		return nil, true, nil
	default:
		err := fmt.Errorf("%w: signaling_state=%s", ErrUnexpectedOffer, before.String())
		pc.logNegotiationTransition("browser_offer_rejected", "browser", before)
		return nil, false, err
	}
}

// ProcessBrowserAnswer installs a browser answer to our outstanding
// subscriber offer (Flow B completion). The PC must hold our offer
// (have-local-offer); otherwise ErrNoPendingOffer is returned and the caller
// applies duplicate/mis-targeted handling. On success the PC is stable and
// deferred work flushes in priority order: a stored inbound browser offer
// first, then one coalesced server offer.
func (pc *PeerConnection) ProcessBrowserAnswer(answer *SessionDescription) error {
	pc.negMu.Lock()
	defer pc.negMu.Unlock()
	pc.mu.Lock()
	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		pc.mu.Unlock()
		return ErrPeerConnectionClosed
	}
	pc.mu.Unlock()
	before := pc.pionPC.SignalingState()
	if before != webrtc.SignalingStateHaveLocalOffer {
		pc.logNegotiationTransition("browser_answer_rejected", "browser", before)
		return ErrNoPendingOffer
	}
	if err := pc.pionPC.SetRemoteDescription(*answer.PionSessionDescription()); err != nil {
		return err
	}
	pc.logRemoteDescription("answer")
	pc.flushICECandidates()
	pc.mu.Lock()
	pc.isNegotiating = false
	pc.mu.Unlock()
	pc.logNegotiationTransition("server_offer_answered", "browser", before)
	pc.flushPendingLocked()
	return nil
}

// answerExchangeLocked runs one inbound answer unit atomically under negMu:
// SetRemote(offer) -> flush ICE queue -> CreateAnswer -> SetLocal(answer) ->
// gather -> clear the publish expectation. The PC must be stable; the caller
// guarantees it. Returns the answer to send. Deferred work is NOT flushed
// here: the answer must reach the browser before any offer produced by a
// flush, and only the caller knows when its send has been enqueued, so the
// caller flushes after delivery (ProcessBrowserAnswer flushes inline; the
// inline publisher-offer path flushes via FlushDeferredNegotiation after
// conn.Send).
func (pc *PeerConnection) answerExchangeLocked(offer *SessionDescription) (*SessionDescription, error) {
	ConnectionMetrics.IncrementAttempts()
	before := pc.pionPC.SignalingState()
	if err := pc.pionPC.SetRemoteDescription(*offer.PionSessionDescription()); err != nil {
		ConnectionMetrics.IncrementFailures()
		return nil, err
	}
	pc.logRemoteDescription("offer")
	pc.flushICECandidates()
	gatherComplete := webrtc.GatheringCompletePromise(pc.pionPC)
	answer, err := pc.pionPC.CreateAnswer(nil)
	if err != nil {
		ConnectionMetrics.IncrementFailures()
		return nil, err
	}
	if err := pc.pionPC.SetLocalDescription(answer); err != nil {
		ConnectionMetrics.IncrementFailures()
		return nil, err
	}
	// Abort instead of blocking forever when the PC is closed mid-gather:
	// GatheringCompletePromise never resolves for a closed PC, and this
	// exchange runs on the message loop under negMu.
	select {
	case <-gatherComplete:
	case <-pc.ctx.Done():
		return nil, ErrPeerConnectionClosed
	}
	localDesc := pc.pionPC.LocalDescription()
	if localDesc == nil {
		ConnectionMetrics.IncrementFailures()
		return nil, ErrInvalidSDP
	}
	wrapped := NewSessionDescription(localDesc)
	pc.logNegotiationTransition("browser_offer_answered", "server", before)
	return wrapped, nil
}

// FlushDeferredNegotiation drains coalesced negotiation work (a stored
// inbound browser offer, then one subscriber offer) once the caller has
// finished delivering the message that preceded the flush. Callers that send
// an answer produced by ProcessBrowserOffer MUST call this only after that
// send is enqueued: conn.Send and the signaling sender share one FIFO send
// channel, so flushing first would let a subscriber offer overtake the
// answer to the browser's own publisher offer (Chrome answers the early
// offer, implicitly rolls back its publish exchange, and the real answer
// lands in stable and is lost).
func (pc *PeerConnection) FlushDeferredNegotiation() {
	pc.negMu.Lock()
	defer pc.negMu.Unlock()
	pc.mu.Lock()
	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		pc.mu.Unlock()
		return
	}
	pc.mu.Unlock()
	pc.flushPendingLocked()
}

// serverOfferLocked creates and sends exactly one server subscriber offer
// under negMu: CreateOffer -> SetLocal -> gather -> webrtc:offer. It marks
// isNegotiating (cleared when the browser's answer is applied) and clears
// pendingNegotiation. The generation guard at entry skips offers that would
// carry no sender change beyond the last created offer (e.g. pion's async
// negotiation-needed echo of an AddTrack the sync driver already offered, or
// a coalesced flush of such an echo): re-offering identical state produces
// the unanswered duplicate offers seen in production. Real changes always
// bump negotiationGeneration (AddTrack / successful RemoveTrack), so they
// never trip the guard. The generation snapshot is taken with the gate flags
// (before CreateOffer) so a mid-offer AddTrack still reads as "newer than
// this offer"; it is recorded as lastOfferGeneration only after
// SetLocalDescription makes the offer live. Pre-SetLocal failures reset
// isNegotiating and re-arm pendingNegotiation so a later trigger retries;
// post-SetLocal the offer is live regardless of send outcome, so
// isNegotiating is kept.
func (pc *PeerConnection) serverOfferLocked() error {
	pc.mu.Lock()
	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		pc.mu.Unlock()
		return ErrPeerConnectionClosed
	}
	sendSignaling := pc.sendSignaling
	participantID := ""
	if pc.participant != nil {
		participantID = pc.participant.ID()
	}
	if sendSignaling == nil {
		pc.mu.Unlock()
		return nil
	}
	before := pc.pionPC.SignalingState()
	if before != webrtc.SignalingStateStable {
		// Invariant enforcement at the operation itself: a server offer is
		// only ever created from stable. A caller that raced past its own
		// check coalesces here instead of driving pion mid-lifecycle; the
		// need stays armed for the next completion flush.
		pc.pendingNegotiation = true
		pc.negotiationsQueued++
		pc.mu.Unlock()
		pc.logger.Info("subscriber offer deferred: signaling state not stable",
			"event", "negotiation_queued",
			"pc", pc.instanceID,
			"participant_id", participantID,
			"reason", "not-stable",
			"signaling_state", before.String(),
			"negotiations_queued", pc.negotiationsQueued,
		)
		return nil
	}
	if pc.lastOfferGeneration == pc.negotiationGeneration {
		pc.pendingNegotiation = false
		pc.negotiationsQueued = 0
		pc.mu.Unlock()
		pc.logger.Debug("subscriber offer skipped: no change since last offer",
			"event", "negotiation_skipped_no_change",
			"pc", pc.instanceID,
			"participant_id", participantID,
		)
		return nil
	}
	pc.pendingNegotiation = false
	pc.negotiationsQueued = 0
	pc.isNegotiating = true
	offerGeneration := pc.negotiationGeneration
	pc.mu.Unlock()

	fail := func(err error) error {
		pc.mu.Lock()
		pc.isNegotiating = false
		pc.pendingNegotiation = true
		pc.mu.Unlock()
		return err
	}

	ConnectionMetrics.IncrementAttempts()
	gatherComplete := webrtc.GatheringCompletePromise(pc.pionPC)
	offer, err := pc.pionPC.CreateOffer(nil)
	if err != nil {
		ConnectionMetrics.IncrementFailures()
		return fail(err)
	}
	if err := pc.pionPC.SetLocalDescription(offer); err != nil {
		ConnectionMetrics.IncrementFailures()
		return fail(err)
	}
	pc.mu.Lock()
	pc.lastOfferGeneration = offerGeneration
	pc.mu.Unlock()
	pc.logNegotiationTransition("server_offer_sent", "server", before)
	// Abort instead of blocking forever when the PC is closed mid-gather:
	// GatheringCompletePromise never resolves for a closed PC, and this runs
	// under negMu (and sometimes on the message loop).
	select {
	case <-gatherComplete:
	case <-pc.ctx.Done():
		return fail(ErrPeerConnectionClosed)
	}
	localDesc := pc.pionPC.LocalDescription()
	if localDesc == nil {
		ConnectionMetrics.IncrementFailures()
		return fail(ErrInvalidSDP)
	}
	wrapped := NewSessionDescription(localDesc)
	offerPayload := struct {
		SourceParticipantID string   `json:"source_participant_id"`
		SDP                 string   `json:"sdp"`
		TrackIDs            []string `json:"track_ids,omitempty"`
	}{
		SourceParticipantID: participantID,
		SDP:                 wrapped.SDP(),
	}
	pc.logger.Info("subscriber offer sent",
		"event", "webrtc_offer_sent",
		"participant_id", participantID,
		"sdp", SDPSummary(wrapped.SDP()),
	)
	if err := sendSignaling("webrtc:offer", offerPayload); err != nil {
		pc.logger.Warn("failed to send offer over signaling channel",
			"event", "signaling_send_failed",
			"participant_id", participantID,
			"msg_type", "webrtc:offer",
			"error", err,
		)
	}
	// isNegotiating stays true until ProcessBrowserAnswer moves to stable.
	return nil
}

// flushPendingLocked drains deferred negotiation work under negMu in strict
// priority order: a stored inbound browser offer first (it establishes
// upstream media), then at most one coalesced server offer. Each step
// re-checks the live pion state, so a unit is only started from the state
// the spec requires (stable). The server-offer step is additionally guarded
// inside serverOfferLocked by the negotiation generation, so coalesced
// triggers that predate the last created offer never produce a redundant
// identical offer.
func (pc *PeerConnection) flushPendingLocked() {
	pc.mu.Lock()
	inbound := pc.pendingInboundOffer
	pc.pendingInboundOffer = nil
	pc.mu.Unlock()
	if inbound != nil {
		if pc.pionPC.SignalingState() != webrtc.SignalingStateStable {
			pc.mu.Lock()
			pc.pendingInboundOffer = inbound
			pc.mu.Unlock()
			pc.logNegotiationTransition("deferred_browser_offer_restored", "server", pc.pionPC.SignalingState())
			return
		}
		answer, err := pc.answerExchangeLocked(inbound)
		if err != nil {
			pc.logger.Warn("deferred browser offer failed",
				"event", "deferred_offer_failed",
				"pc", pc.instanceID,
				"error", err,
			)
			return
		}
		pc.deliverAnswerLocked(answer)
	}
	pc.mu.Lock()
	if !pc.pendingNegotiation {
		pc.mu.Unlock()
		return
	}
	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		pc.pendingNegotiation = false
		pc.mu.Unlock()
		return
	}
	pc.mu.Unlock()
	if pc.pionPC.SignalingState() != webrtc.SignalingStateStable {
		return
	}
	pc.mu.Lock()
	if pc.isNegotiating {
		pc.mu.Unlock()
		return
	}
	pc.mu.Unlock()
	if err := pc.serverOfferLocked(); err != nil {
		pc.logger.Warn("flushed subscriber offer failed",
			"event", "offer_create_failed",
			"pc", pc.instanceID,
			"error", err,
		)
	}
}

// deliverAnswerLocked sends an answer produced outside the message loop
// (deferred inbound offer) back to the browser over the signaling sender.
// Caller holds negMu. The payload mirrors AnswerNotification JSON exactly.
func (pc *PeerConnection) deliverAnswerLocked(answer *SessionDescription) {
	pc.mu.Lock()
	sendSignaling := pc.sendSignaling
	participantID := ""
	if pc.participant != nil {
		participantID = pc.participant.ID()
	}
	pc.mu.Unlock()
	if sendSignaling == nil {
		pc.logger.Warn("deferred answer dropped: no signaling sender",
			"event", "deferred_answer_no_sender",
			"pc", pc.instanceID,
			"participant_id", participantID,
		)
		return
	}
	payload := struct {
		SourceParticipantID string `json:"source_participant_id"`
		SDP                 string `json:"sdp"`
	}{
		SourceParticipantID: participantID,
		SDP:                 answer.SDP(),
	}
	if err := sendSignaling("webrtc:answer", payload); err != nil {
		pc.logger.Warn("failed to deliver deferred answer",
			"event", "deferred_answer_send_failed",
			"pc", pc.instanceID,
			"participant_id", participantID,
			"error", err,
		)
		return
	}
	pc.logger.Info("deferred browser offer answered",
		"event", "deferred_answer_sent",
		"pc", pc.instanceID,
		"participant_id", participantID,
		"DIAGX_answer", SDPSummary(answer.SDP()),
	)
}

// logNegotiationTransition emits one structured record per negotiation
// state-machine transition — the diagnostic spine of the single serialized
// negotiation lifecycle. Every record binds PC and participant to the pion
// signaling state after the operation, who initiated it, and the full gate
// bookkeeping: negotiation generation, deferred browser offer, outstanding
// server offer, coalesced queue depth, and the live MID list. Callable with
// negMu held or not; takes pc.mu only for the snapshot.
func (pc *PeerConnection) logNegotiationTransition(op, initiatedBy string, before webrtc.SignalingState) {
	pc.mu.Lock()
	participantID := ""
	if pc.participant != nil {
		participantID = pc.participant.ID()
	}
	mids := make([]string, 0, 8)
	if pc.pionPC != nil {
		for _, t := range pc.pionPC.GetTransceivers() {
			mid := t.Mid()
			if mid == "" {
				mid = "-"
			}
			mids = append(mids, mid)
		}
	}
	pc.logger.Info("negotiation transition",
		"event", "negotiation_transition",
		"pc", pc.instanceID,
		"participant_id", participantID,
		"operation", op,
		"initiated_by", initiatedBy,
		"signaling_before", before.String(),
		"signaling_after", pc.pionPC.SignalingState().String(),
		"negotiation_generation", pc.negotiationGeneration,
		"last_offer_generation", pc.lastOfferGeneration,
		"server_offer_outstanding", pc.isNegotiating,
		"pending_negotiation", pc.pendingNegotiation,
		"negotiations_queued", pc.negotiationsQueued,
		"deferred_browser_offer", pc.pendingInboundOffer != nil,
		"mids", strings.Join(mids, ","),
	)
	pc.mu.Unlock()
}

// logRemoteDescription emits the remote_description_set lifecycle event.
// It takes pc.mu only briefly for identity fields; callable with or without
// negMu held.
func (pc *PeerConnection) logRemoteDescription(descType string) {
	pc.mu.Lock()
	participantID := ""
	if pc.participant != nil {
		participantID = pc.participant.ID()
	}
	pc.mu.Unlock()
	pc.logger.Info("remote description set",
		"event", "remote_description_set",
		"pc", pc.instanceID,
		"participant_id", participantID,
		"type", descType,
	)
}

// flushICECandidates installs ICE candidates queued before the remote
// description existed (LiveKit-style trickle ICE). It takes pc.mu only in
// brief sections that never span pion calls; callable with or without negMu
// held.
func (pc *PeerConnection) flushICECandidates() {
	pc.mu.Lock()
	queued := pc.pendingICECandidates
	pc.pendingICECandidates = nil
	pc.mu.Unlock()
	if len(queued) == 0 {
		return
	}
	participantID := ""
	if p := pc.Participant(); p != nil {
		participantID = p.ID()
	}
	pc.logger.Info("Flushing queued ICE candidates",
		"event", "ice_candidates_flushed",
		"participant_id", participantID,
		"count", len(queued),
	)
	for _, c := range queued {
		if err2 := pc.AddICECandidate(c); err2 != nil {
			pc.logger.Warn("flushed ICE candidate failed",
				"event", "ice_flush_failed",
				"participant_id", participantID,
				"error", err2,
			)
		}
	}
}

// AddICECandidate adds a remote ICE candidate.
func (pc *PeerConnection) AddICECandidate(candidate *ICECandidate) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		return ErrPeerConnectionClosed
	}

	// DIAG-PCID: prove exactly which object a candidate lands on.
	participantID := ""
	if pc.participant != nil {
		participantID = pc.participant.ID()
	}
	pc.logger.Debug("ice candidate added to pc",
		"event", "ice_candidate_added",
		"pc", pc.instanceID,
		"participant_id", participantID,
	)
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
	pc.isNegotiating = false
	pc.pendingNegotiation = false
	pc.negotiationsQueued = 0
	pc.pendingInboundOffer = nil
	pc.pendingICECandidates = nil
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
// drives exactly one server subscriber offer through the negotiation gate.
//
// Gate (all under negMu, checked against live pion state):
//   - closed or no sender: return.
//   - outstanding server offer (isNegotiating), or pion state != stable:
//     coalesce into pendingNegotiation and return. Additional triggers only
//     re-arm the single flag — never a second offer.
//   - otherwise: create and send one offer now.
//
// The gate re-checks state at execution time because OnNegotiationNeeded is
// asynchronous: by the time this runs, a browser publish exchange may have
// started (or finished). Completion paths (ProcessBrowserOffer,
// ProcessBrowserAnswer) flush a stored flag as exactly one offer.
func (pc *PeerConnection) handleNegotiationNeeded() {
	pc.mu.RLock()
	callback := pc.onNegotiationNeeded
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

	pc.negMu.Lock()
	defer pc.negMu.Unlock()
	pc.mu.Lock()
	if pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed {
		pc.mu.Unlock()
		return
	}
	if pc.sendSignaling == nil {
		pc.mu.Unlock()
		return
	}
	if pc.isNegotiating {
		firstDefer := !pc.pendingNegotiation
		pc.pendingNegotiation = true
		pc.negotiationsQueued++
		queued := pc.negotiationsQueued
		participantID := ""
		if pc.participant != nil {
			participantID = pc.participant.ID()
		}
		sigState := pc.pionPC.SignalingState().String()
		pc.mu.Unlock()
		// First deferral per pending-epoch is operational signal (a stall
		// here with no later completion means media never negotiates);
		// repeats stay at Debug.
		log := pc.logger.Debug
		if firstDefer {
			log = pc.logger.Info
		}
		log("negotiation queued while unavailable",
			"event", "negotiation_queued",
			"pc", pc.instanceID,
			"participant_id", participantID,
			"reason", "have-local-offer",
			"signaling_state", sigState,
			"negotiations_queued", queued,
		)
		return
	}
	pc.mu.Unlock()
	if pc.pionPC.SignalingState() != webrtc.SignalingStateStable {
		pc.mu.Lock()
		firstDefer := !pc.pendingNegotiation
		pc.pendingNegotiation = true
		pc.negotiationsQueued++
		queued := pc.negotiationsQueued
		participantID := ""
		if pc.participant != nil {
			participantID = pc.participant.ID()
		}
		sigState := pc.pionPC.SignalingState().String()
		pc.mu.Unlock()
		log := pc.logger.Debug
		if firstDefer {
			log = pc.logger.Info
		}
		log("negotiation queued signaling not stable",
			"event", "negotiation_queued",
			"pc", pc.instanceID,
			"participant_id", participantID,
			"signaling_state", sigState,
			"negotiations_queued", queued,
		)
		return
	}
	if err := pc.serverOfferLocked(); err != nil {
		participantID := ""
		if p := pc.Participant(); p != nil {
			participantID = p.ID()
		}
		pc.logger.Warn("automatic offer push failed",
			"event", "offer_create_failed",
			"participant_id", participantID,
			"error", err,
		)
	}
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
//
// DIAG-TURN: logs every gathered local candidate with its "typ" (host /
// srflx / relay) so a live test can prove whether this PC gathered a TURN
// relay candidate. Diagnostic only; the candidate flow is unchanged.
func (pc *PeerConnection) dispatchICECandidate(wrapped *ICECandidate) {
	pc.mu.RLock()
	callback := pc.onICECandidate
	sendSignaling := pc.sendSignaling
	pc.mu.RUnlock()

	candidateStr := wrapped.Candidate()
	participantID := ""
	if p := pc.Participant(); p != nil {
		participantID = p.ID()
	}
	pc.logger.Info("ice candidate gathered",
		"event", "ice_candidate_gathered",
		"pc", pc.instanceID,
		"participant_id", participantID,
		"typ", iceCandidateType(candidateStr),
		"candidate", candidateStr,
	)

	if callback != nil {
		callback(wrapped)
	}

	if sendSignaling != nil {
		participantID := ""
		if p := pc.Participant(); p != nil {
			participantID = p.ID()
		}
		payload := struct {
			SourceParticipantID string `json:"source_participant_id"`
			Candidate           string `json:"candidate"`
			SDPMid              string `json:"sdp_mid"`
			SDPMLineIndex       int    `json:"sdp_mline_index"`
		}{
			SourceParticipantID: participantID,
			Candidate:           wrapped.Candidate(),
			SDPMid:              wrapped.SDPMid(),
			SDPMLineIndex:       int(wrapped.SDPMLineIndex()),
		}
		if err := sendSignaling("webrtc:ice-candidate", payload); err != nil {
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

	// Create a domain track for the remote track.
	// LiveKit-style: the published TrackInfo (signaling) and the
	// WebRTC TrackRemote (SDP) must share the same track ID for the SFU
	// forwarder to be found by subscribers. Browsers use getUserMedia
	// IDs (e.g. 520c...) that differ from the signaled IDs
	// (audio-u-*), so we cannot key by TrackRemote.ID() alone.
	// Prefer the existing published domain track from the room registry
	// (matched by kind among this participant's published tracks) so
	// publisher ownership is retained and the forwarder is correctly
	// swapped from placeholder (is_remote=false) to real (is_remote=true)
	// instead of creating a duplicate forwarder with publisher_id="".
	kind := webrtcTrackKindToDomain(track.Kind())
	source := webrtcTrackSource(track.Kind())

	var domainTrack *domain.Track
	if p := pc.Participant(); p != nil {
		if r := p.Room(); r != nil {
			// First try exact ID match (when browser was forced to use signaled ID)
			if existing := r.GetTrack(track.ID()); existing != nil {
				domainTrack = existing
			} else {
				// Fallback: find among this participant's published tracks
				// the first whose kind matches and whose forwarder is still
				// a placeholder (or whose ID will be reused). This handles
				// the common getUserMedia vs signaled ID mismatch.
				for _, pid := range p.PublishedTracks() {
					if cand := r.GetTrack(pid); cand != nil && cand.Kind() == kind {
						// Prefer placeholder not yet swapped (is_remote false)
						// or any unpublished reuse; the first kind match is the
						// correct published track for this remote.
						domainTrack = cand
						break
					}
				}
			}
		}
	}
	if domainTrack == nil {
		var err error
		domainTrack, err = domain.NewTrack(track.ID(), kind, source)
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
		// Fallback ownership: the remote arrived on this participant's PC,
		// so this participant is the publisher when no registry entry exists.
		if p := pc.Participant(); p != nil {
			domainTrack.SetPublisher(p)
			domainTrack.Publish()
		}
	}

	// Create a WebRTCTrack wrapper. Use the published track's ID for the
	// wrapper so Handler.OnTrack→getOrCreateForwarder reuses the placeholder
	// forwarder (publisher_id retained) instead of creating a duplicate with
	// the browser's random remote ID.
	webRTCTrack := NewWebRTCTrack(domainTrack, track, track.Codec())

	pc.mu.Lock()
	// Store under the published ID (domainTrack.ID()) for subscriber lookup,
	// and also under the remote's wire ID for direct remote access.
	pc.remoteTracks[domainTrack.ID()] = webRTCTrack
	if track.ID() != domainTrack.ID() {
		pc.remoteTracks[track.ID()] = webRTCTrack
	}
	callback := pc.onTrack
	pc.mu.Unlock()

	trackID := track.ID()
	trackKind := kind.String()
	publisherID := ""
	if domainTrack != nil {
		if pub := domainTrack.Publisher(); pub != nil {
			publisherID = pub.ID()
		}
	}
	pc.logger.Info("track available",
		"event", "track_available",
		"pc", pc.instanceID,
		"room_id", roomID,
		"participant_id", participantID,
		"track_id", trackID,
		"kind", trackKind,
		"publisher_id", publisherID,
		"is_remote", true,
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
			"pc", pc.instanceID,
			"room_id", roomID,
			"participant_id", participantID,
			"state", "connecting",
		)
	case webrtc.PeerConnectionStateConnected:
		pc.logger.Info("webrtc connection established",
			"event", "peer_connected",
			"pc", pc.instanceID,
			"room_id", roomID,
			"participant_id", participantID,
			"state", "connected",
		)
	case webrtc.PeerConnectionStateDisconnected:
		pc.logger.Warn("webrtc connection disconnected",
			"event", "peer_disconnected",
			"pc", pc.instanceID,
			"room_id", roomID,
			"participant_id", participantID,
			"state", "disconnected",
		)
	case webrtc.PeerConnectionStateFailed:
		failures := ConnectionMetrics.FailuresTotal()
		pc.logger.Warn("webrtc connection failed",
			"event", "peer_failed",
			"pc", pc.instanceID,
			"room_id", roomID,
			"participant_id", participantID,
			"state", "failed",
			"failures_total", failures,
			"error", "peer connection entered failed state",
		)
	case webrtc.PeerConnectionStateClosed:
		pc.logger.Debug("webrtc connection closed",
			"event", "connection_state",
			"pc", pc.instanceID,
			"room_id", roomID,
			"participant_id", participantID,
			"state", "closed",
		)
	}
}

// SDPSummary compresses an SDP into one grep-able line per m-section:
// "m=<kind> <PTs> | mid:<mid> | <direction> | rtpmap:<PT> <codec> ...".
// DIAG-E2E: lets a live two-browser test compare the SFU subscriber offer
// against the browser answer without dumping full SDP.
func SDPSummary(sdp string) string {
	var sections []string
	var cur []string
	flush := func() {
		if len(cur) > 0 {
			sections = append(sections, strings.Join(cur, " "))
			cur = nil
		}
	}
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimRight(line, "\r")
		switch {
		case strings.HasPrefix(line, "m="):
			flush()
			cur = append(cur, line)
		case strings.HasPrefix(line, "a=mid:") ||
			strings.HasPrefix(line, "a=sendrecv") ||
			strings.HasPrefix(line, "a=sendonly") ||
			strings.HasPrefix(line, "a=recvonly") ||
			strings.HasPrefix(line, "a=inactive") ||
			strings.HasPrefix(line, "a=rtpmap:"):
			cur = append(cur, line)
		}
	}
	flush()
	return strings.Join(sections, " || ")
}

// handleICEConnectionStateChange records and logs ICE transitions.
// DIAG-E2E: reaching connected/completed proves ICE (and consent) works, so a
// later failed state with good ICE points at DTLS/SRTP instead.
func (pc *PeerConnection) handleICEConnectionStateChange(state webrtc.ICEConnectionState) {
	var participantID, roomID string
	pc.mu.Lock()
	pc.iceState = state.String()
	if pc.participant != nil {
		participantID = pc.participant.ID()
		if r := pc.participant.Room(); r != nil {
			roomID = r.ID()
		}
	}
	pc.mu.Unlock()

	pc.logger.Info("ice connection state changed",
		"event", "ice_state",
		"pc", pc.instanceID,
		"room_id", roomID,
		"participant_id", participantID,
		"state", state.String(),
	)

	// DIAG-TURN: on connected/completed, report the selected ICE candidate
	// pair (local typ <-> remote typ) to prove whether media flows via the
	// TURN relay. Best effort: SCTP may be absent on media-only PCs, and
	// pion v3 exposes the pair only via the SCTP/ICE transport.
	if state == webrtc.ICEConnectionStateConnected || state == webrtc.ICEConnectionStateCompleted {
		pc.logSelectedCandidatePair(roomID, participantID)
	}
}

// logSelectedCandidatePair queries the SCTP/ICE transport for the selected
// pair and logs local/remote typ, protocol and address. Every failure mode
// (no SCTP, no pair yet) is logged explicitly so "no pair" is never silent.
func (pc *PeerConnection) logSelectedCandidatePair(roomID, participantID string) {
	defer func() {
		_ = recover()
	}()
	pc.mu.RLock()
	pionPC := pc.pionPC
	pc.mu.RUnlock()
	if pionPC == nil {
		pc.logger.Info("ice selected pair unavailable",
			"event", "ice_selected_pair",
			"pc", pc.instanceID,
			"room_id", roomID,
			"participant_id", participantID,
			"reason", "nil peer connection",
		)
		return
	}
	sctp := pionPC.SCTP()
	if sctp == nil {
		pc.logger.Info("ice selected pair unavailable",
			"event", "ice_selected_pair",
			"pc", pc.instanceID,
			"room_id", roomID,
			"participant_id", participantID,
			"reason", "no sctp transport (media-only pc)",
		)
		return
	}
	transport := sctp.Transport()
	if transport == nil {
		pc.logger.Info("ice selected pair unavailable",
			"event", "ice_selected_pair",
			"pc", pc.instanceID,
			"room_id", roomID,
			"participant_id", participantID,
			"reason", "no dtls transport",
		)
		return
	}
	iceTransport := transport.ICETransport()
	if iceTransport == nil {
		pc.logger.Info("ice selected pair unavailable",
			"event", "ice_selected_pair",
			"pc", pc.instanceID,
			"room_id", roomID,
			"participant_id", participantID,
			"reason", "no ice transport",
		)
		return
	}
	pair, err := iceTransport.GetSelectedCandidatePair()
	if err != nil || pair == nil || pair.Local == nil || pair.Remote == nil {
		pc.logger.Info("ice selected pair unavailable",
			"event", "ice_selected_pair",
			"pc", pc.instanceID,
			"room_id", roomID,
			"participant_id", participantID,
			"reason", "no selected pair yet",
			"error", err,
		)
		return
	}
	pc.logger.Info("ice selected pair",
		"event", "ice_selected_pair",
		"pc", pc.instanceID,
		"room_id", roomID,
		"participant_id", participantID,
		"local_typ", pair.Local.Typ.String(),
		"local_protocol", pair.Local.Protocol.String(),
		"local_address", fmt.Sprintf("%s:%d", pair.Local.Address, pair.Local.Port),
		"remote_typ", pair.Remote.Typ.String(),
		"remote_protocol", pair.Remote.Protocol.String(),
		"remote_address", fmt.Sprintf("%s:%d", pair.Remote.Address, pair.Remote.Port),
		"via_relay", pair.Local.Typ == webrtc.ICECandidateTypeRelay || pair.Remote.Typ == webrtc.ICECandidateTypeRelay,
	)
}

// WriteRTCP sends RTCP packets (e.g. relayed PLI/FIR) to this PC's remote.
// It is the upstream half of the keyframe-feedback relay: the signaling
// layer calls it on the PUBLISHER's PC with a PLI/FIR addressed to the
// publisher-ingress media SSRC. Packets are passed to pion unchanged; no
// RTP state is touched.
func (pc *PeerConnection) WriteRTCP(pkts []rtcp.Packet) error {
	pc.mu.RLock()
	pionPC := pc.pionPC
	closed := pc.state == PeerConnectionStateClosed || pc.state == PeerConnectionStateFailed
	pc.mu.RUnlock()
	if pionPC == nil || closed {
		return ErrPeerConnectionClosed
	}
	return pionPC.WriteRTCP(pkts)
}

// EgressSSRCMap returns the subscriber-egress media SSRCs currently bound on
// this PC, mapped to their track IDs. Pion assigns each RTPSender a fixed
// SSRC at AddTrack time and rewrites every forwarded packet to the
// per-binding SSRC/payload-type on WriteRTP, so a subscriber PLI's MediaSSRC
// (egress SSRC) differs from the publisher-ingress SSRC. The keyframe relay
// uses this map to resolve an incoming PLI/FIR MediaSSRC to the subscribed
// track without trusting the reader-closure track ID alone.
func (pc *PeerConnection) EgressSSRCMap() map[uint32]string {
	out := make(map[uint32]string)
	pc.mu.RLock()
	pionPC := pc.pionPC
	pc.mu.RUnlock()
	if pionPC == nil {
		return out
	}
	for _, sender := range pionPC.GetSenders() {
		track := sender.Track()
		if track == nil {
			continue
		}
		for _, enc := range sender.GetParameters().Encodings {
			if enc.SSRC != 0 {
				out[uint32(enc.SSRC)] = track.ID()
			}
		}
	}
	return out
}

// ICEState returns the last observed ICE connection state.
func (pc *PeerConnection) ICEState() string {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.iceState
}

// SetRTCPObserver registers a hook invoked for every RTCP packet read from
// this PC's senders (see RTCPObserver). Passing nil clears it.
// Additive instrumentation only; the RTCP discard behavior is unchanged.
func (pc *PeerConnection) SetRTCPObserver(fn RTCPObserver) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.rtcpObserver = fn
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
