package signaling

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

// Handler handles WebSocket signaling connections.
//
// Lock ordering (must be respected to avoid ABBA deadlocks):
//
//	gcMu > peerConnectionsMutex > trackForwardersMutex > Room.mu > Participant.mu
//
// reapGhost acquires gcMu first then peerConnectionsMutex/trackForwardersMutex
// before calling Room.Leave (which acquires Room.mu), preserving this order.
type Handler struct {
	roomManager          *RoomManager
	connectionManager    *ConnectionManager
	peerConnections      map[string]*webrtc.PeerConnection
	peerConnectionsMutex sync.RWMutex
	// trackForwarders holds the SFU forwarding state keyed by track ID.
	trackForwarders      map[string]*webrtc.TrackForwarder
	trackForwardersMutex sync.RWMutex
	// peerConnectionConfig is used for every peer connection this handler
	// creates; it defaults to DefaultPeerConnectionConfig() and can be
	// overridden with WithPeerConnectionConfig.
	peerConnectionConfig webrtc.PeerConnectionConfig
	// logger receives structured lifecycle and error events; it is also
	// handed to every WebSocket connection and peer connection created by
	// this handler.
	logger *slog.Logger

	// forwarderConfig is used for every TrackForwarder this handler creates.
	forwarderConfig webrtc.ForwarderConfig

	// GC state for ghost participants whose transport dropped without explicit leave.
	gcTTL         time.Duration
	gcTicker      *time.Ticker
	gcStop        chan struct{}
	ghostTimers   map[string]*time.Timer
	gcMu          sync.Mutex
	gcReapedCount uint64

	// WebSocket policy and deadlines.
	allowedOrigins []string
	wsReadTimeout  time.Duration
	wsPingInterval time.Duration
	wsWriteTimeout time.Duration
}

// HandlerOption customises a Handler at construction time.
type HandlerOption func(*Handler)

// WithPeerConnectionConfig sets the configuration used for every peer
// connection created by the handler (join and reconnect paths alike). Use a
// STUN-free config in tests to keep negotiation deterministic and offline.
func WithPeerConnectionConfig(config webrtc.PeerConnectionConfig) HandlerOption {
	return func(h *Handler) {
		h.peerConnectionConfig = config
	}
}

// WithLogger sets the structured logger used for connection lifecycle and
// error events; it is propagated to every WebSocket connection and peer
// connection the handler creates. Passing nil keeps the default logger.
func WithLogger(logger *slog.Logger) HandlerOption {
	return func(h *Handler) {
		if logger != nil {
			h.logger = logger
		}
	}
}

// WithForwarderConfig sets the ForwarderConfig used for every TrackForwarder
// created by the handler. Zero value keeps today's behavior (DefaultQueueSize 64).
func WithForwarderConfig(cfg webrtc.ForwarderConfig) HandlerOption {
	return func(h *Handler) {
		h.forwarderConfig = cfg
	}
}

// WithGCTTL sets the ghost-participant GC TTL. A TTL of 0 disables GC.
// Default is 60s. Exposed for config wiring and tests (TTL 100ms in tests).
func WithGCTTL(d time.Duration) HandlerOption {
	return func(h *Handler) {
		h.gcTTL = d
	}
}

// WithAllowedOrigins sets the allowlist for cross-origin WebSocket requests.
// Implements D1: no-Origin allowed, same-origin allowed, exact allowlist matches allowed.
func WithAllowedOrigins(origins []string) HandlerOption {
	return func(h *Handler) {
		if origins == nil {
			h.allowedOrigins = nil
			return
		}
		cp := make([]string, len(origins))
		copy(cp, origins)
		h.allowedOrigins = cp
	}
}

// WithWSReadTimeout sets the WebSocket read deadline. Zero uses default 60s.
func WithWSReadTimeout(d time.Duration) HandlerOption {
	return func(h *Handler) {
		h.wsReadTimeout = d
	}
}

// WithWSPingInterval sets the ping interval. Zero uses default 30s.
// Enforced ≤ ReadTimeout/2 at construction.
func WithWSPingInterval(d time.Duration) HandlerOption {
	return func(h *Handler) {
		h.wsPingInterval = d
	}
}

// WithWSWriteTimeout sets the WebSocket write deadline. Zero uses default 10s.
func WithWSWriteTimeout(d time.Duration) HandlerOption {
	return func(h *Handler) {
		h.wsWriteTimeout = d
	}
}

// NewHandler creates a new Handler.
func NewHandler(roomManager *RoomManager, opts ...HandlerOption) *Handler {
	h := &Handler{
		roomManager:          roomManager,
		connectionManager:    NewConnectionManager(),
		peerConnections:      make(map[string]*webrtc.PeerConnection),
		trackForwarders:      make(map[string]*webrtc.TrackForwarder),
		peerConnectionConfig: webrtc.DefaultPeerConnectionConfig(),
		logger:               slog.Default(),
		gcTTL:                60 * time.Second,
		ghostTimers:          make(map[string]*time.Timer),
		wsReadTimeout:        DefaultWSReadTimeout,
		wsPingInterval:       DefaultWSPingInterval,
		wsWriteTimeout:       DefaultWSWriteTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
	if h.ghostTimers == nil {
		h.ghostTimers = make(map[string]*time.Timer)
	}
	if h.wsReadTimeout <= 0 {
		h.wsReadTimeout = DefaultWSReadTimeout
	}
	if h.wsWriteTimeout <= 0 {
		h.wsWriteTimeout = DefaultWSWriteTimeout
	}
	if h.wsPingInterval <= 0 {
		h.wsPingInterval = DefaultWSPingInterval
	}
	if h.wsPingInterval > h.wsReadTimeout/2 {
		h.wsPingInterval = h.wsReadTimeout / 2
	}
	return h
}

// GCReapedCount returns the number of ghost participants reaped.
func (h *Handler) GCReapedCount() uint64 {
	return atomic.LoadUint64(&h.gcReapedCount)
}

// CloseRoom closes the room with roomID via canonical teardown: per-participant
// Leave is handled by Room.Close, forwarders for the room's tracks are stopped,
// peer connections for participants in the room are closed, ghost timers cancelled,
// and the room is removed from the manager. It returns ErrRoomNotFound for unknown rooms.
func (h *Handler) CloseRoom(roomID string) error {
	room := h.roomManager.GetRoom(roomID)
	if room == nil {
		return ErrRoomNotFound
	}
	participantIDs := room.Participants()
	trackIDs := room.Tracks()
	for _, tid := range trackIDs {
		h.removeForwarder(tid)
	}
	for _, pid := range participantIDs {
		if pc := h.getPeerConnection(pid); pc != nil {
			h.removeSubscriberFromAllForwarders(pc)
		}
		h.closePeerConnection(pid)
		h.cancelGhostTimer(pid)
	}
	return h.roomManager.CloseRoom(roomID)
}

// Shutdown gracefully shuts down all WebSocket connections and peer connections
// managed by this handler. It closes the connection manager (which sends orderly
// close frames to all registered WS clients) and closes all tracked peer
// connections. After this method returns, the handler should not be used for new
// connections.
func (h *Handler) Shutdown() error {
	// Stop GC timers/loop first so no reapGhost runs after shutdown.
	h.gcMu.Lock()
	if h.gcTicker != nil {
		h.gcTicker.Stop()
	}
	if h.gcStop != nil {
		close(h.gcStop)
		h.gcStop = nil
	}
	for _, t := range h.ghostTimers {
		t.Stop()
	}
	h.ghostTimers = make(map[string]*time.Timer)
	h.gcMu.Unlock()

	h.connectionManager.CloseAll()

	// Stop all forwarders first: forwarder.Stop() removes the forwarded track
	// from every subscriber PC, so do it before closing the PCs themselves.
	h.trackForwardersMutex.Lock()
	for trackID, fw := range h.trackForwarders {
		if err := fw.Stop(); err != nil {
			h.logger.Warn("failed to stop forwarder during shutdown",
				"event", "forwarder_stop_failed",
				"track_id", trackID,
				"error", err,
			)
		}
		delete(h.trackForwarders, trackID)
	}
	h.trackForwardersMutex.Unlock()

	h.peerConnectionsMutex.Lock()
	defer h.peerConnectionsMutex.Unlock()

	for id, pc := range h.peerConnections {
		if err := pc.Close(); err != nil {
			h.logger.Warn("failed to close peer connection during shutdown",
				"event", "peer_connection_close_failed",
				"participant_id", id,
				"error", err,
			)
		}
		delete(h.peerConnections, id)
	}

	return nil
}

// ServeHTTP implements http.Handler for WebSocket connections.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Extract room ID and participant ID from the request
	// TODO: Implement proper path parsing and authentication
	roomID := r.URL.Query().Get("room_id")
	participantID := r.URL.Query().Get("participant_id")

	if roomID == "" || participantID == "" {
		http.Error(w, "room_id and participant_id are required", http.StatusBadRequest)
		return
	}

	// Create a new WebSocket connection
	conn, err := NewConnectionWithConfig(h.logger, w, r, roomID, participantID, h.allowedOrigins, h.wsReadTimeout, h.wsPingInterval, h.wsWriteTimeout)
	if err != nil {
		h.logger.Error("failed to upgrade websocket connection",
			"event", "ws_upgrade_failed",
			"room_id", roomID,
			"participant_id", participantID,
			"error", err,
		)
		return
	}

	// Register the connection. It stays registered for the whole lifetime of
	// the session: broadcasts to other room members are delivered through
	// this registry, so removal must happen on the handleConnection cleanup
	// path, not when ServeHTTP returns (it returns immediately after
	// spawning the goroutine).
	h.connectionManager.Add(conn)

	// Handle the connection in a goroutine
	go h.handleConnection(conn)
}

// handleConnection handles messages from a single connection.
func (h *Handler) handleConnection(conn *Connection) {
	defer conn.Close()

	// Deregister the connection when its lifecycle ends (i.e. when this
	// goroutine returns, after the cleanup below has run). Removing here —
	// and not when ServeHTTP returns — keeps the registry populated so
	// broadcasts reach room members. RemoveIf leaves a newer connection
	// registered if a reconnecting participant already replaced this one.
	defer h.connectionManager.RemoveIf(conn.ID(), conn)

	// Get or create the room
	room, err := h.roomManager.GetOrCreateRoom(conn.RoomID())
	if err != nil {
		h.logger.Error("failed to get or create room",
			"event", "room_lookup_failed",
			"participant_id", conn.ID(),
			"room_id", conn.RoomID(),
			"error", err,
		)
		_ = conn.Send(MessageTypeError, ErrorResponse{
			Error:       "Failed to get or create room",
			Code:        ErrorCodeInternalError,
			RequestType: string(MessageTypeJoinRoom),
		})
		return
	}

	// Signaling sender bound to this WebSocket connection; it is handed to
	// (or swapped onto) the participant's peer connection below so that
	// negotiation and ICE events flow over the newest transport.
	sender := func(msgType string, data interface{}) error {
		return conn.Send(MessageType(msgType), data)
	}

	// Get or create the participant
	participant := room.GetParticipant(conn.ID())
	if participant == nil {
		// Create a new participant
		participant = domain.NewParticipant(conn.ID(), "Participant "+conn.ID())
		if err := room.Join(participant); err != nil {
			h.logger.Error("failed to join room",
				"event", "join_failed",
				"participant_id", conn.ID(),
				"room_id", room.ID(),
				"error", err,
			)
			_ = conn.Send(MessageTypeError, ErrorResponse{
				Error:       "Failed to join room",
				Code:        errorCodeFromDomainError(err),
				RequestType: string(MessageTypeJoinRoom),
			})
			return
		}
		participant.SetRoom(room)

		// Initialize a peer connection for the participant
		if _, err := h.ensurePeerConnection(participant, sender); err != nil {
			h.logger.Error("failed to create peer connection",
				"event", "peer_connection_create_failed",
				"participant_id", participant.ID(),
				"error", err,
			)
			_ = conn.Send(MessageTypeError, ErrorResponse{
				Error:       "Failed to create peer connection",
				Code:        ErrorCodeInternalError,
				RequestType: string(MessageTypeJoinRoom),
			})
			return
		}

		// Notify other participants that a new participant has joined
		h.broadcastParticipantJoined(room, participant)
	} else {
		// Reconnect existing participant - cancel any pending ghost reap.
		h.cancelGhostTimer(conn.ID())

		participant.SetRoom(room)

		// Reuse the existing peer connection or replace it when unusable;
		// either way its signaling output follows the new connection.
		if _, err := h.ensurePeerConnection(participant, sender); err != nil {
			h.logger.Warn("failed to recreate peer connection on reconnect",
				"event", "peer_connection_recreate_failed",
				"participant_id", participant.ID(),
				"error", err,
			)
		}
	}

	// Structured event: peer_connected (outside mu, after ensurePeerConnection)
	{
		roomID := room.ID()
		participantID := participant.ID()
		state := ""
		if pc := h.getPeerConnection(participantID); pc != nil {
			state = pc.State().String()
		}
		h.logger.Info("peer connected",
			"event", "peer_connected",
			"room_id", roomID,
			"participant_id", participantID,
			"state", state,
		)
	}

	// Send room joined response
	if err := h.sendRoomJoined(conn, room, participant); err != nil {
		h.logger.Error("failed to send room joined response",
			"event", "room_joined_send_failed",
			"participant_id", conn.ID(),
			"room_id", room.ID(),
			"error", err,
		)
		return
	}

	// Main message loop
	for {
		msg, err := conn.Receive()
		if err != nil {
			if err == ErrConnectionClosed {
				h.logger.Info("message loop ended: transport closed",
					"event", "receive_loop_ended",
					"participant_id", conn.ID(),
					"reason", "connection_closed",
				)
			} else {
				h.logger.Warn("failed to receive message; ending message loop",
					"event", "receive_loop_ended",
					"participant_id", conn.ID(),
					"reason", "receive_failed",
					"error", err,
				)
			}
			break
		}

		if err := h.handleMessage(conn, room, participant, msg); err != nil {
			h.logger.Warn("failed to handle message",
				"event", "message_handle_failed",
				"participant_id", conn.ID(),
				"msg_type", string(msg.Type),
				"error", err,
			)
			_ = conn.Send(MessageTypeError, ErrorResponse{
				Error:       err.Error(),
				Code:        errorCodeFromError(err),
				RequestType: string(msg.Type),
			})
		}
	}

	// Clean up when connection closes. The transport is gone, but the
	// session (participant + peer connection) stays alive for reconnect.
	h.handleConnectionClosed(room, participant)
}

// ensurePeerConnection returns the peer connection for the given participant,
// creating or replacing it as necessary.
//
//   - When a usable peer connection already exists it is reused as-is; only
//     its signaling sender is swapped so events follow the newest WebSocket.
//   - When the existing connection is Closed or Failed it can no longer carry
//     media: it is closed for good and replaced by a fresh one created with
//     the handler's configured PeerConnectionConfig.
func (h *Handler) ensurePeerConnection(participant *domain.Participant, sender webrtc.SignalingSender) (*webrtc.PeerConnection, error) {
	h.peerConnectionsMutex.RLock()
	existing := h.peerConnections[participant.ID()]
	h.peerConnectionsMutex.RUnlock()

	if existing != nil && existing.State().Usable() {
		// Update the signaling sender to use the new connection
		existing.UpdateSignalingSender(sender)
		return existing, nil
	}

	pc, err := webrtc.NewPeerConnection(h.peerConnectionConfig, participant, sender)
	if err != nil {
		return nil, err
	}

	if existing != nil {
		// Best effort: the old connection is already unusable.
		_ = existing.Close()
	}

	h.peerConnectionsMutex.Lock()
	h.peerConnections[participant.ID()] = pc
	h.peerConnectionsMutex.Unlock()

	// SFU hook: when this participant publishes a track (local or remote),
	// lazily create/start the forwarder so later subscribers can attach.
	pc.OnLocalTrackAdded(func(track *webrtc.WebRTCTrack) {
		if track == nil {
			return
		}
		if _, err := h.getOrCreateForwarder(track.ID(), track); err != nil {
			h.logger.Warn("failed to create forwarder via OnLocalTrackAdded",
				"event", "forwarder_create_failed",
				"participant_id", participant.ID(),
				"track_id", track.ID(),
				"error", err,
			)
		}
	})
	pc.OnTrack(func(track *webrtc.WebRTCTrack) {
		if track == nil {
			return
		}
		if _, err := h.getOrCreateForwarder(track.ID(), track); err != nil {
			h.logger.Warn("failed to create forwarder via OnTrack",
				"event", "forwarder_create_failed",
				"participant_id", participant.ID(),
				"track_id", track.ID(),
				"error", err,
			)
			return
		}
		// A real TrackRemote means the publisher's initial offer/answer has
		// completed. Attach it to existing participants now, rather than
		// racing the publisher's first negotiation from publish_track.
		if room := participant.Room(); room != nil && room.GetTrack(track.ID()) != nil {
			h.autoSubscribeExistingParticipants(room, participant, track.ID())
		}
	})
	// Observe subscriber RTCP feedback (PLI/FIR) and relay keyframe requests
	// upstream via relayKeyframeRequest (RTCP-to-RTCP, no signaling message,
	// no RTP changes). Other feedback types need no action. Locks are taken
	// sequentially (never nested) to preserve the lock hierarchy.
	pc.SetRTCPObserver(func(trackID string, packet rtcp.Packet) {
		kind, senderSSRC, mediaSSRC, ok := rtcpFeedbackIdentity(packet)
		if !ok {
			return
		}
		if kind == "PLI" || kind == "FIR" {
			h.relayKeyframeRequest(pc, trackID, kind, senderSSRC, mediaSSRC)
		}
	})

	return pc, nil
}

// publisherIDForTrack resolves the publisher participant ID for a forwarded
// track, or "" when the track has no forwarder/publisher. Lock-free
// sequencing: getForwarder takes and releases its own lock.
func (h *Handler) publisherIDForTrack(trackID string) string {
	if fw := h.getForwarder(trackID); fw != nil {
		if pt := fw.PublisherTrack(); pt != nil {
			if dt := pt.DomainTrack(); dt != nil {
				if pub := dt.Publisher(); pub != nil {
					return pub.ID()
				}
			}
		}
	}
	return ""
}

// relayKeyframeRequest routes one subscriber PLI/FIR upstream to the
// publisher's PeerConnection (RTCP-to-RTCP; no signaling message, no RTP
// changes). It returns (forwarded, publisherID).
//
// Routing (never blind):
//  1. trackID -> forwarder (drops feedback for unknown tracks);
//  2. subscriber MediaSSRC -> subscribed track via the subscriber PC's
//     egress SSRC map (drops feedback for unknown/unmapped SSRCs, so a PLI
//     can never reach an unrelated publisher);
//  3. forwarder -> publisher participant -> publisher PC;
//  4. publisher-ingress media SSRC from the publisher's TrackRemote;
//  5. fresh PLI/FIR addressed to the publisher-ingress SSRC is sent with
//     publisherPC.WriteRTCP().
//
// The subscriber-side SSRC is never reused as the media SSRC: pion rewrites
// SSRC per subscriber binding on egress, so the two sides differ by design.
// The upstream SenderSSRC preserves the subscriber's RTCP sender SSRC for
// traceability; the publisher keys the request off MediaSSRC. FIR entries
// draw sequence numbers from the forwarder's per-track space.
func (h *Handler) relayKeyframeRequest(subPC *webrtc.PeerConnection, trackID, kind string, senderSSRC, mediaSSRC uint32) (bool, string) {
	if subPC == nil {
		return false, ""
	}
	fw := h.getForwarder(trackID)
	if fw == nil {
		return false, ""
	}
	// SSRC-gated track resolution: the PLI's MediaSSRC must be this track's
	// egress SSRC on this subscriber PC.
	if mappedTrack, ok := subPC.EgressSSRCMap()[mediaSSRC]; !ok || mappedTrack != trackID {
		return false, h.publisherIDForTrack(trackID)
	}
	publisherID := h.publisherIDForTrack(trackID)
	if publisherID == "" {
		return false, ""
	}
	pubPC := h.getPeerConnection(publisherID)
	if pubPC == nil {
		return false, publisherID
	}
	pubTrack := fw.PublisherTrack()
	if pubTrack == nil {
		return false, publisherID
	}
	publisherSSRC, ok := pubTrack.RemoteSSRC()
	if !ok {
		return false, publisherID
	}
	var pkt rtcp.Packet
	switch kind {
	case "PLI":
		pkt = &rtcp.PictureLossIndication{SenderSSRC: senderSSRC, MediaSSRC: publisherSSRC}
	case "FIR":
		pkt = &rtcp.FullIntraRequest{
			SenderSSRC: senderSSRC,
			MediaSSRC:  publisherSSRC,
			FIR:        []rtcp.FIREntry{{SSRC: publisherSSRC, SequenceNumber: fw.NextFIRSequenceNumber()}},
		}
	default:
		return false, publisherID
	}
	if err := pubPC.WriteRTCP([]rtcp.Packet{pkt}); err != nil {
		return false, publisherID
	}
	return true, publisherID
}

// requestUpstreamKeyframe asks the publisher encoder for a fresh keyframe
// for trackID (RTCP PLI via the publisher PC, no signaling message, no RTP
// changes). It is Slive-initiated (no subscriber SSRC to validate), so the
// upstream SenderSSRC reuses the publisher-ingress SSRC for traceability;
// publishers key the request off MediaSSRC. No-ops unless a real remote
// publisher track with a live PC exists. Called after subscribe-attach and
// after publisher swaps with subscribers present, so late subscribers get a
// decodable keyframe without waiting for their own PLI round-trip.
func (h *Handler) requestUpstreamKeyframe(trackID string) {
	fw := h.getForwarder(trackID)
	if fw == nil {
		return
	}
	pt := fw.PublisherTrack()
	if pt == nil || !pt.IsRemote() {
		return
	}
	pubSSRC, ok := pt.RemoteSSRC()
	if !ok || pubSSRC == 0 {
		return
	}
	publisherID := h.publisherIDForTrack(trackID)
	if publisherID == "" {
		return
	}
	pubPC := h.getPeerConnection(publisherID)
	if pubPC == nil {
		return
	}
	pkt := &rtcp.PictureLossIndication{SenderSSRC: pubSSRC, MediaSSRC: pubSSRC}
	if err := pubPC.WriteRTCP([]rtcp.Packet{pkt}); err != nil {
		h.logger.Warn("upstream keyframe request failed",
			"event", "upstream_keyframe_request_failed",
			"track_id", trackID,
			"publisher_id", publisherID,
			"publisher_media_ssrc", pubSSRC,
			"error", err,
		)
	}
}

// rtcpFeedbackIdentity extracts (kind, sender SSRC, media SSRC) from
// keyframe/loss feedback RTCP packets. ok=false for all other RTCP types
// (TWCC, SR/RR, REMB, SDES...), which the observer skips.
func rtcpFeedbackIdentity(packet rtcp.Packet) (kind string, senderSSRC, mediaSSRC uint32, ok bool) {
	switch pkt := packet.(type) {
	case *rtcp.PictureLossIndication:
		return "PLI", pkt.SenderSSRC, pkt.MediaSSRC, true
	case *rtcp.FullIntraRequest:
		return "FIR", pkt.SenderSSRC, pkt.MediaSSRC, true
	case *rtcp.TransportLayerNack:
		return "NACK", pkt.SenderSSRC, pkt.MediaSSRC, true
	default:
		return "", 0, 0, false
	}
}

// --- SFU forwarder registry helpers ---

// getForwarder returns the forwarder for trackID if it exists.
func (h *Handler) getForwarder(trackID string) *webrtc.TrackForwarder {
	h.trackForwardersMutex.RLock()
	defer h.trackForwardersMutex.RUnlock()
	return h.trackForwarders[trackID]
}

// getOrCreateForwarder returns the existing forwarder for trackID or creates
// one backed by publisherTrack and starts it. Thread-safe.
//
// If a forwarder already exists but is bound to a TrackLocal placeholder
// while publisherTrack is a real TrackRemote, the publisher is swapped via
// TrackForwarder.UpdatePublisher so that the forwarding loop pumps real RTP
// instead of staying stuck on the dummy track that exited immediately.
func (h *Handler) getOrCreateForwarder(trackID string, publisherTrack *webrtc.WebRTCTrack) (*webrtc.TrackForwarder, error) {
	if publisherTrack == nil {
		return nil, webrtc.ErrTrackNotReady
	}
	// Fast path: already exists. Handle placeholder → real swap.
	h.trackForwardersMutex.RLock()
	if fw := h.trackForwarders[trackID]; fw != nil {
		h.trackForwardersMutex.RUnlock()
		if publisherTrack.IsRemote() {
			if pt := fw.PublisherTrack(); pt != nil && !pt.IsRemote() {
				h.logger.Info("forwarder publisher swap starting",
					"event", "forwarder_swap_start",
					"track_id", trackID,
				)
				if err := fw.UpdatePublisher(publisherTrack); err != nil {
					return nil, err
				}
				// The swap may have reconciled subscriber egress codecs;
				// request a fresh keyframe so they decode immediately.
				if fw.SubscriberCount() > 0 {
					h.requestUpstreamKeyframe(trackID)
				}
			}
		}
		return fw, nil
	}
	h.trackForwardersMutex.RUnlock()

	h.trackForwardersMutex.Lock()
	// swappedWithSubs defers the upstream keyframe request until after the
	// map lock is released: requestUpstreamKeyframe takes getForwarder's
	// RLock, which would self-deadlock under the write lock.
	swappedWithSubs := false
	if fw := h.trackForwarders[trackID]; fw != nil {
		if publisherTrack.IsRemote() {
			if pt := fw.PublisherTrack(); pt != nil && !pt.IsRemote() {
				h.logger.Info("forwarder publisher swap starting",
					"event", "forwarder_swap_start",
					"track_id", trackID,
				)
				if err := fw.UpdatePublisher(publisherTrack); err != nil {
					h.trackForwardersMutex.Unlock()
					return nil, err
				}
				swappedWithSubs = fw.SubscriberCount() > 0
			}
		}
		h.trackForwardersMutex.Unlock()
		if swappedWithSubs {
			h.requestUpstreamKeyframe(trackID)
		}
		return fw, nil
	}
	fw, err := webrtc.NewTrackForwarderWithConfig(publisherTrack, h.forwarderConfig)
	if err != nil {
		h.trackForwardersMutex.Unlock()
		return nil, err
	}
	if err := fw.Start(); err != nil {
		h.trackForwardersMutex.Unlock()
		return nil, err
	}
	h.trackForwarders[trackID] = fw
	h.trackForwardersMutex.Unlock()
	h.logger.Info("forwarder created",
		"event", "forwarder_created",
		"track_id", trackID,
		"is_remote", publisherTrack.IsRemote(),
	)
	return fw, nil
}

// adoptOrphanForwarder merges a wire-ID forwarder left behind by Ordering B
// (WebRTC OnTrack fired before publish_track, or the browser wire track ID
// differs from the signaled ID) into the signaled-ID forwarder for sigTrack.
//
// It scans for a forwarder that (a) is keyed by a different ID, (b) wraps a
// real remote publisher track of the same participant and kind, and (c) is an
// orphan, i.e. its ID is not a track registered in the room. When found, the
// orphan is stopped and removed, and the signaled-ID forwarder is created (or
// swapped, if a placeholder already exists) around the same TrackRemote
// re-wrapped in the signaled domain track, so every subscriber-facing ID
// stays the signaled one. Returns true when an adoption happened.
//
// Locking: takes trackForwardersMutex only for the scan; removal and creation
// go through removeForwarder/getOrCreateForwarder, preserving the
// peerConnectionsMutex > trackForwardersMutex order (no PC lock held here).
func (h *Handler) adoptOrphanForwarder(room *domain.Room, sigTrack *domain.Track, participant *domain.Participant) bool {
	if room == nil || sigTrack == nil || participant == nil {
		return false
	}
	sigID := sigTrack.ID()

	h.trackForwardersMutex.RLock()
	var orphan *webrtc.TrackForwarder
	orphanID := ""
	for id, fw := range h.trackForwarders {
		if id == sigID {
			continue
		}
		pt := fw.PublisherTrack()
		if pt == nil || !pt.IsRemote() {
			continue
		}
		dt := pt.DomainTrack()
		if dt == nil || dt.Kind() != sigTrack.Kind() {
			continue
		}
		pub := dt.Publisher()
		if pub == nil || pub.ID() != participant.ID() {
			continue
		}
		if room.GetTrack(id) != nil {
			// A legitimately registered track's forwarder: never steal it
			// (e.g. a second same-kind track from the same participant).
			continue
		}
		orphan, orphanID = fw, id
		break
	}
	h.trackForwardersMutex.RUnlock()

	if orphan == nil {
		return false
	}

	orphanPub := orphan.PublisherTrack()
	remote, ok := orphanPub.PionTrack().(*pionwebrtc.TrackRemote)
	if !ok || remote == nil {
		return false
	}
	codec := orphanPub.Codec()
	adopted := webrtc.NewWebRTCTrack(sigTrack, remote, codec)

	// Stop the orphan first so two run loops never Read the same TrackRemote
	// concurrently, then bind the signaled forwarder to the real track.
	// getOrCreateForwarder creates it fresh, or swaps a placeholder via
	// UpdatePublisher when one already exists.
	h.removeForwarder(orphanID)
	if _, err := h.getOrCreateForwarder(sigID, adopted); err != nil {
		h.logger.Warn("failed to adopt orphan forwarder",
			"event", "forwarder_adopt_failed",
			"participant_id", participant.ID(),
			"track_id", sigID,
			"orphan_track_id", orphanID,
			"error", err,
		)
		return false
	}
	h.logger.Info("forwarder adopted orphan wire-ID track",
		"event", "forwarder_adopted",
		"room_id", room.ID(),
		"participant_id", participant.ID(),
		"track_id", sigID,
		"orphan_track_id", orphanID,
		"kind", sigTrack.Kind().String(),
	)
	return true
}

// removeForwarder stops the forwarder for trackID and removes it from the registry.
func (h *Handler) removeForwarder(trackID string) {
	h.trackForwardersMutex.Lock()
	fw, exists := h.trackForwarders[trackID]
	if exists {
		delete(h.trackForwarders, trackID)
	}
	h.trackForwardersMutex.Unlock()
	if exists {
		_ = fw.Stop()
	}
}

// removeSubscriberFromForwarder removes pc as subscriber from forwarder for trackID.
// If the forwarder has no more subscribers it is stopped and removed.
func (h *Handler) removeSubscriberFromForwarder(trackID string, pc *webrtc.PeerConnection) {
	if pc == nil {
		return
	}
	h.trackForwardersMutex.RLock()
	fw := h.trackForwarders[trackID]
	h.trackForwardersMutex.RUnlock()
	if fw == nil {
		return
	}
	_ = fw.RemoveSubscriber(pc)
	if fw.SubscriberCount() == 0 {
		// Keep publisher forwarder alive until explicit unpublish/leave.
		// Spec says stop and remove when no more subscribers; we do so only
		// for unsubscribe path where publisher still alive. For publisher
		// teardown, removeForwarder already handles stopping regardless.
		// To honor spec, stop and remove on last unsubscribe.
		h.trackForwardersMutex.Lock()
		// Re-check under write lock that count is still zero and entry is same.
		if cur := h.trackForwarders[trackID]; cur == fw && fw.SubscriberCount() == 0 {
			delete(h.trackForwarders, trackID)
			h.trackForwardersMutex.Unlock()
			_ = fw.Stop()
			return
		}
		h.trackForwardersMutex.Unlock()
	}
}

// removeSubscriberFromAllForwarders removes pc from every forwarder (leave path).
func (h *Handler) removeSubscriberFromAllForwarders(pc *webrtc.PeerConnection) {
	if pc == nil {
		return
	}
	h.trackForwardersMutex.RLock()
	fws := make([]*webrtc.TrackForwarder, 0, len(h.trackForwarders))
	ids := make([]string, 0, len(h.trackForwarders))
	for id, fw := range h.trackForwarders {
		fws = append(fws, fw)
		ids = append(ids, id)
	}
	h.trackForwardersMutex.RUnlock()
	for i, fw := range fws {
		_ = fw.RemoveSubscriber(pc)
		if fw.SubscriberCount() == 0 {
			// Only auto-remove subscriber-only forwarders; publisher
			// forwarder cleanup is handled by removeForwarder on unpublish/leave.
			// Check if publisher still alive: if forwarder still has publisher
			// track published, keep it. For leave cleanup we explicitly handle
			// publisher forwarders elsewhere. Here we stop idle forwarders.
			h.trackForwardersMutex.Lock()
			if cur := h.trackForwarders[ids[i]]; cur == fw && fw.SubscriberCount() == 0 {
				// If publisher track still exists in room, don't auto-remove;
				// but spec says to stop and remove when no subscribers.
				// We remove to avoid leaking idle forwarders.
				delete(h.trackForwarders, ids[i])
				h.trackForwardersMutex.Unlock()
				_ = fw.Stop()
			} else {
				h.trackForwardersMutex.Unlock()
			}
		}
	}
}

// createPublisherWebRTCTrack builds a WebRTCTrack wrapping domainTrack with a
// Pion TrackLocalStaticRTP suitable for AddTrack. Codec is inferred from kind.
func (h *Handler) createPublisherWebRTCTrack(domainTrack *domain.Track) (*webrtc.WebRTCTrack, error) {
	// Codec is UNKNOWN until the real TrackRemote arrives (OnTrack). The
	// stored codec params stay zero-valued so no consumer mistakes the
	// placeholder for an authoritative source; AddSubscriber infers a
	// provisional egress codec from kind and UpdatePublisher reconciles
	// subscribers once the real codec is known. The pion object is never
	// bound (never added to any PC), so an empty capability fails loudly
	// if it is ever misused instead of silently masquerading as VP8/Opus.
	pionTrack, err := pionwebrtc.NewTrackLocalStaticRTP(pionwebrtc.RTPCodecCapability{}, domainTrack.ID(), domainTrack.ID()+"-stream")
	if err != nil {
		return nil, err
	}
	return webrtc.NewWebRTCTrack(domainTrack, pionTrack, pionwebrtc.RTPCodecParameters{}), nil
}

// handleMessage handles a single message from a connection.
func (h *Handler) handleMessage(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	switch msg.Type {
	case MessageTypeCreateRoom:
		return h.handleCreateRoom(conn, msg)

	case MessageTypeJoinRoom:
		return h.handleJoinRoom(conn, room, participant, msg)

	case MessageTypeLeaveRoom:
		return h.handleLeaveRoom(conn, room, participant, msg)

	case MessageTypePublishTrack:
		return h.handlePublishTrack(conn, room, participant, msg)

	case MessageTypeUnpublishTrack:
		return h.handleUnpublishTrack(conn, room, participant, msg)

	case MessageTypeSubscribeTrack:
		return h.handleSubscribeTrack(conn, room, participant, msg)

	case MessageTypeUnsubscribeTrack:
		return h.handleUnsubscribeTrack(conn, room, participant, msg)

	case MessageTypeOffer:
		return h.handleOffer(conn, room, participant, msg)

	case MessageTypeAnswer:
		return h.handleAnswer(conn, room, participant, msg)

	case MessageTypeICECandidate:
		return h.handleICECandidate(conn, room, participant, msg)

	default:
		h.logger.Debug("ignoring unknown message type",
			"event", "unknown_message_type",
			"participant_id", conn.ID(),
			"msg_type", string(msg.Type),
		)
		return nil
	}
}

// handleCreateRoom handles a create room request.
// It is idempotent: creating an already-existing room with an already-joined
// participant returns success instead of participant_already_exists, matching
// the SDK Client.JoinRoom contract (B-4) and preventing duplicate lifecycle
// errors when the WebSocket auto-join (handleConnection) and an explicit
// create_room message race for the same room/participant.
func (h *Handler) handleCreateRoom(conn *Connection, msg *Message) error {
	var req CreateRoomRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	room, err := h.roomManager.GetOrCreateRoom(req.RoomID)
	if err != nil {
		return err
	}

	// If participant already in room, treat as idempotent success (no duplicate Join).
	if existing := room.GetParticipant(req.ParticipantID); existing != nil {
		resp := RoomCreatedResponse{
			RoomID:        req.RoomID,
			ParticipantID: req.ParticipantID,
			Status:        "success",
		}
		return conn.Send(MessageTypeRoomCreated, resp)
	}

	participant := domain.NewParticipant(req.ParticipantID, req.ParticipantName)
	if err := room.Join(participant); err != nil {
		// Race: another goroutine (auto-join or concurrent create) won.
		if existing := room.GetParticipant(req.ParticipantID); existing != nil {
			resp := RoomCreatedResponse{
				RoomID:        req.RoomID,
				ParticipantID: req.ParticipantID,
				Status:        "success",
			}
			return conn.Send(MessageTypeRoomCreated, resp)
		}
		return err
	}
	participant.SetRoom(room)

	// Ensure a peer connection exists for the newly created participant
	// (the auto-join path in handleConnection does this, but a pure
	// create_room via message must also have one). If the WS transport is
	// the same participant, bind the sender to the new PC so future
	// negotiation/ICE pushes use this socket.
	if conn.ID() == req.ParticipantID && conn.RoomID() == req.RoomID {
		sender := func(msgType string, data interface{}) error {
			return conn.Send(MessageType(msgType), data)
		}
		if _, err := h.ensurePeerConnection(participant, sender); err != nil {
			h.logger.Warn("failed to create peer connection for create_room",
				"event", "peer_connection_create_failed",
				"participant_id", participant.ID(),
				"error", err,
			)
		} else {
			h.broadcastParticipantJoined(room, participant)
		}
	}

	resp := RoomCreatedResponse{
		RoomID:        req.RoomID,
		ParticipantID: req.ParticipantID,
		Status:        "success",
	}

	return conn.Send(MessageTypeRoomCreated, resp)
}

// handleJoinRoom handles a join room request.
func (h *Handler) handleJoinRoom(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req JoinRoomRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Update participant name if provided
	if req.ParticipantName != "" {
		// TODO: Add SetName method to Participant
	}

	// Send room joined response
	return h.sendRoomJoined(conn, room, participant)
}

// handleLeaveRoom handles a leave room request.
//
// This is the explicit leave path: unlike a WebSocket drop it is terminal —
// the participant leaves the room and its peer connection is closed and
// removed from the handler registry (see closePeerConnection).
func (h *Handler) handleLeaveRoom(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req LeaveRoomRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Cancel any pending ghost timer for this participant.
	h.cancelGhostTimer(req.ParticipantID)

	// Snapshot SFU state before room.Leave clears participant bookkeeping.
	publishedTracks := participant.PublishedTracks()
	// Resolve subscriber PC before it is removed.
	leaverPC := h.getPeerConnection(req.ParticipantID)

	// Remove participant from room
	if err := room.Leave(req.ParticipantID); err != nil {
		return err
	}

	// Publisher teardown: stop forwarders for tracks this participant published.
	for _, trackID := range publishedTracks {
		h.removeForwarder(trackID)
	}
	// Subscriber teardown: detach leaver from every forwarder it subscribed to.
	if leaverPC != nil {
		h.removeSubscriberFromAllForwarders(leaverPC)
	}

	// Tear down the peer connection: an explicit leave does not get to
	// reconnect into the same media session.
	h.closePeerConnection(req.ParticipantID)

	// Send response
	resp := RoomLeftResponse{
		RoomID:        req.RoomID,
		ParticipantID: req.ParticipantID,
		Status:        "success",
	}

	if err := conn.Send(MessageTypeRoomLeft, resp); err != nil {
		return err
	}

	// Notify other participants
	h.broadcastParticipantLeft(room, participant)

	return nil
}

// closePeerConnection closes and deregisters the peer connection of the given
// participant. Used on the explicit leave_room path; the WebSocket-drop path
// deliberately keeps the peer connection alive so a reconnecting client
// resumes its media session.
func (h *Handler) closePeerConnection(participantID string) {
	h.peerConnectionsMutex.Lock()
	pc, exists := h.peerConnections[participantID]
	if exists {
		delete(h.peerConnections, participantID)
	}
	h.peerConnectionsMutex.Unlock()

	if !exists {
		return
	}

	if err := pc.Close(); err != nil {
		h.logger.Warn("failed to close peer connection",
			"event", "peer_connection_close_failed",
			"participant_id", participantID,
			"error", err,
		)
	}
}

// handlePublishTrack handles a publish track request.
func (h *Handler) handlePublishTrack(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req PublishTrackRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Convert track info to domain.Track
	kind := domain.TrackKindAudio
	if req.Track.Kind == "video" {
		kind = domain.TrackKindVideo
	}

	source := domain.TrackSourceMicrophone
	switch req.Track.Source {
	case "camera":
		source = domain.TrackSourceCamera
	case "screen_share":
		source = domain.TrackSourceScreenShare
	}

	track, err := domain.NewTrack(req.Track.ID, kind, source)
	if err != nil {
		return err
	}
	if err := participant.PublishTrack(track); err != nil {
		return err
	}

	// Register the track in the room-wide registry so that subscribers can
	// resolve it through the room instead of reaching into the publisher.
	// Re-publishing after a reconnect is tolerated as idempotent.
	if err := room.PublishTrack(track); err != nil && err != domain.ErrTrackAlreadyPublished {
		return err
	}

	// SFU wiring: ensure a forwarder exists so subscribers can attach.
	// We create a standalone WebRTCTrack (not added to publisher PC) to avoid
	// spurious negotiation on the publisher side; real media arrives via
	// OnTrack and the forwarder will forward via WriteRTP. If a local track
	// already exists on the publisher PC (e.g. from a previous AddTrack via
	// another path), prefer that track as the forwarder source.
	//
	// Ordering-B adoption: when the publisher's WebRTC OnTrack fired before
	// this publish_track (or the browser wire track ID differs from the
	// signaled ID), a real remote-backed "orphan" forwarder may already exist
	// under the wire ID. Adopt it into the signaled-ID forwarder instead of
	// leaving two forwarders (one with RTP and no subscribers, one with
	// subscribers and no RTP).
	if h.getForwarder(track.ID()) == nil && !h.adoptOrphanForwarder(room, track, participant) {
		// Prefer an existing local track on the publisher PC if present.
		var publisherWebTrack *webrtc.WebRTCTrack
		if pc := h.getPeerConnection(participant.ID()); pc != nil {
			publisherWebTrack = pc.GetLocalTrack(track.ID())
		}
		if publisherWebTrack == nil {
			webTrack, err := h.createPublisherWebRTCTrack(track)
			if err != nil {
				h.logger.Warn("failed to create publisher web track",
					"event", "publisher_track_create_failed",
					"participant_id", participant.ID(),
					"track_id", track.ID(),
					"error", err,
				)
			} else {
				if _, err := h.getOrCreateForwarder(track.ID(), webTrack); err != nil {
					h.logger.Warn("failed to create forwarder for publisher track",
						"event", "forwarder_create_failed",
						"participant_id", participant.ID(),
						"track_id", track.ID(),
						"error", err,
					)
				}
			}
		} else {
			if _, err := h.getOrCreateForwarder(track.ID(), publisherWebTrack); err != nil {
				h.logger.Warn("failed to ensure forwarder for existing publisher track",
					"event", "forwarder_ensure_failed",
					"participant_id", participant.ID(),
					"track_id", track.ID(),
					"error", err,
				)
			}
		}
	}
	// If OnTrack arrived before publish_track, adoptOrphanForwarder above has
	// just rebound the real remote track under the signaled ID. The OnTrack
	// callback could not attach it then because the room had no publication;
	// attach it now, but never attach a provisional placeholder here.
	if fw := h.getForwarder(track.ID()); fw != nil {
		if publisherTrack := fw.PublisherTrack(); publisherTrack != nil && publisherTrack.IsRemote() {
			h.autoSubscribeExistingParticipants(room, participant, track.ID())
		}
	}
	// Send response
	resp := TrackPublishedResponse{
		TrackID:       req.Track.ID,
		ParticipantID: req.ParticipantID,
		Status:        "success",
	}

	if err := conn.Send(MessageTypeTrackPublished, resp); err != nil {
		return err
	}

	// Structured event outside mu: track_available
	roomID := room.ID()
	participantID := participant.ID()
	trackID := track.ID()
	kindStr := track.Kind().String()
	h.logger.Info("track available",
		"event", "track_available",
		"room_id", roomID,
		"participant_id", participantID,
		"track_id", trackID,
		"kind", kindStr,
	)

	// Notify other participants
	h.broadcastTrackAvailable(room, participant, track)

	return nil
}

// autoSubscribeExistingParticipants attaches trackID to every other live
// participant in room and drives the subscriber-side renegotiation. The
// domain subscription registry remains client-driven; this server-side SFU
// attachment makes live publication symmetric while preserving the existing
// subscribe_track protocol and its duplicate/error semantics.
func (h *Handler) autoSubscribeExistingParticipants(room *domain.Room, publisher *domain.Participant, trackID string) {
	fw := h.getForwarder(trackID)
	if fw == nil {
		return
	}
	for _, participantID := range room.Participants() {
		if participantID == publisher.ID() {
			continue
		}
		pc := h.getPeerConnection(participantID)
		if pc == nil {
			continue
		}
		// Do not start a server-initiated negotiation before the participant's
		// initial browser offer has been answered. Once the browser offer is
		// stable, a connecting PC can safely queue the subscriber offer behind
		// ICE/DTLS establishment.
		if !h.subscriberReadyForAutomaticAttach(pc) {
			continue
		}
		alreadyAttached := fw.HasSubscriber(pc)
		if err := fw.AddSubscriber(pc); err != nil {
			h.logger.Warn("automatic subscriber attach failed",
				"event", "automatic_subscriber_attach_failed",
				"track_id", trackID,
				"publisher_id", publisher.ID(),
				"subscriber_id", participantID,
				"error", err,
			)
			continue
		}
		h.logger.Info("automatic subscriber attached",
			"event", "automatic_subscriber_attached",
			"track_id", trackID,
			"publisher_id", publisher.ID(),
			"subscriber_id", participantID,
		)
		if !alreadyAttached {
			go pc.RequestSubscriberOffer()
		}
	}
}

// subscriberReadyForAutomaticAttach reports whether a subscriber PC has a
// stable browser exchange. Connected PCs are ready; a connecting PC is also
// ready once its browser offer has been answered. A brand-new PC must wait so
// an automatic server offer cannot overtake the browser's initial offer.
func (h *Handler) subscriberReadyForAutomaticAttach(pc *webrtc.PeerConnection) bool {
	if pc == nil {
		return false
	}
	if pc.State() == webrtc.PeerConnectionStateConnected {
		return true
	}
	if pc.State() == webrtc.PeerConnectionStateClosed || pc.State() == webrtc.PeerConnectionStateFailed {
		return false
	}
	pionPC := pc.PionPeerConnection()
	return pionPC != nil && pionPC.RemoteDescription() != nil && pionPC.SignalingState() == pionwebrtc.SignalingStateStable
}

// attachSubscribedReadyTracks flushes subscriptions that were recorded while
// a participant's initial browser offer was in flight. It is called after the
// browser offer is answered; only authoritative remote-backed forwarders are
// attached, so no provisional codec can enter the subscriber SDP.
func (h *Handler) attachSubscribedReadyTracks(room *domain.Room, subscriber *domain.Participant) {
	if room == nil || subscriber == nil {
		return
	}
	pc := h.getPeerConnection(subscriber.ID())
	if !h.subscriberReadyForAutomaticAttach(pc) {
		return
	}
	for _, trackID := range subscriber.SubscribedTracks() {
		track := room.GetTrack(trackID)
		if track == nil || track.Publisher() == nil || track.Publisher().ID() == subscriber.ID() {
			continue
		}
		fw := h.getForwarder(trackID)
		if fw == nil || fw.PublisherTrack() == nil || !fw.PublisherTrack().IsRemote() {
			continue
		}
		alreadyAttached := fw.HasSubscriber(pc)
		if err := fw.AddSubscriber(pc); err != nil {
			h.logger.Warn("deferred subscriber attach failed",
				"event", "deferred_subscriber_attach_failed",
				"track_id", trackID,
				"publisher_id", track.Publisher().ID(),
				"subscriber_id", subscriber.ID(),
				"error", err,
			)
			continue
		}
		if !alreadyAttached {
			h.logger.Info("deferred subscriber attached",
				"event", "deferred_subscriber_attached",
				"track_id", trackID,
				"publisher_id", track.Publisher().ID(),
				"subscriber_id", subscriber.ID(),
			)
			go pc.RequestSubscriberOffer()
		}
	}
}

// handleUnpublishTrack handles an unpublish track request.
func (h *Handler) handleUnpublishTrack(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req UnpublishTrackRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Unpublish the track
	if err := participant.UnpublishTrack(req.TrackID); err != nil {
		return err
	}

	// Keep the room registry coherent with the participant's publication
	// state; an entry that is already gone is tolerated as idempotent.
	if err := room.UnpublishTrack(req.TrackID); err != nil && err != domain.ErrTrackNotFound {
		return err
	}

	// SFU teardown: stop forwarder and clean subscriber PCs.
	h.removeForwarder(req.TrackID)
	// Also remove local track from publisher PC if present (forwarder.Stop already did, but handle idempotent case).
	if pc := h.getPeerConnection(participant.ID()); pc != nil {
		_ = pc.RemoveTrack(req.TrackID)
	}

	// Send response
	resp := TrackUnpublishedResponse{
		TrackID:       req.TrackID,
		ParticipantID: req.ParticipantID,
		Status:        "success",
	}

	if err := conn.Send(MessageTypeTrackUnpublished, resp); err != nil {
		return err
	}

	roomID := room.ID()
	participantID := participant.ID()
	trackID := req.TrackID
	// Kind unknown after unpublish; log without kind but keep required keys.
	h.logger.Info("track unavailable",
		"event", "track_unavailable",
		"room_id", roomID,
		"participant_id", participantID,
		"track_id", trackID,
		"kind", "",
	)

	// Notify other participants
	h.broadcastTrackUnavailable(room, participant, req.TrackID)

	return nil
}

// handleSubscribeTrack handles a subscribe track request.
func (h *Handler) handleSubscribeTrack(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req SubscribeTrackRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}
	h.logger.Info("subscribe request",
		"event", "subscribe_request",
		"participant_id", participant.ID(),
		"track_id", req.TrackID,
	)

	// Subscribe through the room registry: the room owns track lookup and
	// keeps subscriber bookkeeping consistent for the whole room. Domain
	// errors (unknown track, closed room, ...) are mapped to error codes by
	// the generic message loop.
	if err := room.SubscribeToTrack(participant, req.TrackID); err != nil {
		return err
	}
	subscribedTrack := participant.GetSubscribedTrack(req.TrackID)
	publisherID := ""
	if subscribedTrack != nil {
		if publisher := subscribedTrack.Publisher(); publisher != nil {
			publisherID = publisher.ID()
		}
	}

	// SFU wiring: add subscriber PC to forwarder.
	subPC := h.getPeerConnection(participant.ID())
	if subPC == nil {
		_ = participant.UnsubscribeTrack(req.TrackID)
		return webrtc.ErrNoPeerConnection
	}
	fw := h.getForwarder(req.TrackID)
	if fw == nil {
		// Lazy forwarder creation from publisher's local track (publisher may have
		// published via domain before SFU wiring existed, or forwarder was pruned
		// after last unsubscribe).
		if domTrack := room.GetTrack(req.TrackID); domTrack != nil {
			if pub := domTrack.Publisher(); pub != nil {
				if pubPC := h.getPeerConnection(pub.ID()); pubPC != nil {
					if webTrack := pubPC.GetLocalTrack(req.TrackID); webTrack != nil {
						var err error
						fw, err = h.getOrCreateForwarder(req.TrackID, webTrack)
						if err != nil {
							_ = participant.UnsubscribeTrack(req.TrackID)
							return err
						}
					}
				}
				// Fallback: publisher track has no WebRTCTrack yet; create a
				// standalone publisher track so subscribers can still attach.
				if fw == nil {
					webTrack, err := h.createPublisherWebRTCTrack(domTrack)
					if err == nil {
						fw, _ = h.getOrCreateForwarder(req.TrackID, webTrack)
					}
				}
			}
		}
	}
	if fw == nil {
		_ = participant.UnsubscribeTrack(req.TrackID)
		return webrtc.ErrTrackNotReady
	}
	// Any live subscriber can wait for the publisher's real TrackRemote.
	// Attaching the placeholder here would negotiate a guessed codec (VP8 for
	// video); when the publisher later arrives as H264 the egress sender has to
	// be removed and recreated, which browsers observe as a duplicate remote
	// track and may leave muted with no RTP. Keep the domain subscription, then
	// attach the authoritative egress after the browser exchange is stable.
	if deferred, reason := h.shouldDeferProvisionalSubscription(req.TrackID, fw, subPC); deferred {
		h.logger.Info("subscription deferred until publisher track is ready",
			"event", "subscription_deferred",
			"track_id", req.TrackID,
			"publisher_id", publisherID,
			"subscriber_id", participant.ID(),
			"reason", reason,
		)
		resp := TrackSubscribedResponse{
			TrackID:     req.TrackID,
			PublisherID: publisherID,
			Status:      "success",
		}
		return conn.Send(MessageTypeTrackSubscribed, resp)
	}
	alreadyAttached := fw.HasSubscriber(subPC)
	if err := fw.AddSubscriber(subPC); err != nil {
		_ = participant.UnsubscribeTrack(req.TrackID)
		return err
	}
	// Sync drive: push the subscriber offer through the negotiation gate so a
	// first subscription never depends solely on pion's asynchronous
	// negotiation-needed callback (pion aborts that callback when the PC is
	// not stable, and its chain-empty re-drive is timing-dependent). Run off
	// the message loop: the gate's offer waits for ICE gathering, which must
	// never stall this participant's message processing. When the gate is
	// closed the need stays coalesced in pendingNegotiation and the
	// completion paths flush exactly one offer; the generation guard
	// deduplicates the async echo of the same AddTrack.
	if !alreadyAttached {
		go subPC.RequestSubscriberOffer()
	}
	// Ask the publisher encoder for a fresh keyframe now that a subscriber
	// is attached (no-op until the real TrackRemote exists). This shortens
	// time-to-first-decodable-frame; the subscriber-PLI relay covers the
	// steady state.
	h.requestUpstreamKeyframe(req.TrackID)

	// Send response
	resp := TrackSubscribedResponse{
		TrackID:     req.TrackID,
		PublisherID: publisherID,
		Status:      "success",
	}

	return conn.Send(MessageTypeTrackSubscribed, resp)
}

// shouldDeferProvisionalSubscription reports whether a connected subscriber
// should wait for the publisher's real remote track instead of attaching the
// eager placeholder forwarder. Domain-only tests and server-local publishers
// intentionally retain the legacy provisional path; they have no browser
// publisher exchange from which an OnTrack callback can complete the attach.
func (h *Handler) shouldDeferProvisionalSubscription(trackID string, fw *webrtc.TrackForwarder, subPC *webrtc.PeerConnection) (bool, string) {
	if fw == nil || subPC == nil || !subPC.State().Usable() {
		return false, ""
	}
	// Before the subscriber's first browser offer has been answered, the
	// provisional track is still needed to describe the subscription in that
	// initial negotiation. Once that exchange has started, wait for the
	// publisher's authoritative TrackRemote instead of attaching a placeholder
	// that can later produce a duplicate media section.
	if subPC.State() != webrtc.PeerConnectionStateConnected {
		pionSubscriber := subPC.PionPeerConnection()
		if pionSubscriber == nil || pionSubscriber.RemoteDescription() == nil {
			return false, ""
		}
	}
	publisherTrack := fw.PublisherTrack()
	if publisherTrack == nil || publisherTrack.IsRemote() {
		return false, ""
	}
	domainTrack := publisherTrack.DomainTrack()
	if domainTrack == nil || domainTrack.Publisher() == nil {
		return false, ""
	}
	publisherPC := h.getPeerConnection(domainTrack.Publisher().ID())
	if publisherPC == nil {
		return false, ""
	}
	// A local server track already has its authoritative source on the
	// publisher PC and must not wait for an OnTrack callback.
	if publisherPC.GetLocalTrack(trackID) != nil {
		return false, "publisher_local_track"
	}
	pionPC := publisherPC.PionPeerConnection()
	if pionPC == nil {
		return false, ""
	}
	if pionPC.LocalDescription() != nil || pionPC.RemoteDescription() != nil {
		return true, "publisher_description_set"
	}
	if pionPC.SignalingState() != pionwebrtc.SignalingStateStable {
		return true, "publisher_signaling_not_stable"
	}
	return false, ""
}

// handleUnsubscribeTrack handles an unsubscribe track request.
func (h *Handler) handleUnsubscribeTrack(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req UnsubscribeTrackRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	if err := room.UnsubscribeFromTrack(participant, req.TrackID); err != nil {
		return err
	}

	if pc := h.getPeerConnection(participant.ID()); pc != nil {
		h.removeSubscriberFromForwarder(req.TrackID, pc)
	}

	resp := TrackUnsubscribedResponse{
		TrackID:       req.TrackID,
		ParticipantID: req.ParticipantID,
		Status:        "success",
	}

	return conn.Send(MessageTypeTrackUnsubscribed, resp)
}

// handleOffer handles a WebRTC offer: the offer is applied to the target
// participant's peer connection (the server answers on its behalf) and the
// generated answer is sent back to the source connection.
//
// Failures are reported to the client through sendWebRTCError with a mapped
// error code instead of falling back to the generic message loop, which
// would label every failure internal_error.
func (h *Handler) handleOffer(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req OfferRequest
	if err := msg.UnmarshalData(&req); err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}

	// Validate the offer request
	if err := ValidateOfferRequest(&req); err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}

	// Find the target participant
	target := room.GetParticipant(req.TargetParticipantID)
	if target == nil {
		h.sendWebRTCError(conn, msg.Type, domain.ErrParticipantNotFound)
		return nil
	}

	// Get the target peer connection
	pc := h.getPeerConnection(req.TargetParticipantID)
	if pc == nil {
		h.sendWebRTCError(conn, msg.Type, ErrConnectionNotFound)
		return nil
	}

	offer, err := webrtc.NewSessionDescriptionFromString(req.SDP, pionwebrtc.SDPTypeOffer)
	if err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}

	// ProcessBrowserOffer runs the inbound answer unit through the
	// per-PC negotiation gate: stable offers are answered inline; an offer
	// arriving behind our own outstanding subscriber offer is stored and
	// answered once stable (deferred, never InvalidModificationError).
	answer, deferred, err := pc.ProcessBrowserOffer(offer)
	if err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}
	if deferred {
		// No error and no answer yet: the stored offer is answered with
		// priority when the PC returns to stable (delivered as
		// webrtc:answer over the participant's signaling sender).
		h.logger.Debug("browser offer deferred behind server negotiation",
			"event", "webrtc_offer_deferred",
			"participant_id", conn.ID(),
			"target", req.TargetParticipantID,
			"pc", pc.InstanceID(),
		)
		return nil
	}
	// DIAG-PCID: bind the answered offer to the exact PC object.
	h.logger.Debug("offer applied, answer generated",
		"event", "webrtc_offer_applied",
		"participant_id", conn.ID(),
		"target", req.TargetParticipantID,
		"pc", pc.InstanceID(),
		"sdp", webrtc.SDPSummary(answer.SDP()),
	)

	// Send the answer back to the source participant. Deferred negotiation
	// (stored browser offer, coalesced subscriber offer) is flushed only
	// after the answer is enqueued on this connection's FIFO send channel,
	// so the browser never receives an offer that overtakes the answer to
	// its own publisher offer.
	answerNotification := AnswerNotification{
		SourceParticipantID: req.TargetParticipantID,
		SDP:                 answer.SDP(),
	}

	if err := conn.Send(MessageTypeAnswer, answerNotification); err != nil {
		return err
	}
	h.attachSubscribedReadyTracks(room, target)
	pc.FlushDeferredNegotiation()
	return nil
}

// handleAnswer handles a WebRTC answer by installing it as the remote
// description of the target participant's peer connection. Errors are mapped
// and reported to the client via sendWebRTCError.
func (h *Handler) handleAnswer(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req AnswerRequest
	if err := msg.UnmarshalData(&req); err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}

	// Validate the answer request
	if err := ValidateAnswerRequest(&req); err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}

	// The answer is applied to the peer connection owned by its target.
	pc := h.getPeerConnection(req.TargetParticipantID)
	if pc == nil {
		h.sendWebRTCError(conn, msg.Type, ErrConnectionNotFound)
		return nil
	}

	answer, err := webrtc.NewSessionDescriptionFromString(req.SDP, pionwebrtc.SDPTypeAnswer)
	if err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}

	// Set the remote description on the target peer connection through the
	// negotiation gate (stable after applying; deferred work flushes).
	// LiveKit-style idempotency: duplicate answer while stable (retransmit)
	// is ignored instead of returning internal_error.
	if err := pc.ProcessBrowserAnswer(answer); err != nil {
		if errors.Is(err, webrtc.ErrNoPendingOffer) {
			// Check if already have same remote SDP (duplicate)
			if rd := pc.PionPeerConnection().RemoteDescription(); rd != nil && rd.SDP == answer.SDP() {
				h.logger.Debug("duplicate answer ignored (stable)",
					"event", "duplicate_answer_ignored",
					"participant_id", conn.ID(),
					"target", req.TargetParticipantID,
				)
				return nil
			}
			// Also ignore if already stable and answer is same as local? treat as idempotent.
			// Warn (not Debug): a non-matching answer on a stable PC with no
			// pending offer is almost always a mis-targeted answer (wrong
			// target_participant_id, e.g. answering the publisher instead of
			// the subscriber's own PC). Silent swallowing hid exactly that
			// client bug in production (2026-09-04), so make it visible.
			h.logger.Warn("answer on stable ignored",
				"event", "answer_on_stable_ignored",
				"participant_id", conn.ID(),
				"target", req.TargetParticipantID,
				"hint", "answer matched no pending offer; check target_participant_id",
				"error", err,
			)
			return nil
		}
		// Live-debug aid: the browser's own answer is the ground truth for
		// bind failures (ErrUnsupportedCodec) — log which codecs it chose.
		h.logger.Warn("browser answer rejected",
			"event", "webrtc_answer_rejected",
			"participant_id", conn.ID(),
			"target", req.TargetParticipantID,
			"pc", pc.InstanceID(),
			"sdp", webrtc.SDPSummary(answer.SDP()),
			"error", err,
		)
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}
	// DIAG-E2E: one-line answer summary so a live test can compare the
	// browser answer against the SFU subscriber offer (codecs/PT/direction).
	// DIAG-PCID: pc binds the answer to the exact object that installed it.
	h.logger.Info("webrtc answer applied",
		"event", "webrtc_answer_applied",
		"participant_id", conn.ID(),
		"target", req.TargetParticipantID,
		"pc", pc.InstanceID(),
		"sdp", webrtc.SDPSummary(answer.SDP()),
	)
	h.attachSubscribedReadyTracks(room, room.GetParticipant(req.TargetParticipantID))
	return nil
}

// handleICECandidate handles an ICE candidate by adding it to the target
// participant's peer connection (with bounded retries for transient
// failures). Errors are mapped and reported to the client via
// sendWebRTCError.
func (h *Handler) handleICECandidate(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req ICECandidateRequest
	if err := msg.UnmarshalData(&req); err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}

	// Validate the ICE candidate request
	if err := ValidateICECandidateRequest(&req); err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}

	// Get the target peer connection
	pc := h.getPeerConnection(req.TargetParticipantID)
	if pc == nil {
		h.sendWebRTCError(conn, msg.Type, ErrConnectionNotFound)
		return nil
	}

	candidate, err := webrtc.NewICECandidateFromString(req.Candidate)
	if err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}
	candidate.SetSDPMid(req.SDPMid)
	candidate.SetSDPMLineIndex(uint16(req.SDPMLineIndex))

	// Add the ICE candidate to the target peer connection with retry for transient failures
	if err := pc.AddICECandidateWithRetry(candidate); err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}
	// DIAG-E2E: proves which PC a client candidate was actually applied to —
	// the decisive check for mis-targeted subscriber ICE (target must be the
	// subscriber's own PC, i.e. the source of the server offer).
	h.logger.Debug("ice candidate applied",
		"event", "ice_candidate_applied",
		"participant_id", conn.ID(),
		"target", req.TargetParticipantID,
		"pc", pc.InstanceID(),
		"sdp_mid", req.SDPMid,
		"sdp_mline_index", req.SDPMLineIndex,
	)
	return nil
}

// getPeerConnection looks up the peer connection registered for a
// participant.
func (h *Handler) getPeerConnection(participantID string) *webrtc.PeerConnection {
	h.peerConnectionsMutex.RLock()
	defer h.peerConnectionsMutex.RUnlock()
	return h.peerConnections[participantID]
}

// sendWebRTCError replies to a WebRTC signaling request with an ErrorResponse
// carrying the mapped error code (see errorCodeFromError). The generic
// message loop would report any returned error as internal_error, so WebRTC
// handlers report through this helper and then return nil.
func (h *Handler) sendWebRTCError(conn *Connection, requestType MessageType, err error) {
	code := errorCodeFromError(err)
	h.logger.Warn("webrtc signaling operation failed",
		"event", "webrtc_operation_failed",
		"participant_id", conn.ID(),
		"request_type", string(requestType),
		"code", code,
		"error", err,
	)

	_ = conn.Send(MessageTypeError, ErrorResponse{
		Error:       err.Error(),
		Code:        code,
		RequestType: string(requestType),
	})
}

// handleConnectionClosed handles cleanup when the WebSocket transport drops.
//
// This is deliberately NOT a leave: the participant stays joined to the room
// and its peer connection stays registered and usable, so a reconnecting
// client resumes its media session — ensurePeerConnection swaps the signaling
// sender onto the new socket, or replaces the connection wholesale when it
// turned unusable meanwhile. Only the transport registry entry goes away,
// handled by handleConnection's defer RemoveIf. Explicit leave_room is the
// terminal path (see handleLeaveRoom/closePeerConnection).
func (h *Handler) handleConnectionClosed(room *domain.Room, participant *domain.Participant) {
	pc := h.getPeerConnection(participant.ID())

	pcState := "<none>"
	if pc != nil {
		pcState = pc.State().String()
	}

	roomID := room.ID()
	participantID := participant.ID()
	ttl := h.gcTTL.String()
	h.logger.Info("peer transport dropped; session kept alive for reconnect",
		"event", "peer_disconnected",
		"room_id", roomID,
		"participant_id", participantID,
		"state", pcState,
		"ttl", ttl,
	)

	h.armGhostTimer(roomID, participantID)
}

// armGhostTimer starts or resets the ghost reap timer for participantID.
func (h *Handler) armGhostTimer(roomID, participantID string) {
	if h.gcTTL <= 0 {
		return
	}
	h.gcMu.Lock()
	defer h.gcMu.Unlock()
	if t, exists := h.ghostTimers[participantID]; exists {
		t.Stop()
		delete(h.ghostTimers, participantID)
	}
	// Capture values for closure.
	rid := roomID
	pid := participantID
	h.ghostTimers[participantID] = time.AfterFunc(h.gcTTL, func() {
		h.reapGhost(rid, pid)
	})
}

// cancelGhostTimer stops and removes the ghost timer for participantID.
func (h *Handler) cancelGhostTimer(participantID string) {
	h.gcMu.Lock()
	defer h.gcMu.Unlock()
	if t, exists := h.ghostTimers[participantID]; exists {
		t.Stop()
		delete(h.ghostTimers, participantID)
	}
}

// reapGhost reaps a ghost participant whose transport dropped and never
// reconnected within gcTTL. It is idempotent and respects lock ordering
// gcMu > peerConnectionsMutex > trackForwardersMutex > Room.mu.
func (h *Handler) reapGhost(roomID, participantID string) {
	// Remove timer under gcMu.
	h.gcMu.Lock()
	if t, exists := h.ghostTimers[participantID]; exists {
		// Timer already fired; just delete entry. Stop is optional.
		if t != nil {
			t.Stop()
		}
		delete(h.ghostTimers, participantID)
	} else {
		// If timer not found, continue idempotently; another reap or cancel may have run.
	}
	h.gcMu.Unlock()

	room := h.roomManager.GetRoom(roomID)

	var publishedTracks []string
	var leaverPC *webrtc.PeerConnection

	if room != nil {
		if p := room.GetParticipant(participantID); p != nil {
			publishedTracks = p.PublishedTracks()
		}
	}

	leaverPC = h.getPeerConnection(participantID)

	if room != nil {
		if err := room.Leave(participantID); err != nil && err != domain.ErrParticipantNotFound {
			h.logger.Warn("ghost reap: room.Leave failed",
				"event", "ghost_reap_leave_failed",
				"participant_id", participantID,
				"room_id", roomID,
				"error", err,
			)
		}
		// If room nil, participant not in room: still clean forwarders/PC below.
		_ = room // avoid unused if needed
	}

	for _, trackID := range publishedTracks {
		h.removeForwarder(trackID)
	}
	if leaverPC != nil {
		h.removeSubscriberFromAllForwarders(leaverPC)
	}
	h.closePeerConnection(participantID)

	atomic.AddUint64(&h.gcReapedCount, 1)

	h.logger.Info("ghost participant reaped",
		"event", "ghost_reaped",
		"participant_id", participantID,
		"room_id", roomID,
		"published_tracks", len(publishedTracks),
		"ttl", h.gcTTL.String(),
	)
}

// sendRoomJoined sends a room joined response.
func (h *Handler) sendRoomJoined(conn *Connection, room *domain.Room, participant *domain.Participant) error {
	// Collect participant info
	participants := make([]ParticipantInfo, 0)
	for _, pID := range room.Participants() {
		p := room.GetParticipant(pID)
		if p != nil {
			participants = append(participants, ParticipantInfo{
				ID:   p.ID(),
				Name: p.Name(),
			})
		}
	}

	resp := RoomJoinedResponse{
		RoomID:        room.ID(),
		ParticipantID: participant.ID(),
		Participants:  participants,
		Tracks:        h.publishedTracksForJoiner(room, participant.ID()),
		Status:        "success",
	}

	return conn.Send(MessageTypeRoomJoined, resp)
}

// publishedTracksForJoiner snapshots the room's authoritative published-track
// registry for a joining participant (late-joiner discovery). It returns one
// entry per published track owned by OTHER participants: the joiner's own
// tracks are excluded, and unpublished tracks never appear (UnpublishTrack
// removes them from the registry; state is re-checked defensively).
// Tracks without a resolvable publisher are skipped — there is no upstream
// RTCP/RTP path for them. Read-only; takes no handler locks beyond the
// room's own RLock discipline.
func (h *Handler) publishedTracksForJoiner(room *domain.Room, joinerID string) []PublishedTrackInfo {
	tracks := make([]PublishedTrackInfo, 0)
	for _, trackID := range room.Tracks() {
		track := room.GetTrack(trackID)
		if track == nil || track.State() != domain.TrackStatePublished {
			continue
		}
		pub := track.Publisher()
		if pub == nil || pub.ID() == "" || pub.ID() == joinerID {
			continue
		}
		tracks = append(tracks, PublishedTrackInfo{
			ParticipantID: pub.ID(),
			Track: TrackInfo{
				ID:     track.ID(),
				Kind:   track.Kind().String(),
				Source: track.Source().String(),
			},
		})
	}
	return tracks
}

// broadcastParticipantJoined broadcasts a participant joined notification to all other participants in the room.
func (h *Handler) broadcastParticipantJoined(room *domain.Room, participant *domain.Participant) {
	notification := ParticipantJoinedNotification{
		Participant: ParticipantInfo{
			ID:   participant.ID(),
			Name: participant.Name(),
		},
	}

	h.broadcastToRoom(room, MessageTypeParticipantJoined, notification, participant.ID())
}

// broadcastParticipantLeft broadcasts a participant left notification to all other participants in the room.
func (h *Handler) broadcastParticipantLeft(room *domain.Room, participant *domain.Participant) {
	notification := ParticipantLeftNotification{
		ParticipantID: participant.ID(),
	}

	h.broadcastToRoom(room, MessageTypeParticipantLeft, notification, participant.ID())
}

// broadcastTrackAvailable broadcasts a track available notification to all other participants in the room.
func (h *Handler) broadcastTrackAvailable(room *domain.Room, participant *domain.Participant, track *domain.Track) {
	notification := TrackAvailableNotification{
		ParticipantID: participant.ID(),
		Track: TrackInfo{
			ID:     track.ID(),
			Kind:   track.Kind().String(),
			Source: track.Source().String(),
		},
	}

	h.broadcastToRoom(room, MessageTypeTrackAvailable, notification, participant.ID())
}

// broadcastTrackUnavailable broadcasts a track unavailable notification to all other participants in the room.
func (h *Handler) broadcastTrackUnavailable(room *domain.Room, participant *domain.Participant, trackID string) {
	notification := TrackUnavailableNotification{
		ParticipantID: participant.ID(),
		TrackID:       trackID,
	}

	h.broadcastToRoom(room, MessageTypeTrackUnavailable, notification, participant.ID())
}

// broadcastToRoom sends a message to all participants in a room except the excluded participant.
func (h *Handler) broadcastToRoom(room *domain.Room, msgType MessageType, data interface{}, excludeParticipantID string) {
	for _, pID := range room.Participants() {
		if pID == excludeParticipantID {
			continue
		}

		conn := h.connectionManager.Get(pID)
		if conn != nil {
			_ = conn.Send(msgType, data)
		}
	}
}

// Connection-related errors.
var (
	ErrConnectionNotFound = fmt.Errorf("connection not found")
)
