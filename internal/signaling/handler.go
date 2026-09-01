package signaling

import (
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

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
		}
	})

	return pc, nil
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
				if err := fw.UpdatePublisher(publisherTrack); err != nil {
					return nil, err
				}
			}
		}
		return fw, nil
	}
	h.trackForwardersMutex.RUnlock()

	h.trackForwardersMutex.Lock()
	defer h.trackForwardersMutex.Unlock()
	if fw := h.trackForwarders[trackID]; fw != nil {
		if publisherTrack.IsRemote() {
			if pt := fw.PublisherTrack(); pt != nil && !pt.IsRemote() {
				if err := fw.UpdatePublisher(publisherTrack); err != nil {
					return nil, err
				}
			}
		}
		return fw, nil
	}
	fw, err := webrtc.NewTrackForwarderWithConfig(publisherTrack, h.forwarderConfig)
	if err != nil {
		return nil, err
	}
	if err := fw.Start(); err != nil {
		return nil, err
	}
	h.trackForwarders[trackID] = fw
	return fw, nil
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
	var capability pionwebrtc.RTPCodecCapability
	switch domainTrack.Kind() {
	case domain.TrackKindAudio:
		capability = pionwebrtc.RTPCodecCapability{
			MimeType:    pionwebrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "",
		}
	default:
		capability = pionwebrtc.RTPCodecCapability{
			MimeType:  pionwebrtc.MimeTypeVP8,
			ClockRate: 90000,
		}
	}
	pionTrack, err := pionwebrtc.NewTrackLocalStaticRTP(capability, domainTrack.ID(), domainTrack.ID()+"-stream")
	if err != nil {
		return nil, err
	}
	codecParams := pionwebrtc.RTPCodecParameters{RTPCodecCapability: capability}
	return webrtc.NewWebRTCTrack(domainTrack, pionTrack, codecParams), nil
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
func (h *Handler) handleCreateRoom(conn *Connection, msg *Message) error {
	var req CreateRoomRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Create the room
	room, err := h.roomManager.CreateRoom(req.RoomID)
	if err != nil {
		return err
	}

	// Create the participant
	participant := domain.NewParticipant(req.ParticipantID, req.ParticipantName)
	if err := room.Join(participant); err != nil {
		return err
	}
	participant.SetRoom(room)

	// Send response
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
	if h.getForwarder(track.ID()) == nil {
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

	// Subscribe through the room registry: the room owns track lookup and
	// keeps subscriber bookkeeping consistent for the whole room. Domain
	// errors (unknown track, closed room, ...) are mapped to error codes by
	// the generic message loop.
	if err := room.SubscribeToTrack(participant, req.TrackID); err != nil {
		return err
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
	if err := fw.AddSubscriber(subPC); err != nil {
		_ = participant.UnsubscribeTrack(req.TrackID)
		return err
	}

	// Send response
	resp := TrackSubscribedResponse{
		TrackID:     req.TrackID,
		PublisherID: req.ParticipantID,
		Status:      "success",
	}

	return conn.Send(MessageTypeTrackSubscribed, resp)
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

	// CreateAnswer installs the remote offer before generating the answer.
	answer, err := pc.CreateAnswer(offer)
	if err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}

	// Send the answer back to the source participant
	answerNotification := AnswerNotification{
		SourceParticipantID: req.TargetParticipantID,
		SDP:                 answer.SDP(),
	}

	return conn.Send(MessageTypeAnswer, answerNotification)
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

	// Set the remote description on the source peer connection
	if err := pc.SetRemoteDescription(answer); err != nil {
		h.sendWebRTCError(conn, msg.Type, err)
		return nil
	}
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
		Status:        "success",
	}

	return conn.Send(MessageTypeRoomJoined, resp)
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
