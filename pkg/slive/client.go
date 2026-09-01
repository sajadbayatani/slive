package slive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/config"
	"github.com/sajadbayatani/slive/internal/domain"
	slvhttp "github.com/sajadbayatani/slive/internal/http"
	"github.com/sajadbayatani/slive/internal/signaling"
	"github.com/sajadbayatani/slive/internal/webrtc"
)

// In-process signaling server wiring: the SDK never pulls net/http/httptest
// into a consumer's binary, so the signaling endpoint is served by a plain
// http.Server on a loopback listener.
const (
	// signalingLoopbackBind is loopback-only by contract: the in-process
	// server is a test/SDK convenience, never a way to expose signaling on a
	// routable interface.
	signalingLoopbackBind = "127.0.0.1:0"
	// signalingReadHeaderTimeout bounds header reads on the in-process server.
	signalingReadHeaderTimeout = 5 * time.Second
	// signalingShutdownTimeout bounds the graceful drain of the in-process
	// server before it is force-closed, so Close stays prompt.
	signalingShutdownTimeout = 2 * time.Second
)

// Client is the SDK entry point. It owns a RoomManager and a signaling
// Handler so callers can JoinRoom, PublishTrack, SubscribeTrack and observe
// health via Snapshot without importing internal packages.
//
// Client is safe for concurrent use. Close is idempotent.
type Client struct {
	mu          sync.Mutex
	cfg         SDKConfig
	roomManager *RoomManager
	handler     *Handler
	closed      bool
	// srv is the lazily started in-process HTTP server that mounts the
	// signaling WebSocket endpoint; Session dialers connect through it.
	srv *http.Server
	// srvLn is the loopback listener behind srv; it is what makes the server
	// URL and lets Close release the bound port.
	srvLn net.Listener
	// sessions tracks open signaling sessions so Close can tear them down
	// before shutting the handler (gorilla ws keeps the listener busy).
	sessions map[*Session]struct{}
	// closedRooms records roomIDs that have been closed via CloseRoom for
	// idempotent semantics: a second CloseRoom on the same ID returns nil
	// while an unknown ID still returns ErrRoomNotFound.
	closedRooms map[string]struct{}
}

// NewClient creates a new Client with the given SDKConfig. Zero values are
// normalized to defaults: GCParticipantTTL defaults to
// config.DefaultGCParticipantTTL (60s), QueueSize defaults to DefaultQueueSize
// (64) and is plumbed to the signaling handler's forwarder config, and nil
// Logger uses slog.Default().
// STUNServers nil uses the signaling default; pass an empty slice to force
// STUN-free (useful for offline tests).
func NewClient(cfg SDKConfig) (*Client, error) {
	if cfg.GCParticipantTTL == 0 {
		cfg.GCParticipantTTL = DefaultSDKConfig().GCParticipantTTL
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	rm := signaling.NewRoomManager()

	// Build peer connection config from STUNServers; empty slice means
	// STUN-free, nil means default.
	var pcCfg webrtc.PeerConnectionConfig
	if cfg.STUNServers != nil {
		if len(cfg.STUNServers) == 0 {
			pcCfg = webrtc.PeerConnectionConfig{
				SDPSemantics: webrtc.DefaultPeerConnectionConfig().SDPSemantics,
				ICEServers:   []pionwebrtc.ICEServer{},
				Logger:       cfg.Logger,
			}
		} else {
			servers := make([]pionwebrtc.ICEServer, 0, len(cfg.STUNServers))
			for _, url := range cfg.STUNServers {
				servers = append(servers, pionwebrtc.ICEServer{URLs: []string{url}})
			}
			pcCfg = webrtc.PeerConnectionConfig{
				SDPSemantics: webrtc.DefaultPeerConnectionConfig().SDPSemantics,
				ICEServers:   servers,
				Logger:       cfg.Logger,
			}
		}
	} else {
		pcCfg = webrtc.DefaultPeerConnectionConfig()
		pcCfg.Logger = cfg.Logger
	}

	fwdCfg := webrtc.ForwarderConfig{QueueSize: cfg.QueueSize}

	h := signaling.NewHandler(rm,
		signaling.WithGCTTL(cfg.GCParticipantTTL),
		signaling.WithPeerConnectionConfig(pcCfg),
		signaling.WithLogger(cfg.Logger),
		signaling.WithForwarderConfig(fwdCfg),
		signaling.WithAllowedOrigins(cfg.AllowedOrigins),
	)

	return &Client{
		cfg:         cfg,
		roomManager: rm,
		handler:     h,
	}, nil
}

// JoinRoom joins or creates the room with roomID and adds a participant with
// participantID. It returns the Room on success. The context is checked before
// blocking operations.
//
// The join is idempotent: an already-present participantID returns the same
// *Room without re-joining, including when several goroutines race for the
// same room and participant.
func (c *Client) JoinRoom(ctx context.Context, roomID, participantID string) (*Room, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if roomID == "" || participantID == "" {
		return nil, fmt.Errorf("%w: roomID and participantID are required", ErrInvalidArgument)
	}

	// The whole create-room / check-participant / join sequence is one
	// critical section, and the closed flag is re-checked inside it.
	// Releasing the lock between the GetParticipant probe and room.Join made
	// the second joiner lose with domain.ErrParticipantAlreadyExists, which
	// contradicts the documented idempotency (B-4).
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("%w: client is closed", ErrClientClosed)
	}

	room, err := c.roomManager.GetOrCreateRoom(roomID)
	if err != nil {
		return nil, err
	}
	// Clear idempotent CloseRoom record so a re-created room can be closed again.
	if c.closedRooms != nil {
		delete(c.closedRooms, roomID)
	}

	// If participant already exists, return the room as-is (idempotent join).
	if existing := room.GetParticipant(participantID); existing != nil {
		return room, nil
	}

	p := domain.NewParticipant(participantID, "Participant "+participantID)
	if err := room.Join(p); err != nil {
		// Another path (the signaling Handler, or a join that lost the race
		// before this section existed) already holds the ID: that is the
		// success case the contract promises, not an error.
		if errors.Is(err, domain.ErrParticipantAlreadyExists) {
			return room, nil
		}
		return nil, err
	}
	p.SetRoom(room)

	return room, nil
}

// LeaveRoom removes participantID from roomID. A missing room reports
// ErrRoomNotFound, which does not match ErrParticipantNotFound; a missing
// participant reports ErrParticipantNotFound. It is otherwise idempotent in
// the sense that leaving twice returns ErrParticipantNotFound the second time.
func (c *Client) LeaveRoom(ctx context.Context, roomID, participantID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	room := c.roomManager.GetRoom(roomID)
	if room == nil {
		return ErrRoomNotFound
	}
	return room.Leave(participantID)
}

// PublishTrack publishes a track with trackID owned by participantID in roomID.
// kind and source must be valid. A missing room reports ErrRoomNotFound, which
// does not match ErrParticipantNotFound; a missing participant reports
// ErrParticipantNotFound.
func (c *Client) PublishTrack(ctx context.Context, roomID, participantID, trackID string, kind TrackKind, source TrackSource) (*Track, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	room := c.roomManager.GetRoom(roomID)
	if room == nil {
		return nil, ErrRoomNotFound
	}
	participant := room.GetParticipant(participantID)
	if participant == nil {
		return nil, ErrParticipantNotFound
	}

	track, err := domain.NewTrack(trackID, kind, source)
	if err != nil {
		return nil, err
	}
	if err := participant.PublishTrack(track); err != nil {
		return nil, err
	}
	if err := room.PublishTrack(track); err != nil {
		// Roll back participant publish on room failure.
		_ = participant.UnpublishTrack(trackID)
		return nil, err
	}
	return track, nil
}

// SubscribeTrack subscribes participantID to trackID in roomID. A missing room
// reports ErrRoomNotFound, which does not match ErrParticipantNotFound; a
// missing participant reports ErrParticipantNotFound.
func (c *Client) SubscribeTrack(ctx context.Context, roomID, participantID, trackID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	room := c.roomManager.GetRoom(roomID)
	if room == nil {
		return ErrRoomNotFound
	}
	participant := room.GetParticipant(participantID)
	if participant == nil {
		return ErrParticipantNotFound
	}
	return room.SubscribeToTrack(participant, trackID)
}

// UnsubscribeTrack unsubscribes participantID from trackID in roomID. A missing
// room reports ErrRoomNotFound, which does not match ErrParticipantNotFound; a
// missing participant reports ErrParticipantNotFound.
func (c *Client) UnsubscribeTrack(ctx context.Context, roomID, participantID, trackID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	room := c.roomManager.GetRoom(roomID)
	if room == nil {
		return ErrRoomNotFound
	}
	participant := room.GetParticipant(participantID)
	if participant == nil {
		return ErrParticipantNotFound
	}
	return room.UnsubscribeFromTrack(participant, trackID)
}

// Snapshot returns a point-in-time metrics snapshot. It never holds handler
// locks during encoding and is safe to call concurrently.
func (c *Client) Snapshot() MetricsSnapshot {
	if c.handler == nil {
		return MetricsSnapshot{}
	}
	return c.handler.Snapshot()
}

// RoomManager returns the underlying RoomManager. It is exposed for advanced
// use without leaking the signaling import path; prefer Client methods for
// normal flows.
func (c *Client) RoomManager() *RoomManager {
	return c.roomManager
}

// Handler returns the underlying signaling Handler.
//
// Deprecated: Use HTTPHandler, Connect, or the new lifecycle methods RoomIDs and CloseRoom instead.
// The Handler is an unstable alias for signaling.Handler whose exported test hooks are gated behind
// //go:build slive_internal and whose surface may change in any release. Prefer Client methods.
func (c *Client) Handler() *Handler {
	return c.handler
}

// RoomIDs returns a snapshot of active room IDs, sorted deterministically.
// It is safe for concurrent use.
func (c *Client) RoomIDs() []string {
	if c.roomManager == nil {
		return nil
	}
	ids := c.roomManager.RoomIDs()
	sort.Strings(ids)
	return ids
}

// CloseRoom closes the room with roomID via the canonical teardown: per-participant
// Leave, forwarder stops, and manager removal. It returns ErrRoomNotFound for
// unknown rooms and is idempotent — closing an already-closed room returns nil.
// The context is checked before blocking operations.
func (c *Client) CloseRoom(ctx context.Context, roomID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if roomID == "" {
		return fmt.Errorf("%w: roomID is required", ErrInvalidArgument)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("%w: client is closed", ErrClientClosed)
	}
	c.mu.Unlock()

	room := c.roomManager.GetRoom(roomID)
	if room == nil {
		c.mu.Lock()
		if c.closedRooms != nil {
			if _, ok := c.closedRooms[roomID]; ok {
				c.mu.Unlock()
				return nil
			}
		}
		c.mu.Unlock()
		return ErrRoomNotFound
	}
	// If room was previously closed and re-created, clear the idempotent record so
	// the new instance can be closed again.
	c.mu.Lock()
	if c.closedRooms != nil {
		delete(c.closedRooms, roomID)
	}
	c.mu.Unlock()

	// Snapshot participants and tracks before teardown.
	participantIDs := room.Participants()
	trackIDs := room.Tracks()

	// Clean up signaling resources if handler is present.
	if c.handler != nil {
		if err := c.handler.CloseRoom(roomID); err != nil {
			if errors.Is(err, signaling.ErrRoomNotFound) || errors.Is(err, domain.ErrParticipantNotFound) {
				// We already verified room existed via GetRoom, so this is a concurrent
				// close race — treat as idempotent success.
				c.mu.Lock()
				if c.closedRooms == nil {
					c.closedRooms = make(map[string]struct{})
				}
				c.closedRooms[roomID] = struct{}{}
				c.mu.Unlock()
				return nil
			}
			return err
		}
		// Handler.CloseRoom already removed the room from manager, so record idempotency.
		c.mu.Lock()
		if c.closedRooms == nil {
			c.closedRooms = make(map[string]struct{})
		}
		c.closedRooms[roomID] = struct{}{}
		c.mu.Unlock()
		return nil
	}

	// No handler path: per-participant Leave via Room, then manager removal.
	for _, pid := range participantIDs {
		_ = room.Leave(pid)
	}
	_ = trackIDs
	if err := c.roomManager.CloseRoom(roomID); err != nil {
		if errors.Is(err, signaling.ErrRoomNotFound) || errors.Is(err, domain.ErrParticipantNotFound) {
			c.mu.Lock()
			if c.closedRooms == nil {
				c.closedRooms = make(map[string]struct{})
			}
			// If we had seen the room, this is a race — idempotent.
			if _, ok := c.closedRooms[roomID]; ok {
				c.mu.Unlock()
				return nil
			}
			c.mu.Unlock()
			return ErrRoomNotFound
		}
		return err
	}
	c.mu.Lock()
	if c.closedRooms == nil {
		c.closedRooms = make(map[string]struct{})
	}
	c.closedRooms[roomID] = struct{}{}
	c.mu.Unlock()
	return nil
}

// HTTPHandler returns an http.Handler that mounts the client's real HTTP
// surface: the health/diagnostics endpoints (/health and /healthz, wired to
// Client.Snapshot) and the signaling WebSocket endpoint on /ws. It composes
// the same router production uses, so examples and tests can serve it with
// httptest.NewServer without importing internal/http.
func (c *Client) HTTPHandler() http.Handler {
	router := slvhttp.NewRouter(config.Config{
		HealthPath:    config.DefaultHealthPath,
		WebSocketPath: config.DefaultWebSocketPath,
	}, slvhttp.HandlerDeps{
		SignalingHandler: c.handler,
		MetricsSnapshot:  func() MetricsSnapshot { return c.Snapshot() },
	})
	return router.ServeMux()
}

// SignalingURL starts (once) an in-process HTTP server hosting
// HTTPHandler on a loopback address and returns its base URL. Sessions dial
// through it; Close shuts the server down. Callers must not import
// net/http/httptest to wire signaling themselves — use Connect.
//
// The server is a plain net/http server on a 127.0.0.1 listener: the SDK does
// not link net/http/httptest, so no test-only server semantics are part of the
// stable surface. The bind address is loopback-only and the endpoint is
// in-process.
func (c *Client) SignalingURL() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return "", fmt.Errorf("%w: client is closed", ErrClientClosed)
	}
	if c.srv == nil {
		ln, err := net.Listen("tcp", signalingLoopbackBind)
		if err != nil {
			return "", fmt.Errorf("listen signaling: %w", err)
		}
		srv := &http.Server{
			Handler:           c.HTTPHandler(),
			ReadHeaderTimeout: signalingReadHeaderTimeout,
		}
		c.srv, c.srvLn = srv, ln
		go func() {
			// Serve exits once closeSignalingServer closes the listener;
			// http.ErrServerClosed is the expected goodbye.
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				c.logger().Warn("signaling server stopped",
					"event", "signaling_server_stopped",
					"error", err,
				)
			}
		}()
	}
	return "http://" + c.srvLn.Addr().String(), nil
}

// logger returns the configured logger, falling back to the default so a
// Client built without NewClient cannot panic on a lifecycle log line.
func (c *Client) logger() *slog.Logger {
	if c.cfg.Logger != nil {
		return c.cfg.Logger
	}
	return slog.Default()
}

// registerSession tracks an open session for Close; it is package-internal.
// A client that closed in the meantime tears the session down and returns an
// error instead of re-creating the map after Close, which would leak the
// session and its transport past the client's lifetime.
func (c *Client) registerSession(s *Session) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = s.closeTransport()
		return fmt.Errorf("signaling session rejected: client is closed: %w", ErrSessionClosed)
	}
	if c.sessions == nil {
		c.sessions = make(map[*Session]struct{})
	}
	c.sessions[s] = struct{}{}
	c.mu.Unlock()
	return nil
}

// unregisterSession stops tracking a closed session.
func (c *Client) unregisterSession(s *Session) {
	c.mu.Lock()
	delete(c.sessions, s)
	c.mu.Unlock()
}

// closeSignalingServer drains and then force-closes the in-process signaling
// server and its listener. The graceful window is bounded so a handler parked
// on a connection the SDK no longer owns cannot stall Close, and the forced
// close is what releases hijacked WebSocket conns.
func (c *Client) closeSignalingServer(srv *http.Server, ln net.Listener) {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), signalingShutdownTimeout)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = srv.Close()
	if ln != nil {
		_ = ln.Close()
	}
}

// Close shuts down the client's handler and marks the client closed. It is
// idempotent and safe to call concurrently with other methods, including
// concurrently with a Session round-trip that is parked waiting for a peer:
// session teardown only needs the transport lock, so it never queues behind a
// pending read. Open signaling sessions are closed first, then the in-process
// signaling server, then the handler.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	h := c.handler
	srv := c.srv
	ln := c.srvLn
	sessions := make([]*Session, 0, len(c.sessions))
	for s := range c.sessions {
		sessions = append(sessions, s)
	}
	c.sessions = nil
	c.mu.Unlock()

	for _, s := range sessions {
		_ = s.closeTransport()
	}
	c.closeSignalingServer(srv, ln)

	if h != nil {
		return h.Shutdown()
	}
	return nil
}
