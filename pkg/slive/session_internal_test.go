package slive

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sajadbayatani/slive/internal/signaling"
)

// session_internal_test.go covers the Session transport internals that need
// reach into unexported state: the wedged-peer teardown (B-5), error-frame
// request correlation (#3) and the ErrorResponse → sentinel mapping (B-2).
// Everything here drives a hand-rolled loopback peer, so no signaling Handler
// is involved and the timing is fully under the test's control.

// newSTUNFreeTestClient returns a Client with STUN-free ICE so these tests stay
// offline and deterministic.
func newSTUNFreeTestClient(t *testing.T) *Client {
	t.Helper()
	cfg := DefaultSDKConfig()
	cfg.STUNServers = []string{}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// startWSPeer serves loopback WebSocket connections whose peer behaviour is
// serve. It returns the address to dial. The peer is torn down with the test.
func startWSPeer(t *testing.T, serve func(conn *websocket.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", signalingLoopbackBind)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		go serve(conn)
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = ln.Close()
	})
	return ln.Addr().String()
}

// dialPeer connects a raw signaling client to addr for a manual Session.
func dialPeer(t *testing.T, addr string) *websocket.Conn {
	t.Helper()
	dialer := &websocket.Dialer{Proxy: nil, HandshakeTimeout: 5 * time.Second}
	ws, resp, err := dialer.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		t.Fatalf("dial peer: %v", err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// drainPeer keeps a peer connection open, discarding everything the client
// sends, until the client goes away.
func drainPeer(conn *websocket.Conn) {
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

// writeErrorResponse sends a protocol-level error frame tagged with requestType.
func writeErrorResponse(conn *websocket.Conn, requestType signaling.MessageType, code, text string) error {
	msg, err := signaling.NewMessage(signaling.MessageTypeError, signaling.ErrorResponse{
		Error:       text,
		Code:        code,
		RequestType: string(requestType),
	})
	if err != nil {
		return err
	}
	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, data)
}

// awaitRoundTrip blocks until a concurrently running round-trip on s holds the
// round-trip lock, i.e. it is genuinely inside a socket operation. TryLock only
// fails while that lock is owned, so the test never races on an arbitrary sleep.
func awaitRoundTrip(t *testing.T, s *Session) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.mu.TryLock() {
			s.mu.Unlock()
			time.Sleep(2 * time.Millisecond)
			continue
		}
		return
	}
	t.Fatal("round-trip never took the mutex; the test did not wedge a read")
}

// TestClientCloseDoesNotHangOnWedgedSession is the B-5 regression: a session
// parked in ReadMessage against a peer that never answers used to hold the one
// mutex that closeTransport needed, so Client.Close blocked forever. The
// context deliberately carries a deadline far beyond the test bound, so only
// the transport lock split (not the round-trip timeout) can save Close.
func TestClientCloseDoesNotHangOnWedgedSession(t *testing.T) {
	addr := startWSPeer(t, drainPeer) // silent: reads, never replies

	client := newSTUNFreeTestClient(t)
	s := &Session{
		client:        client,
		roomID:        "room-wedged",
		participantID: "alice",
		ws:            dialPeer(t, addr),
	}
	if err := client.registerSession(s); err != nil {
		t.Fatalf("registerSession: %v", err)
	}

	awaitErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		awaitErr <- s.await(ctx, signaling.MessageTypePublishTrack, signaling.MessageTypeTrackPublished)
	}()
	awaitRoundTrip(t, s)

	closeErr := make(chan error, 1)
	go func() { closeErr <- client.Close() }()

	select {
	case err := <-closeErr:
		if err != nil {
			t.Errorf("Client.Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Client.Close never returned while a session round-trip was parked (B-5)")
	}

	select {
	case err := <-awaitErr:
		if !errors.Is(err, ErrSessionClosed) {
			t.Errorf("parked round-trip returned %v, want ErrSessionClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("parked round-trip was never released by Close")
	}

	if !s.isClosed() {
		t.Error("session is not closed after Client.Close")
	}
	if err := s.PublishTrack(context.Background(), "mic-1", TrackKindAudio, TrackSourceMicrophone); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("PublishTrack after teardown = %v, want ErrSessionClosed", err)
	}
}

// TestSessionCloseDoesNotHangOnWedgedRoundTrip is the same guarantee for
// Session.Close while the round-trip lock is held.
func TestSessionCloseDoesNotHangOnWedgedRoundTrip(t *testing.T) {
	addr := startWSPeer(t, drainPeer)

	client := newSTUNFreeTestClient(t)
	s := &Session{client: client, roomID: "room-wedged-2", participantID: "bob", ws: dialPeer(t, addr)}
	if err := client.registerSession(s); err != nil {
		t.Fatalf("registerSession: %v", err)
	}

	awaitErr := make(chan error, 1)
	go func() {
		awaitErr <- s.await(context.Background(), signaling.MessageTypeSubscribeTrack, signaling.MessageTypeTrackSubscribed)
	}()
	awaitRoundTrip(t, s)

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("Session.Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Session.Close blocked behind a parked round-trip")
	}

	select {
	case err := <-awaitErr:
		if err == nil {
			t.Error("parked round-trip returned nil after teardown")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("parked round-trip was never released by Session.Close")
	}
}

// TestSessionRoundTripBoundsSilentPeerWithoutContextDeadline proves the other
// half of B-5: a round-trip started with context.Background() against a peer
// that never answers still fails on the default bound instead of parking
// forever. It costs one defaultRoundTripTimeout, so it is skipped in -short.
func TestSessionRoundTripBoundsSilentPeerWithoutContextDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for defaultRoundTripTimeout")
	}
	addr := startWSPeer(t, drainPeer)

	client := newSTUNFreeTestClient(t)
	s := &Session{client: client, roomID: "room-silent", participantID: "carol", ws: dialPeer(t, addr)}

	start := time.Now()
	err := s.await(context.Background(), signaling.MessageTypePublishTrack, signaling.MessageTypeTrackPublished)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("await against a silent peer returned nil")
	}
	if elapsed > defaultRoundTripTimeout+3*time.Second {
		t.Errorf("round-trip took %v, want the %v default bound", elapsed, defaultRoundTripTimeout)
	}
	t.Logf("silent peer bounded after %v: %v", elapsed, err)
	if s.isClosed() {
		t.Error("a read timeout must not be reported as a closed session")
	}
}

// TestRoundTripDeadlineAlwaysPositive pins the invariant behind that bound: a
// zero time means "no timeout" to gorilla, so roundTripDeadline must never
// return it, whatever the context says.
func TestRoundTripDeadlineAlwaysPositive(t *testing.T) {
	now := time.Now()

	cases := []struct {
		name  string
		ctx   func() context.Context
		below time.Duration // must be at least this far out
		above time.Duration // must be no further than this
	}{
		{
			name:  "no-deadline",
			ctx:   func() context.Context { return context.Background() },
			below: defaultRoundTripTimeout / 2,
			above: defaultRoundTripTimeout + time.Second,
		},
		{
			name: "far-future-deadline",
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
				t.Cleanup(cancel)
				return ctx
			},
			below: time.Hour - time.Minute,
			above: time.Hour + time.Minute,
		},
		{
			name: "expired-deadline",
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				t.Cleanup(cancel)
				<-ctx.Done()
				return ctx
			},
			below: 0,
			above: minRoundTripTimeout + time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := roundTripDeadline(tc.ctx())
			if got.IsZero() {
				t.Fatal("roundTripDeadline returned the zero time: gorilla reads that as no timeout")
			}
			delta := got.Sub(now)
			if delta <= tc.below {
				t.Errorf("deadline %v is too close (delta %v, want > %v)", got, delta, tc.below)
			}
			if delta > tc.above {
				t.Errorf("deadline %v is too far out (delta %v, want <= %v)", got, delta, tc.above)
			}
		})
	}

	if d := handshakeTimeout(context.Background()); d <= 0 {
		t.Errorf("handshakeTimeout(no deadline) = %v, want positive", d)
	}
	expiredCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	<-expiredCtx.Done()
	if d := handshakeTimeout(expiredCtx); d <= 0 {
		t.Errorf("handshakeTimeout(expired ctx) = %v, want positive (0 means no timeout)", d)
	}
}

// TestSessionErrorFrameForOtherRequestTearsDownSession covers #3: the server
// tags each error with the request it answers. An error frame tagged for a
// different exchange must not be attributed to this call; the session is
// dropped instead.
func TestSessionErrorFrameForOtherRequestTearsDownSession(t *testing.T) {
	addr := startWSPeer(t, func(conn *websocket.Conn) {
		// Consume the publish request, then answer on behalf of a request this
		// session never made.
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		if err := writeErrorResponse(conn, signaling.MessageTypeSubscribeTrack,
			signaling.ErrorCodeTrackNotFound, "stale subscribe failure"); err != nil {
			return
		}
		drainPeer(conn)
	})

	client := newSTUNFreeTestClient(t)
	s := &Session{client: client, roomID: "room-correlation", participantID: "dave", ws: dialPeer(t, addr)}

	err := s.request(context.Background(), signaling.MessageTypePublishTrack,
		signaling.PublishTrackRequest{RoomID: "room-correlation", ParticipantID: "dave"},
		signaling.MessageTypeTrackPublished)
	if err == nil {
		t.Fatal("round-trip answered by a foreign error frame: want error, got nil")
	}
	t.Logf("correlation failure surfaced: %v", err)
	if !errors.Is(err, ErrSessionClosed) {
		t.Errorf("correlation failure %v does not match ErrSessionClosed; the session must be dropped", err)
	}
	if !strings.Contains(err.Error(), string(signaling.MessageTypeSubscribeTrack)) ||
		!strings.Contains(err.Error(), string(signaling.MessageTypePublishTrack)) {
		t.Errorf("correlation failure %v must name both the foreign tag and the awaited request", err)
	}
	if !s.isClosed() {
		t.Error("session was not torn down after a mis-correlated error frame")
	}
	if err := s.PublishTrack(context.Background(), "mic-1", TrackKindAudio, TrackSourceMicrophone); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("PublishTrack after teardown = %v, want ErrSessionClosed", err)
	}
}

// TestSessionErrorFrameForThisRequestIsAttributed is the control for #3: an
// error frame tagged with the pending request is a normal answer and must be
// mapped, not treated as a correlation failure (the session stays usable).
func TestSessionErrorFrameForThisRequestIsAttributed(t *testing.T) {
	addr := startWSPeer(t, func(conn *websocket.Conn) {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		if err := writeErrorResponse(conn, signaling.MessageTypePublishTrack,
			signaling.ErrorCodeInternalError, domainTextTrackAlreadyPublished); err != nil {
			return
		}
		drainPeer(conn)
	})

	client := newSTUNFreeTestClient(t)
	s := &Session{client: client, roomID: "room-correlated", participantID: "erin", ws: dialPeer(t, addr)}

	err := s.request(context.Background(), signaling.MessageTypePublishTrack,
		signaling.PublishTrackRequest{RoomID: "room-correlated", ParticipantID: "erin"},
		signaling.MessageTypeTrackPublished)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if errors.Is(err, ErrSessionClosed) {
		t.Errorf("a correctly tagged error must not tear the session down, got %v", err)
	}
	if !errors.Is(err, ErrTrackAlreadyPublished) {
		t.Errorf("error %v does not match ErrTrackAlreadyPublished", err)
	}
	if s.isClosed() {
		t.Error("session closed after an attributed error response")
	}
}

// domainTextTrackAlreadyPublished is the domain text the wire carries for a
// duplicate publish; internal/signaling has no dedicated code for it yet.
const domainTextTrackAlreadyPublished = "track already published by participant"

// TestSentinelForErrorReply is the unit table for the B-2 mapping: known codes
// are authoritative, known domain texts cover the codes internal/signaling
// still collapses into internal_error, and anything else claims no identity.
func TestSentinelForErrorReply(t *testing.T) {
	cases := []struct {
		name  string
		reply signaling.ErrorResponse
		want  error
	}{
		{"code-track-not-found", signaling.ErrorResponse{Code: signaling.ErrorCodeTrackNotFound, Error: "nope"}, ErrTrackNotFound},
		{"code-participant-not-found", signaling.ErrorResponse{Code: signaling.ErrorCodeParticipantNotFound, Error: "nope"}, ErrParticipantNotFound},
		{"code-room-closed", signaling.ErrorResponse{Code: signaling.ErrorCodeRoomClosed, Error: "nope"}, ErrRoomClosed},
		{"code-room-not-found", signaling.ErrorResponse{Code: signaling.ErrorCodeRoomNotFound, Error: "nope"}, ErrRoomNotFound},
		{"text-track-already-published", signaling.ErrorResponse{Code: signaling.ErrorCodeInternalError, Error: domainTextTrackAlreadyPublished}, ErrTrackAlreadyPublished},
		{"wrapped-text-track-already-published", signaling.ErrorResponse{Code: signaling.ErrorCodeInternalError, Error: "publish failed: " + domainTextTrackAlreadyPublished}, ErrTrackAlreadyPublished},
		{"text-participant-left", signaling.ErrorResponse{Code: signaling.ErrorCodeInternalError, Error: "participant has left the room"}, ErrParticipantLeft},
		{"code-beats-text", signaling.ErrorResponse{Code: signaling.ErrorCodeTrackNotFound, Error: domainTextTrackAlreadyPublished}, ErrTrackNotFound},
		{"unknown-code", signaling.ErrorResponse{Code: signaling.ErrorCodeInternalError, Error: "the server had a bad day"}, nil},
		{"empty-reply", signaling.ErrorResponse{}, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sentinelForErrorReply(tc.reply)
			if got != tc.want {
				t.Errorf("sentinelForErrorReply(%+v) = %v, want %v", tc.reply, got, tc.want)
			}
		})
	}
}

// TestSessionErrorKeepsMessageForUnknownReply pins the shape of the wrapped
// error: the wire text and code survive for humans while Unwrap claims nothing.
func TestSessionErrorKeepsMessageForUnknownReply(t *testing.T) {
	addr := startWSPeer(t, func(conn *websocket.Conn) {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		if err := writeErrorResponse(conn, signaling.MessageTypeSubscribeTrack,
			signaling.ErrorCodeInternalError, "the server had a bad day"); err != nil {
			return
		}
		drainPeer(conn)
	})

	client := newSTUNFreeTestClient(t)
	s := &Session{client: client, roomID: "room-unknown", participantID: "frank", ws: dialPeer(t, addr)}

	err := s.request(context.Background(), signaling.MessageTypeSubscribeTrack,
		signaling.SubscribeTrackRequest{RoomID: "room-unknown", ParticipantID: "frank", TrackID: "mic-1"},
		signaling.MessageTypeTrackSubscribed)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "the server had a bad day") {
		t.Errorf("error %q lost the server text", err)
	}
	if !strings.Contains(err.Error(), signaling.ErrorCodeInternalError) {
		t.Errorf("error %q lost the wire code", err)
	}
	var se *sessionError
	if !errors.As(err, &se) {
		t.Fatalf("error %v (%T) is not a *sessionError", err, err)
	}
	if se.Unwrap() != nil {
		t.Errorf("unknown reply must claim no sentinel, Unwrap() = %v", se.Unwrap())
	}
	for _, sentinel := range []error{ErrTrackNotFound, ErrParticipantNotFound, ErrRoomClosed, ErrRoomNotFound, ErrSessionClosed} {
		if errors.Is(err, sentinel) {
			t.Errorf("unknown reply must not match %v", sentinel)
		}
	}
}

// TestRegisterSessionAfterCloseTearsDownSession covers #7: registering against a
// closed client must not rebuild the session map (which leaked the session past
// Close); it drops the transport and reports an error instead.
func TestRegisterSessionAfterCloseTearsDownSession(t *testing.T) {
	client := newSTUNFreeTestClient(t)
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s := &Session{client: client, roomID: "room-late", participantID: "grace"}
	err := client.registerSession(s)
	if err == nil {
		t.Fatal("registerSession on a closed client: want error, got nil")
	}
	if !errors.Is(err, ErrSessionClosed) {
		t.Errorf("registerSession error %v does not match ErrSessionClosed", err)
	}
	if !s.isClosed() {
		t.Error("late session was not torn down")
	}

	client.mu.Lock()
	n := len(client.sessions)
	client.mu.Unlock()
	if n != 0 {
		t.Errorf("sessions map holds %d entries after Close; Close must not be followed by re-registration", n)
	}
}
