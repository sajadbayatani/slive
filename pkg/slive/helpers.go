package slive

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sajadbayatani/slive/internal/config"
	"github.com/sajadbayatani/slive/internal/domain"
	"github.com/sajadbayatani/slive/internal/signaling"
)

// helpers.go provides the SDK client helpers from TASK-032: a thin
// WebSocket signaling Session so an SDK participant can run the real
// join/publish/subscribe protocol against the Client's Handler. The
// domain-only Client methods (JoinRoom/PublishTrack/SubscribeTrack) manage
// room state in-process, but SFU forwarders are owned by the signaling
// Handler and are only attached through its message loop; Session is the
// supported way to end up with real forwarder subscribers
// (MetricsSnapshot.ForwarderSubscribers) from the SDK.

// ErrSessionClosed is returned by Session methods after Close.
var ErrSessionClosed = errors.New("signaling session is closed")

// Round-trip bounds. Every socket operation runs against a positive deadline:
// gorilla treats a zero deadline as "no timeout", so a peer that accepts the
// connection and then goes silent would otherwise park the caller forever
// while it holds the round-trip lock (B-5). A caller-supplied context deadline
// still wins whenever it is usable.
const (
	// defaultRoundTripTimeout bounds one request/response round-trip when the
	// caller's context carries no deadline. It mirrors the dial handshake
	// bound, since the endpoint is in-process and replies promptly.
	defaultRoundTripTimeout = 5 * time.Second
	// minRoundTripTimeout keeps an expired (or nearly expired) context
	// deadline positive so the socket call fails fast instead of turning into
	// "no timeout".
	minRoundTripTimeout = 100 * time.Millisecond
	// closeControlTimeout bounds the best-effort close frame written during
	// teardown; it must never be able to stall Close.
	closeControlTimeout = time.Second
)

// Session is a WebSocket signaling connection for one participant. It
// performs the documented protocol handshake: connecting auto-joins the
// room (the server replies room_joined), then PublishTrack and
// SubscribeTrack round-trips create the peer connection, the track
// forwarder and its subscriber registration inside the Handler.
//
// A Session drives one signaling connection; methods serialize their
// request/response round-trips and are safe for concurrent use. It is not a
// media source: RTP arrives via WebRTC once clients negotiate, or is pushed
// by the server SFU.
//
// Two mutexes guard a Session. mu serializes request/response round-trips and
// can be held for as long as a read takes; closeMu guards the transport
// itself and is only ever held for a pointer snapshot. The split is what lets
// Session.Close and Client.Close drop the socket — and thereby release a
// parked read — without queueing behind the round-trip lock (B-5).
type Session struct {
	client        *Client
	roomID        string
	participantID string

	mu sync.Mutex // serializes request/response round-trips

	closeMu sync.Mutex // guards ws and closed; never held across a socket operation
	ws      *websocket.Conn
	closed  bool
}

// Connect opens a signaling Session for participantID in roomID against the
// client's in-process HTTP server (started on first use) and waits for the
// room_joined handshake. The participant is created and joined by the
// Handler if it does not exist yet; an existing in-process participant
// reuses its session state. Close the Session when done.
func (c *Client) Connect(ctx context.Context, roomID, participantID string) (*Session, error) {
	if roomID == "" || participantID == "" {
		return nil, fmt.Errorf("%w: roomID and participantID are required", ErrInvalidArgument)
	}
	base, err := c.SignalingURL()
	if err != nil {
		return nil, err
	}
	wsURL, err := websocketURL(base, roomID, participantID)
	if err != nil {
		return nil, err
	}

	// Dedicated dialer: never follow ambient HTTP(S)_PROXY settings, the
	// endpoint is always in-process.
	dialer := &websocket.Dialer{Proxy: nil, HandshakeTimeout: handshakeTimeout(ctx)}
	ws, resp, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("dial signaling: %w (status %d)", err, resp.StatusCode)
		}
		return nil, fmt.Errorf("dial signaling: %w", err)
	}

	s := &Session{client: c, roomID: roomID, participantID: participantID, ws: ws}
	// The handler auto-joins on connect and answers with room_joined. Errors
	// for that exchange are tagged join_room, the request type, so both halves
	// of the exchange are named here.
	if err := s.await(ctx, signaling.MessageTypeJoinRoom, signaling.MessageTypeRoomJoined); err != nil {
		_ = s.closeTransport()
		return nil, fmt.Errorf("signaling join: %w", err)
	}
	if err := c.registerSession(s); err != nil {
		// The client closed while the handshake was in flight. Returning a
		// dead session would leak it past Close, so registerSession has already
		// torn the transport down; fail the call instead.
		return nil, err
	}
	return s, nil
}

// RoomID returns the room this session is connected to.
func (s *Session) RoomID() string { return s.roomID }

// ParticipantID returns the participant this session carries.
func (s *Session) ParticipantID() string { return s.participantID }

// PublishTrack sends publish_track over the signaling connection and waits
// for track_published. The Handler registers the domain track in the room
// and creates the TrackForwarder the subscriber attaches to. Publishing the
// same trackID twice fails with an error matching ErrTrackAlreadyPublished.
func (s *Session) PublishTrack(ctx context.Context, trackID string, kind TrackKind, source TrackSource) error {
	payload := signaling.PublishTrackRequest{
		RoomID:        s.roomID,
		ParticipantID: s.participantID,
		Track: signaling.TrackInfo{
			ID:     trackID,
			Kind:   kind.String(),
			Source: source.String(),
		},
	}
	return s.request(ctx, signaling.MessageTypePublishTrack, payload, signaling.MessageTypeTrackPublished)
}

// SubscribeTrack sends subscribe_track and waits for track_subscribed. On
// success the subscriber's peer connection is registered on the track's
// TrackForwarder, so Client.Snapshot reports it in ForwarderSubscribers.
func (s *Session) SubscribeTrack(ctx context.Context, trackID string) error {
	payload := signaling.SubscribeTrackRequest{
		RoomID:        s.roomID,
		ParticipantID: s.participantID,
		TrackID:       trackID,
	}
	return s.request(ctx, signaling.MessageTypeSubscribeTrack, payload, signaling.MessageTypeTrackSubscribed)
}

// Close ends the signaling transport. The server keeps the participant
// session alive for reconnect until the ghost GC TTL elapses; call
// Client.Close for a full teardown. Close is idempotent and, because it only
// needs the transport lock, it also succeeds while a round-trip is parked
// waiting for a peer that never answers.
func (s *Session) Close() error {
	s.client.unregisterSession(s)
	return s.closeTransport()
}

// closeTransport drops the socket. It takes only closeMu, so it never blocks
// behind a pending read; closing the connection is precisely what releases a
// round-trip parked in ReadMessage. It is idempotent.
func (s *Session) closeTransport() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	ws := s.ws
	s.ws = nil
	if ws == nil {
		return nil
	}
	// Best-effort close frame with a bounded deadline: a wedged peer that never
	// drains the socket must not be able to hold teardown hostage.
	_ = ws.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(closeControlTimeout))
	return ws.Close()
}

// transport returns the live connection, or ErrSessionClosed once the session
// has been torn down. The pointer is snapshotted under closeMu; a concurrent
// closeTransport closes that connection, which makes any in-flight socket
// operation fail rather than use a dangling handle.
func (s *Session) transport() (*websocket.Conn, error) {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed || s.ws == nil {
		return nil, ErrSessionClosed
	}
	return s.ws, nil
}

// isClosed reports whether the transport has been torn down.
func (s *Session) isClosed() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	return s.closed || s.ws == nil
}

// request performs one round-trip: write msgType with payload, then wait
// for expectType (skipping broadcast notifications).
func (s *Session) request(ctx context.Context, msgType signaling.MessageType, payload interface{}, expectType signaling.MessageType) error {
	msg, err := signaling.NewMessage(msgType, payload)
	if err != nil {
		return err
	}
	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeLocked(ctx, data); err != nil {
		return err
	}
	return s.awaitLocked(ctx, msgType, expectType)
}

func (s *Session) writeLocked(ctx context.Context, data []byte) error {
	ws, err := s.transport()
	if err != nil {
		return err
	}
	_ = ws.SetWriteDeadline(roundTripDeadline(ctx))
	if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if s.isClosed() {
			return ErrSessionClosed
		}
		return err
	}
	return nil
}

// await reads frames until want arrives. reqType is the request type the
// server echoes in ErrorResponse.RequestType for this exchange; it differs
// from want when the response type and the request type are not the same
// message (the auto-join handshake, join_room -> room_joined). Broadcast
// notifications aimed at other flows (participant_joined, track_available,
// ...) are skipped.
func (s *Session) await(ctx context.Context, reqType, want signaling.MessageType) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.awaitLocked(ctx, reqType, want)
}

func (s *Session) awaitLocked(ctx context.Context, reqType, want signaling.MessageType) error {
	for {
		ws, err := s.transport()
		if err != nil {
			return err
		}
		if err := ws.SetReadDeadline(roundTripDeadline(ctx)); err != nil && s.isClosed() {
			// The socket vanished between the snapshot and the deadline call;
			// ReadMessage would report it, but say so in sentinel terms here.
			return ErrSessionClosed
		}
		_, data, err := ws.ReadMessage()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if s.isClosed() {
				return ErrSessionClosed
			}
			return err
		}
		msg, err := signaling.ParseMessage(data)
		if err != nil {
			return err
		}
		switch msg.Type {
		case want:
			return nil
		case signaling.MessageTypeError:
			var e signaling.ErrorResponse
			if err := msg.UnmarshalData(&e); err != nil {
				return &sessionError{
					message: fmt.Sprintf("signaling error (%s): unreadable payload", want),
				}
			}
			// Correlation: the server tags every error frame with the request
			// it answers. A tag naming a different exchange means this reply is
			// a late leftover, and accepting it would attribute the wrong
			// outcome to this call. The stream can no longer be trusted: drop
			// the session and say so. Both halves of the exchange count as
			// belonging to it, because the auto-join handshake is tagged with
			// its request type (join_room) while the awaited frame is
			// room_joined.
			if e.RequestType != "" && !answersExchange(e.RequestType, reqType, want) {
				_ = s.closeTransport()
				return fmt.Errorf("%w: signaling error tagged for %q received while awaiting %q [code %s]",
					ErrSessionClosed, e.RequestType, reqType, e.Code)
			}
			return newSessionError(want, e)
		default:
			// Notification or unrelated response: keep waiting.
		}
	}
}

// answersExchange reports whether an ErrorResponse.RequestType tag belongs to
// the exchange that is awaiting wantType as the answer to reqType. Request and
// response types are disjoint in this protocol, so accepting both halves of the
// pair cannot mask cross-talk from another exchange.
func answersExchange(tag string, reqType, wantType signaling.MessageType) bool {
	return tag == string(reqType) || tag == string(wantType)
}

// sessionError is the SDK-side error for a signaling ErrorResponse. It keeps
// the wire text for humans and exposes the frozen pkg/slive sentinel that the
// reply identifies through Unwrap, so callers can match
// errors.Is(sessionErr, slive.ErrTrackAlreadyPublished) without inventing new
// exported sentinels (B-2).
type sessionError struct {
	message string
	// target is the mapped sentinel, nil when the reply carries no
	// recognizable identity; errors.Is then matches nothing but itself.
	target error
}

func (e *sessionError) Error() string { return e.message }

// Unwrap returns the mapped sentinel, or nil for an unmapped reply.
func (e *sessionError) Unwrap() error { return e.target }

// newSessionError renders reply for want and binds the sentinel it maps to.
func newSessionError(want signaling.MessageType, reply signaling.ErrorResponse) error {
	return &sessionError{
		message: fmt.Sprintf("signaling error (%s): %s [%s]", want, reply.Error, reply.Code),
		target:  sentinelForErrorReply(reply),
	}
}

// sentinelForErrorReply maps a signaling ErrorResponse to the frozen pkg/slive
// sentinel it identifies. The wire code is authoritative; the message text
// fallback is legacy belt-and-braces for any pre-code replies that may still
// carry domain text with ErrorCodeInternalError. Unrecognized replies map to
// nil, so no identity is claimed for them.
func sentinelForErrorReply(reply signaling.ErrorResponse) error {
	switch reply.Code {
	case signaling.ErrorCodeRoomClosed:
		return ErrRoomClosed
	case signaling.ErrorCodeRoomNotFound:
		return ErrRoomNotFound
	case signaling.ErrorCodeParticipantNotFound:
		return ErrParticipantNotFound
	case signaling.ErrorCodeTrackNotFound:
		return ErrTrackNotFound
	case signaling.ErrorCodePeerConnectionClosed:
		return ErrPeerConnectionClosed
	case signaling.ErrorCodeTrackAlreadyPublished:
		return ErrTrackAlreadyPublished
	case signaling.ErrorCodeTrackAlreadySubscribed:
		return ErrTrackAlreadySubscribed
	case signaling.ErrorCodeTrackNotPublished:
		return ErrTrackNotPublished
	case signaling.ErrorCodeParticipantAlreadyExists:
		return ErrParticipantAlreadyExists
	case signaling.ErrorCodeParticipantLeft:
		return ErrParticipantLeft
	case signaling.ErrorCodeInvalidTrackKind:
		return ErrInvalidTrackKind
	case signaling.ErrorCodeInvalidTrackSource:
		return ErrInvalidTrackSource
	}

	// Legacy text fallback: retained as belt-and-braces for any
	// internal_error reply that still carries domain text.
	text := strings.ToLower(reply.Error)
	for _, m := range sentinelByText {
		if strings.Contains(text, m.text) {
			return m.sentinel
		}
	}
	return nil
}

// sentinelByText pairs a frozen sentinel with the internal error text the
// signaling layer carries when it has no dedicated code. Matching is by
// substring because handlers wrap domain errors with context; the entries are
// pairwise non-overlapping, so order does not matter.
var sentinelByText = []struct {
	sentinel error
	text     string
}{
	{ErrTrackAlreadyPublished, strings.ToLower(domain.ErrTrackAlreadyPublished.Error())},
	{ErrTrackAlreadySubscribed, strings.ToLower(domain.ErrTrackAlreadySubscribed.Error())},
	{ErrTrackNotPublished, strings.ToLower(domain.ErrTrackNotPublished.Error())},
	{ErrInvalidTrackKind, strings.ToLower(domain.ErrInvalidTrackKind.Error())},
	{ErrInvalidTrackSource, strings.ToLower(domain.ErrInvalidTrackSource.Error())},
	{ErrParticipantAlreadyExists, strings.ToLower(domain.ErrParticipantAlreadyExists.Error())},
	{ErrParticipantLeft, strings.ToLower(domain.ErrParticipantLeft.Error())},
	{ErrParticipantNotFound, strings.ToLower(domain.ErrParticipantNotFound.Error())},
	{ErrRoomClosed, strings.ToLower(domain.ErrRoomClosed.Error())},
}

// websocketURL converts the in-process server base URL to the signaling ws
// endpoint with the required query parameters.
func websocketURL(base, roomID, participantID string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}
	u.Path = config.DefaultWebSocketPath
	if !strings.HasPrefix(u.Path, "/") {
		u.Path = "/" + u.Path
	}
	q := u.Query()
	q.Set("room_id", roomID)
	q.Set("participant_id", participantID)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// handshakeTimeout bounds the WS handshake: the ctx deadline if one exists and
// is usable, otherwise defaultRoundTripTimeout (the in-process dial is
// instantaneous; this only guards a wedged handler). It never returns 0, which
// gorilla would read as "no timeout".
func handshakeTimeout(ctx context.Context) time.Duration {
	if dl, ok := ctx.Deadline(); ok {
		if d := time.Until(dl); d > minRoundTripTimeout {
			return d
		}
		return minRoundTripTimeout
	}
	return defaultRoundTripTimeout
}

// roundTripDeadline converts ctx into the deadline for one socket operation.
// The result is always in the future: a zero time means "no timeout" to
// gorilla, which is how a silent peer could park a round-trip — and with it
// Close — forever (B-5).
func roundTripDeadline(ctx context.Context) time.Time {
	dl, ok := ctx.Deadline()
	if !ok {
		return time.Now().Add(defaultRoundTripTimeout)
	}
	if time.Until(dl) < minRoundTripTimeout {
		// Expired or about to: keep it tiny but positive so the socket call
		// fails immediately instead of disabling the timeout.
		return time.Now().Add(minRoundTripTimeout)
	}
	return dl
}
