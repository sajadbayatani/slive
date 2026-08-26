// Package e2e contains black-box end-to-end tests for Slive's signaling
// stack: a real HTTP router mounts the real signaling handler (exactly like
// cmd/slive wires them) and plain WebSocket clients drive the control plane
// through the wire protocol. No internal handles are used beyond the
// message payloads themselves.
package e2e

import (
	"encoding/json"
	"errors"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/config"
	apphttp "github.com/sajadbayatani/slive/internal/http"
	"github.com/sajadbayatani/slive/internal/signaling"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

// e2eTimeout bounds every individual network wait; the whole test stays far
// below 30s even under the race detector.
const e2eTimeout = 15 * time.Second

// newE2EServer boots an httptest server around the REAL mounted router with
// a STUN-free signaling handler so negotiation stays deterministic offline.
func newE2EServer(t *testing.T) *httptest.Server {
	t.Helper()

	router := apphttp.NewRouter(config.Config{
		HealthPath:    "/health",
		WebSocketPath: "/ws",
	}, apphttp.HandlerDeps{
		SignalingHandler: signaling.NewHandler(
			signaling.NewRoomManager(),
			signaling.WithPeerConnectionConfig(webrtc.PeerConnectionConfig{
				SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
			}),
		),
	})

	ts := httptest.NewServer(router.ServeMux())
	t.Cleanup(ts.Close)
	return ts
}

// wsClient wraps one participant's WebSocket transport.
type wsClient struct {
	t    *testing.T
	conn *websocket.Conn
}

// dialWS connects a participant to the server's WebSocket endpoint.
func dialWS(t *testing.T, ts *httptest.Server, roomID, participantID string) *wsClient {
	t.Helper()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") +
		"/ws?room_id=" + roomID + "&participant_id=" + participantID

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", wsURL, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &wsClient{t: t, conn: conn}
}

// send marshals and transmits one signaling request.
func (c *wsClient) send(msgType signaling.MessageType, payload interface{}) {
	c.t.Helper()

	data, err := json.Marshal(payload)
	if err != nil {
		c.t.Fatalf("marshal %s payload: %v", msgType, err)
	}
	msg, err := json.Marshal(signaling.Message{Type: msgType, Data: data})
	if err != nil {
		c.t.Fatalf("encode %s message: %v", msgType, err)
	}
	if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
		c.t.Fatalf("send %s: %v", msgType, err)
	}
}

// receive reads the next signaling message within the timeout budget.
func (c *wsClient) receive(what string) *signaling.Message {
	c.t.Helper()

	if err := c.conn.SetReadDeadline(time.Now().Add(e2eTimeout)); err != nil {
		c.t.Fatalf("SetReadDeadline (%s): %v", what, err)
	}
	_, data, err := c.conn.ReadMessage()
	if err != nil {
		c.t.Fatalf("read %s: %v", what, err)
	}
	msg, err := signaling.ParseMessage(data)
	if err != nil {
		c.t.Fatalf("parse %s message %q: %v", what, data, err)
	}
	return msg
}

// receiveOfType reads messages until one of the wanted type arrives; every
// skipped message is reported, so ordering mistakes surface clearly.
func (c *wsClient) receiveOfType(want signaling.MessageType, what string) *signaling.Message {
	c.t.Helper()

	for {
		msg := c.receive(what)
		if msg.Type == want {
			return msg
		}
		c.t.Logf("[%s] skipping %s while waiting for %s", what, msg.Type, want)
	}
}

// expectSilence verifies that no message (in particular no error response)
// arrives within a short window; used to confirm fire-and-forget operations
// were accepted. The window elapses via read deadline, never a bare sleep.
func (c *wsClient) expectSilence(what string, window time.Duration) {
	c.t.Helper()

	if err := c.conn.SetReadDeadline(time.Now().Add(window)); err != nil {
		c.t.Fatalf("SetReadDeadline (%s): %v", what, err)
	}
	_, data, err := c.conn.ReadMessage()
	if err == nil {
		c.t.Fatalf("%s: expected silence, got message %s", what, data)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		c.t.Fatalf("%s: expected read-deadline timeout, got %v", what, err)
	}
}

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}
