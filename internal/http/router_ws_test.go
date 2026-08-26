package http

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/config"
	"github.com/sajadbayatani/slive/internal/signaling"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

// offlineSignalingHandler returns a signaling handler with a STUN-free peer
// connection config so tests stay deterministic and offline.
func offlineSignalingHandler() *signaling.Handler {
	return signaling.NewHandler(
		signaling.NewRoomManager(),
		signaling.WithPeerConnectionConfig(webrtc.PeerConnectionConfig{
			SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
		}),
	)
}

// TestRouterMountsSignalingHandler is the TASK-016 acceptance test: the
// signaling handler becomes reachable through the HTTP server on the
// configured WebSocket path, while /health keeps its contract.
func TestRouterMountsSignalingHandler(t *testing.T) {
	router := NewRouter(config.Config{
		HealthPath:    "/health",
		WebSocketPath: "/ws",
	}, HandlerDeps{SignalingHandler: offlineSignalingHandler()})

	ts := httptest.NewServer(router.ServeMux())
	defer ts.Close()

	// A plain HTTP GET without the required query parameters must reach the
	// signaling handler and be rejected by it with a 400.
	resp, err := http.Get(ts.URL + "/ws")
	if err != nil {
		t.Fatalf("GET /ws: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("GET /ws status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "room_id and participant_id are required") {
		t.Errorf("GET /ws body = %q, want the signaling handler's rejection message", string(body))
	}

	// The health contract stays intact next to the new route.
	healthResp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Errorf("GET /health status = %d, want %d", healthResp.StatusCode, http.StatusOK)
	}
}

// TestRouterHonoursCustomWebSocketPath verifies the route follows runtime
// configuration instead of a hardcoded path.
func TestRouterHonoursCustomWebSocketPath(t *testing.T) {
	router := NewRouter(config.Config{
		HealthPath:    "/health",
		WebSocketPath: "/signal",
	}, HandlerDeps{SignalingHandler: offlineSignalingHandler()})

	ts := httptest.NewServer(router.ServeMux())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/signal")
	if err != nil {
		t.Fatalf("GET /signal: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("GET /signal status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}

	missing, err := http.Get(ts.URL + "/ws")
	if err != nil {
		t.Fatalf("GET /ws: %v", err)
	}
	missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Errorf("GET /ws status = %d, want 404 when the configured path is different", missing.StatusCode)
	}
}

// TestRouterWithoutSignalingHandlerKeepsHealthOnly ensures minimal setups
// that do not inject a signaling handler behave exactly as before.
func TestRouterWithoutSignalingHandlerKeepsHealthOnly(t *testing.T) {
	router := NewRouter(config.Config{HealthPath: "/health"}, HandlerDeps{})

	ts := httptest.NewServer(router.ServeMux())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/ws")
	if err != nil {
		t.Fatalf("GET /ws: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET /ws status = %d, want 404 without an injected handler", resp.StatusCode)
	}
}

// TestServerMountsSignalingViaOption verifies the end-to-end wiring through
// NewServer's option: a real WebSocket client can join a room through the
// server's mux and receives the room_joined response.
func TestServerMountsSignalingViaOption(t *testing.T) {
	server := NewServer(config.Config{HTTPAddr: ":0"}, nil,
		WithSignalingHandler(offlineSignalingHandler()))

	ts := httptest.NewServer(server.router.ServeMux())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") +
		"/ws?room_id=mount-room&participant_id=p-1"

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", wsURL, err)
	}
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("waiting for room_joined: %v", err)
		}
		msg, err := signaling.ParseMessage(data)
		if err != nil {
			continue
		}
		if msg.Type == signaling.MessageTypeRoomJoined {
			var joined signaling.RoomJoinedResponse
			if err := msg.UnmarshalData(&joined); err != nil {
				t.Fatalf("unmarshal room_joined: %v", err)
			}
			if joined.ParticipantID != "p-1" || joined.RoomID != "mount-room" {
				t.Errorf("room_joined = %+v, want p-1/mount-room", joined)
			}
			return
		}
	}
}
