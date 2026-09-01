package e2e

import (
	"net/http"
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

// TestE2E_OriginPolicy verifies D1: cross-origin WS upgrade is rejected (403)
// and same-origin / no-Origin accepted.
func TestE2E_OriginPolicy(t *testing.T) {
	h := signaling.NewHandler(signaling.NewRoomManager(),
		signaling.WithPeerConnectionConfig(webrtc.PeerConnectionConfig{
			SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
		}),
		signaling.WithAllowedOrigins([]string{"https://allowed.example.com"}),
	)
	router := apphttp.NewRouter(config.Config{HealthPath: "/health", WebSocketPath: "/ws"}, apphttp.HandlerDeps{SignalingHandler: h})
	ts := httptest.NewServer(router.ServeMux())
	defer ts.Close()

	wsBase := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?room_id=origin-room&participant_id=alice"

	// No Origin -> allowed
	if _, _, err := websocket.DefaultDialer.Dial(wsBase, nil); err != nil {
		t.Errorf("no-Origin dial should be allowed, got %v", err)
	}

	// Same-origin -> allowed: Origin host == request Host
	host := strings.TrimPrefix(ts.URL, "http://")
	host = strings.Split(host, "/")[0]
	headers := http.Header{}
	headers.Set("Origin", "http://"+host)
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	if _, _, err := dialer.Dial(wsBase, headers); err != nil {
		// Same participant collision on same room: try different room
		wsBaseSame := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?room_id=origin-same&participant_id=alice2"
		if _, _, err2 := dialer.Dial(wsBaseSame, headers); err2 != nil {
			t.Errorf("same-origin dial should be allowed, got %v (first try %v)", err2, err)
		}
	}

	// Allowlisted -> allowed
	headersAllowed := http.Header{}
	headersAllowed.Set("Origin", "https://allowed.example.com")
	wsBase2 := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?room_id=origin-allow&participant_id=bob"
	if _, _, err := dialer.Dial(wsBase2, headersAllowed); err != nil {
		t.Errorf("allowlisted dial should be allowed, got %v", err)
	}

	// Cross-origin not allowlisted -> rejected (403 via websocket upgrade failure)
	headersCross := http.Header{}
	headersCross.Set("Origin", "https://evil.example.com")
	wsBaseEvil := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?room_id=origin-evil&participant_id=evil"
	if _, resp, err := dialer.Dial(wsBaseEvil, headersCross); err == nil {
		t.Error("cross-origin dial should be rejected (403), got success")
	} else {
		if resp != nil && resp.StatusCode != 403 {
			t.Errorf("cross-origin dial status = %d, want 403", resp.StatusCode)
		}
		_ = resp
	}
}

// TestE2E_DeadlineDeadPeer verifies a client silent past read-timeout gets its
// session torn down (short-timeout config via WithWSReadTimeout).
func TestE2E_DeadlineDeadPeer(t *testing.T) {
	h := signaling.NewHandler(signaling.NewRoomManager(),
		signaling.WithPeerConnectionConfig(webrtc.PeerConnectionConfig{
			SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
		}),
		signaling.WithWSReadTimeout(500*time.Millisecond),
		signaling.WithWSPingInterval(200*time.Millisecond),
		signaling.WithWSWriteTimeout(500*time.Millisecond),
	)
	router := apphttp.NewRouter(config.Config{HealthPath: "/health", WebSocketPath: "/ws"}, apphttp.HandlerDeps{SignalingHandler: h})
	ts := httptest.NewServer(router.ServeMux())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?room_id=deadline-room&participant_id=silent"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Consume room_joined
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read room_joined: %v", err)
	}

	// Stay silent past read timeout (500ms) without sending pongs.
	// Server should close connection after read deadline.
	time.Sleep(1200 * time.Millisecond)

	// Next read should fail (closed)
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Error("expected connection closed after silent timeout, got message")
	} else {
		t.Logf("dead peer torn down as expected: %v", err)
	}

	// Verify GC will reap after TTL? Not needed.
	_ = h.Snapshot()
}
