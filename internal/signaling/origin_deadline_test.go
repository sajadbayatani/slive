package signaling

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// dialWithOrigin dials ws with given Origin header. If origin == "" no Origin header is sent.
// Uses websocket.Dialer with custom header.
func dialWithOrigin(t *testing.T, serverURL, roomID, participantID, origin string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	wsURL := strings.Replace(serverURL, "http", "ws", 1) + "/ws?room_id=" + roomID + "&participant_id=" + participantID
	dialer := websocket.Dialer{}
	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
		// If origin empty, don't set header at all to simulate no-Origin client.
		return dialer.Dial(wsURL, header)
	}
	// For no-Origin case, dial without header (gorilla adds no Origin by default if header nil? Explicit nil ensures no Origin)
	return dialer.Dial(wsURL, nil)
}

func TestOrigin_NoOriginAccepted(t *testing.T) {
	h := NewHandler(NewRoomManager(), WithPeerConnectionConfig(newTestPeerConnectionConfig()))
	srv := httptest.NewServer(h)
	defer srv.Close()

	conn, _, err := dialWithOrigin(t, srv.URL, "origin-room", "p-no-origin", "")
	if err != nil {
		t.Fatalf("dial no-Origin should succeed: %v", err)
	}
	defer conn.Close()
	// Should receive room_joined
	msg := waitForWSMessage(t, conn, MessageTypeRoomJoined, 2*time.Second)
	if msg == nil {
		t.Fatal("expected room_joined")
	}
}

func TestOrigin_SameOriginAccepted(t *testing.T) {
	h := NewHandler(NewRoomManager(), WithPeerConnectionConfig(newTestPeerConnectionConfig()))
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Extract host from server URL for same-origin origin
	host := strings.TrimPrefix(srv.URL, "http://")
	origin := "http://" + host

	conn, _, err := dialWithOrigin(t, srv.URL, "origin-room", "p-same", origin)
	if err != nil {
		t.Fatalf("dial same-Origin should succeed: %v", err)
	}
	defer conn.Close()
	waitForWSMessage(t, conn, MessageTypeRoomJoined, 2*time.Second)
}

func TestOrigin_AllowlistedAccepted(t *testing.T) {
	allowed := "https://allowed.example.com"
	h := NewHandler(NewRoomManager(), WithPeerConnectionConfig(newTestPeerConnectionConfig()), WithAllowedOrigins([]string{allowed}))
	srv := httptest.NewServer(h)
	defer srv.Close()

	conn, _, err := dialWithOrigin(t, srv.URL, "origin-room", "p-allow", allowed)
	if err != nil {
		t.Fatalf("dial allowlisted origin should succeed: %v", err)
	}
	defer conn.Close()
	waitForWSMessage(t, conn, MessageTypeRoomJoined, 2*time.Second)
}

func TestOrigin_CrossOriginRejected(t *testing.T) {
	h := NewHandler(NewRoomManager(), WithPeerConnectionConfig(newTestPeerConnectionConfig()), WithAllowedOrigins([]string{"https://allowed.example.com"}))
	srv := httptest.NewServer(h)
	defer srv.Close()

	_, resp, err := dialWithOrigin(t, srv.URL, "origin-room", "p-bad", "https://evil.example.com")
	if err == nil {
		t.Fatal("dial cross-origin should be rejected, but succeeded")
	}
	if resp != nil && resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403, got %d", resp.StatusCode)
	}
	// Also verify that no participant was created
	if rm := h.roomManager.GetRoom("origin-room"); rm != nil {
		if rm.GetParticipant("p-bad") != nil {
			t.Error("participant should not exist after rejected handshake")
		}
	}
}

// Dead peer detected within read-timeout window.
func TestDeadline_DeadPeerDetected(t *testing.T) {
	readTimeout := 400 * time.Millisecond
	pingInterval := 150 * time.Millisecond
	h := NewHandler(NewRoomManager(),
		WithPeerConnectionConfig(newTestPeerConnectionConfig()),
		WithWSReadTimeout(readTimeout),
		WithWSPingInterval(pingInterval),
		WithWSWriteTimeout(200*time.Millisecond),
		WithGCTTL(5*time.Second), // keep ghost alive so we can observe transport drop separate from GC
	)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Create a raw TCP-ish peer that does not respond to ping.
	// Use websocket dial but then stop reading: set client PingHandler to no-op and do not call ReadMessage after initial handshake,
	// so server pings will never be ponged and read deadline will fire.
	wsURL := strings.Replace(srv.URL, "http", "ws", 1) + "/ws?room_id=dead-room&participant_id=dead-peer"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Read initial room_joined to confirm connection up.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read room_joined: %v", err)
	}
	if msg, err := ParseMessage(data); err != nil || msg.Type != MessageTypeRoomJoined {
		t.Fatalf("expected room_joined, got %v %v", msg, err)
	}
	// Now stop reading and prevent auto pong: set PingHandler that does nothing and suppress pong
	conn.SetPingHandler(func(string) error { return nil })
	// Also set PongHandler nil? Not needed.
	// From now on client is dead: it will not pong server pings.

	// Server should close connection within readTimeout + some grace
	deadline := time.Now().Add(readTimeout + 800*time.Millisecond)
	for time.Now().Before(deadline) {
		if h.connectionManager.Get("dead-peer") == nil {
			return // reaped as expected
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("dead peer not detected within %v; still in connectionManager", readTimeout+800*time.Millisecond)
}

// Healthy session survives ≥2 ping intervals via pong refresh.
func TestPingPong_Keepalive(t *testing.T) {
	readTimeout := 600 * time.Millisecond
	pingInterval := 150 * time.Millisecond
	h := NewHandler(NewRoomManager(),
		WithPeerConnectionConfig(newTestPeerConnectionConfig()),
		WithWSReadTimeout(readTimeout),
		WithWSPingInterval(pingInterval),
		WithWSWriteTimeout(200*time.Millisecond),
		WithGCTTL(5*time.Second),
	)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := strings.Replace(srv.URL, "http", "ws", 1) + "/ws?room_id=keep-room&participant_id=keep-peer"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Keep reading in background so client auto-pongs.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}()

	// Wait for initial room_joined via handler state: wait until participant appears
	waitForCondition(t, 2*time.Second, "keep-peer joined", func() bool {
		if rm := h.roomManager.GetRoom("keep-room"); rm != nil {
			return rm.GetParticipant("keep-peer") != nil
		}
		return false
	})

	// Now wait ≥2 ping intervals and assert connection still registered and participant still present
	time.Sleep(pingInterval*3 + 200*time.Millisecond)

	if h.connectionManager.Get("keep-peer") == nil {
		t.Fatal("healthy connection was reaped despite pong refresh")
	}
	if rm := h.roomManager.GetRoom("keep-room"); rm == nil || rm.GetParticipant("keep-peer") == nil {
		t.Fatal("participant missing after keepalive period")
	}
	_ = conn.Close()
	<-done
}

// Verify ping interval enforcement ≤ ReadTimeout/2
func TestKeepalive_PingIntervalEnforced(t *testing.T) {
	h := NewHandler(NewRoomManager(),
		WithPeerConnectionConfig(newTestPeerConnectionConfig()),
		WithWSReadTimeout(1*time.Second),
		WithWSPingInterval(800*time.Millisecond), // exceeds 500ms cap
	)
	if h.wsPingInterval > 500*time.Millisecond {
		t.Errorf("ping interval not capped: got %v want ≤500ms", h.wsPingInterval)
	}
	if h.wsPingInterval != 500*time.Millisecond {
		t.Errorf("ping interval = %v, want 500ms (read/2)", h.wsPingInterval)
	}
}
