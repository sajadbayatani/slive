package signaling

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// dialSignalingWS connects a signaling client to the handler served by the
// given test server. Connection closing is registered via t.Cleanup so it
// happens before the server shutdown cleanup (cleanups run LIFO).
func dialSignalingWS(t *testing.T, serverURL, roomID, participantID string) *websocket.Conn {
	t.Helper()

	wsURL := strings.Replace(serverURL, "http", "ws", 1) +
		"/ws?room_id=" + roomID + "&participant_id=" + participantID

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", wsURL, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// waitForWSMessage reads frames from the connection until a message of the
// wanted type arrives or the timeout expires. Messages of other types are
// skipped: each socket in a test only asserts on its own message types, and
// unrelated traffic is consumed harmlessly.
func waitForWSMessage(t *testing.T, conn *websocket.Conn, want MessageType, timeout time.Duration) *Message {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("waiting for %s: %v", want, err)
		}
		msg, err := ParseMessage(data)
		if err != nil {
			continue // not a valid signaling frame; keep waiting
		}
		if msg.Type == want {
			return msg
		}
	}
}

// TestHandlerBroadcastReachesRegisteredConnections is the regression test for
// the premature connection-registry removal bug: ServeHTTP used to schedule
// the registry removal as soon as it returned (immediately after spawning the
// session goroutine), leaving broadcasts with an empty registry. Connections
// must stay registered for the whole session so room members receive
// participant and track notifications.
func TestHandlerBroadcastReachesRegisteredConnections(t *testing.T) {
	handler := newTestHandler()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close) // registered first => runs after sockets are closed

	alice := dialSignalingWS(t, server.URL, "broadcast-room", "alice")
	waitForWSMessage(t, alice, MessageTypeRoomJoined, 5*time.Second)

	bob := dialSignalingWS(t, server.URL, "broadcast-room", "bob")
	bobJoined := waitForWSMessage(t, bob, MessageTypeRoomJoined, 5*time.Second)

	var bobRoom RoomJoinedResponse
	if err := bobJoined.UnmarshalData(&bobRoom); err != nil {
		t.Fatalf("unmarshal room_joined: %v", err)
	}
	if len(bobRoom.Participants) != 2 {
		t.Errorf("expected 2 participants in room_joined, got %d", len(bobRoom.Participants))
	}

	// Regression (1): bob's join must be broadcast to alice through the
	// connection registry.
	joinedMsg := waitForWSMessage(t, alice, MessageTypeParticipantJoined, 5*time.Second)
	var joined ParticipantJoinedNotification
	if err := joinedMsg.UnmarshalData(&joined); err != nil {
		t.Fatalf("unmarshal participant_joined: %v", err)
	}
	if joined.Participant.ID != "bob" {
		t.Errorf("participant_joined for %q, want bob", joined.Participant.ID)
	}

	// Alice publishes a track; she gets the acknowledgement...
	publish, err := NewMessage(MessageTypePublishTrack, PublishTrackRequest{
		RoomID:        "broadcast-room",
		ParticipantID: "alice",
		Track: TrackInfo{
			ID:     "alice-audio",
			Kind:   "audio",
			Source: "microphone",
		},
	})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	payload, err := publish.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := alice.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	waitForWSMessage(t, alice, MessageTypeTrackPublished, 5*time.Second)

	// Regression (2): ...and bob must see the track availability broadcast.
	availableMsg := waitForWSMessage(t, bob, MessageTypeTrackAvailable, 5*time.Second)
	var available TrackAvailableNotification
	if err := availableMsg.UnmarshalData(&available); err != nil {
		t.Fatalf("unmarshal track_available: %v", err)
	}
	if available.ParticipantID != "alice" || available.Track.ID != "alice-audio" {
		t.Errorf("track_available = %+v, want alice/alice-audio", available)
	}

	// The track must be resolvable through the room registry now that
	// publishing registers tracks there.
	room := handler.roomManager.GetRoom("broadcast-room")
	if room == nil {
		t.Fatal("broadcast-room missing from room manager")
	}
	if room.GetTrack("alice-audio") == nil {
		t.Error("published track missing from room registry")
	}

	// Both connections must still be registered while their sessions run.
	for _, id := range []string{"alice", "bob"} {
		if handler.connectionManager.Get(id) == nil {
			t.Errorf("connection %q missing from registry during session", id)
		}
	}
}
