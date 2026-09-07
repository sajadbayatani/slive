package signaling

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestRegression_DuplicateCreateRoomIdempotent verifies that a duplicate
// create_room for the same participant does not return participant_already_exists
// but succeeds deterministically (idempotent). This covers the reported
// duplicate request where p-59750466 sent create_room twice after the WS
// auto-join had already placed it in test-room-003.
func TestRegression_DuplicateCreateRoomIdempotent(t *testing.T) {
	handler := newTestHandler()
	server := httptest.NewServer(handler)
	defer server.Close()

	conn := dialSignalingWS(t, server.URL, "test-room-003", "p-59750466")
	// Auto-join delivers room_joined
	waitForWSMessage(t, conn, MessageTypeRoomJoined, 5*time.Second)

	// Explicit create_room for same room/participant (duplicate)
	createReq := CreateRoomRequest{
		RoomID:          "test-room-003",
		ParticipantID:   "p-59750466",
		ParticipantName: "Tester",
	}
	msg, err := NewMessage(MessageTypeCreateRoom, createReq)
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	data, _ := msg.Marshal()
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	// Expect room_created success, NOT error participant_already_exists
	created := waitForWSMessage(t, conn, MessageTypeRoomCreated, 5*time.Second)
	var resp RoomCreatedResponse
	if err := created.UnmarshalData(&resp); err != nil {
		t.Fatalf("unmarshal room_created: %v", err)
	}
	if resp.RoomID != "test-room-003" || resp.ParticipantID != "p-59750466" || resp.Status != "success" {
		t.Errorf("room_created = %+v, want success for duplicate", resp)
	}

	// Second duplicate should also be idempotent success
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("second WriteMessage: %v", err)
	}
	created2 := waitForWSMessage(t, conn, MessageTypeRoomCreated, 5*time.Second)
	var resp2 RoomCreatedResponse
	if err := created2.UnmarshalData(&resp2); err != nil {
		t.Fatalf("unmarshal second room_created: %v", err)
	}
	if resp2.Status != "success" {
		t.Errorf("second duplicate create_room status = %s, want success", resp2.Status)
	}

	// Verify no duplicate participant state remains: room should have exactly 1 participant
	room := handler.roomManager.GetRoom("test-room-003")
	if room == nil {
		t.Fatal("room missing")
	}
	ids := room.Participants()
	if len(ids) != 1 {
		t.Errorf("participants = %v, want exactly 1 (no duplicate)", ids)
	}

	// Verify second WS connection as same participant is treated as reconnect,
	// not duplicate error: it should get room_joined and not create duplicate.
	conn2 := dialSignalingWS(t, server.URL, "test-room-003", "p-59750466")
	joined := waitForWSMessage(t, conn2, MessageTypeRoomJoined, 5*time.Second)
	var joinedResp RoomJoinedResponse
	if err := joined.UnmarshalData(&joinedResp); err != nil {
		t.Fatalf("unmarshal room_joined reconnect: %v", err)
	}
	if joinedResp.ParticipantID != "p-59750466" {
		t.Errorf("reconnect room_joined participant = %s, want p-59750466", joinedResp.ParticipantID)
	}
	// Room still has 1 participant (reconnect reuses)
	if len(room.Participants()) != 1 {
		t.Errorf("after reconnect participants = %d, want 1", len(room.Participants()))
	}

	// Ensure the second duplicate did not produce an error message in the meantime
	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, data2, err := conn.ReadMessage()
	if err == nil {
		if m, err2 := ParseMessage(data2); err2 == nil && m.Type == MessageTypeError {
			var er ErrorResponse
			_ = m.UnmarshalData(&er)
			t.Errorf("unexpected error after duplicate create_room: %+v", er)
		}
	}
}

// TestRegression_CreateRoomNewParticipantInExistingRoom verifies that a second
// participant p-f8d2a6b3 can create/join the same room where p-59750466 already
// exists, and both are present.
func TestRegression_CreateRoomNewParticipantInExistingRoom(t *testing.T) {
	handler := newTestHandler()
	server := httptest.NewServer(handler)
	defer server.Close()

	alice := dialSignalingWS(t, server.URL, "test-room-003", "p-59750466")
	waitForWSMessage(t, alice, MessageTypeRoomJoined, 5*time.Second)

	bob := dialSignalingWS(t, server.URL, "test-room-003", "p-f8d2a6b3")
	bobJoined := waitForWSMessage(t, bob, MessageTypeRoomJoined, 5*time.Second)
	var bj RoomJoinedResponse
	if err := bobJoined.UnmarshalData(&bj); err != nil {
		t.Fatalf("unmarshal bob room_joined: %v", err)
	}
	if len(bj.Participants) != 2 {
		t.Errorf("bob room_joined participants = %+v, want 2", bj.Participants)
	}
	// Alice should see bob joined broadcast
	waitForWSMessage(t, alice, MessageTypeParticipantJoined, 5*time.Second)

	room := handler.roomManager.GetRoom("test-room-003")
	if len(room.Participants()) != 2 {
		t.Errorf("room participants = %d, want 2", len(room.Participants()))
	}
}
