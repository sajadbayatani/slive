package signaling

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	pionwebrtc "github.com/pion/webrtc/v3"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

// newHeadlessConn builds a bare *Connection without a WebSocket transport.
// handleMessage and the connection registry take the concrete *Connection
// type, so tests drive them with these literals: Send lands in sendChan,
// from which drainMessages collects the responses.
func newHeadlessConn(participantID, roomID string) *Connection {
	return &Connection{
		participantID: participantID,
		roomID:        roomID,
		state:         ConnectionStateConnected,
		sendChan:      make(chan []byte, 64),
		receiveChan:   make(chan []byte, 64),
		closeChan:     make(chan struct{}),
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// drainMessages returns every queued outbound message without blocking.
func drainMessages(conn *Connection) []*Message {
	var msgs []*Message
	for {
		select {
		case data := <-conn.sendChan:
			if msg, err := ParseMessage(data); err == nil {
				msgs = append(msgs, msg)
			}
		default:
			return msgs
		}
	}
}

// TestHandleConnectionClosedKeepsSessionAlive is the regression test for the
// reconnect semantics: a WebSocket drop used to tear down the whole session
// (room.Leave + pc.Close). It must now keep both alive so a reconnecting
// client resumes its media session, and the rejoin must reuse the SAME peer
// connection instance with its signaling sender swapped onto the new socket.
func TestHandleConnectionClosedKeepsSessionAlive(t *testing.T) {
	h := newTestHandler()
	room, participant := joinParticipant(t, h, "drop-room", "p-drop")

	senderA := make(chan string, 16)
	pc1, err := h.ensurePeerConnection(participant, channelSender(senderA))
	if err != nil {
		t.Fatalf("ensurePeerConnection (join): %v", err)
	}

	// An observer connection must not be told that the participant left:
	// the session stays joined, so no participant_left broadcast is correct.
	observer := newHeadlessConn("observer", "drop-room")
	h.connectionManager.Add(observer)

	// WebSocket transport dies.
	h.handleConnectionClosed(room, participant)

	// Participant must still be in the room...
	if got := room.GetParticipant("p-drop"); got == nil {
		t.Fatal("participant was removed from the room on websocket drop")
	}

	// ...and the peer connection must stay registered and usable.
	stored := h.getPeerConnection("p-drop")
	if stored == nil {
		t.Fatal("peer connection removed from registry on websocket drop")
	}
	if stored != pc1 {
		t.Fatal("registry references a different peer connection than before the drop")
	}
	if !stored.State().Usable() {
		t.Errorf("peer connection state = %s after drop, want a usable state", stored.State())
	}

	for _, m := range drainMessages(observer) {
		if m.Type == MessageTypeParticipantLeft {
			t.Error("observer received participant_left although the session stayed alive")
		}
	}

	// Reconnect: ensurePeerConnection must return the same instance.
	senderB := make(chan string, 16)
	pc2, err := h.ensurePeerConnection(participant, channelSender(senderB))
	if err != nil {
		t.Fatalf("ensurePeerConnection (rejoin): %v", err)
	}
	if pc2 != pc1 {
		t.Fatal("rejoin created a new peer connection instead of reusing the live one")
	}

	// Prove the sender swap took effect: a new transceiver fires
	// negotiation-needed and the automatic offer lands on the NEW sender.
	if _, err := pc2.PionPeerConnection().AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("AddTransceiverFromKind: %v", err)
	}
	waitForMessageOnSender(t, senderB, "webrtc:offer", "swapped-in sender after reconnect")
	select {
	case msgType := <-senderA:
		t.Errorf("stale pre-drop sender received %q", msgType)
	default:
	}

	// Explicit leave afterwards is terminal: closes and deregisters.
	h.closePeerConnection("p-drop")
	if got := h.getPeerConnection("p-drop"); got != nil {
		t.Error("peer connection still registered after explicit close")
	}
	if state := pc1.State(); state != webrtc.PeerConnectionStateClosed {
		t.Errorf("peer connection state after explicit close = %s, want closed", state)
	}

	// A later rejoin therefore gets a fresh connection.
	pc3, err := h.ensurePeerConnection(participant, channelSender(make(chan string, 8)))
	if err != nil {
		t.Fatalf("ensurePeerConnection (after leave): %v", err)
	}
	if pc3 == pc1 {
		t.Error("expected a fresh peer connection after the previous one was closed")
	}
}

// TestLeaveRoomClosesPeerConnection drives the explicit leave_room message
// through the dispatch table: the participant leaves the room AND its peer
// connection is closed and removed from the registry (terminal path).
func TestLeaveRoomClosesPeerConnection(t *testing.T) {
	h := newTestHandler()
	room, participant := joinParticipant(t, h, "leave-room", "p-leave")

	pc1, err := h.ensurePeerConnection(participant, channelSender(make(chan string, 8)))
	if err != nil {
		t.Fatalf("ensurePeerConnection: %v", err)
	}

	payload, err := json.Marshal(LeaveRoomRequest{RoomID: "leave-room", ParticipantID: "p-leave"})
	if err != nil {
		t.Fatalf("marshal leave request: %v", err)
	}

	conn := newHeadlessConn("p-leave", "leave-room")
	if err := h.handleMessage(conn, room, participant, &Message{
		Type: MessageTypeLeaveRoom,
		Data: payload,
	}); err != nil {
		t.Fatalf("handleMessage(leave_room): %v", err)
	}

	var gotLeft bool
	for _, m := range drainMessages(conn) {
		if m.Type == MessageTypeRoomLeft {
			gotLeft = true
		}
	}
	if !gotLeft {
		t.Error("expected a room_left response for the leaving participant")
	}

	if got := room.GetParticipant("p-leave"); got != nil {
		t.Error("participant still in room after leave_room")
	}
	if got := h.getPeerConnection("p-leave"); got != nil {
		t.Error("peer connection still registered after leave_room")
	}
	if state := pc1.State(); state != webrtc.PeerConnectionStateClosed {
		t.Errorf("peer connection state after leave_room = %s, want closed", state)
	}
}

// TestWebRTCErrorResponsesCarryMappedCodes drives the WebRTC handlers with
// failure inputs through the real dispatch table and asserts the client sees
// the mapped error codes instead of internal_error fallbacks.
func TestWebRTCErrorResponsesCarryMappedCodes(t *testing.T) {
	h := newTestHandler()
	room, participant := joinParticipant(t, h, "err-room", "p-err")
	if _, err := h.ensurePeerConnection(participant, channelSender(make(chan string, 8))); err != nil {
		t.Fatalf("ensurePeerConnection: %v", err)
	}

	tests := []struct {
		name     string
		msgType  MessageType
		payload  interface{}
		wantCode string
	}{
		{
			// handleOffer checks room membership first.
			name:     "offer unknown target",
			msgType:  MessageTypeOffer,
			payload:  OfferRequest{ParticipantID: "p-err", TargetParticipantID: "ghost", SDP: "v=0\r\n"},
			wantCode: ErrorCodeParticipantNotFound,
		},
		{
			// handleAnswer resolves targets through the peer-connection registry.
			name:     "answer unknown target",
			msgType:  MessageTypeAnswer,
			payload:  AnswerRequest{ParticipantID: "p-err", TargetParticipantID: "ghost", SDP: "v=0\r\n"},
			wantCode: ErrorCodeConnectionNotFound,
		},
		{
			name:     "ice candidate unknown target",
			msgType:  MessageTypeICECandidate,
			payload:  ICECandidateRequest{ParticipantID: "p-err", TargetParticipantID: "ghost", Candidate: "candidate:1 1 udp 1 192.0.2.1 1 typ host", SDPMid: "0", SDPMLineIndex: 0},
			wantCode: ErrorCodeConnectionNotFound,
		},
		{
			name:     "ice candidate oversized",
			msgType:  MessageTypeICECandidate,
			payload:  ICECandidateRequest{ParticipantID: "p-err", TargetParticipantID: "p-err", Candidate: string(make([]byte, MaxCandidateLength+1)), SDPMid: "0", SDPMLineIndex: 0},
			wantCode: ErrorCodeInvalidRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.payload)
			if err != nil {
				t.Fatalf("marshal payload: %v", err)
			}

			conn := newHeadlessConn("p-err", "err-room")
			if err := h.handleMessage(conn, room, participant, &Message{Type: tt.msgType, Data: data}); err != nil {
				t.Fatalf("handleMessage returned error: %v", err)
			}

			messages := drainMessages(conn)
			if len(messages) != 1 {
				t.Fatalf("expected exactly one response message, got %d", len(messages))
			}
			if messages[0].Type != MessageTypeError {
				t.Fatalf("message type = %s, want error", messages[0].Type)
			}

			var resp ErrorResponse
			if err := messages[0].UnmarshalData(&resp); err != nil {
				t.Fatalf("unmarshal error response: %v", err)
			}
			if resp.Code != tt.wantCode {
				t.Errorf("error code = %q, want %q", resp.Code, tt.wantCode)
			}
			if resp.RequestType != string(tt.msgType) {
				t.Errorf("request_type = %q, want %q", resp.RequestType, string(tt.msgType))
			}
		})
	}
}

// TestWebRTCErrorOnClosedPeerConnection pins the peer_connection_closed
// mapping through the real dispatch path: a registered-but-closed peer
// connection is still found by handleAnswer, and its operation failure is
// reported to the client with that specific code.
func TestWebRTCErrorOnClosedPeerConnection(t *testing.T) {
	h := newTestHandler()
	room, participant := joinParticipant(t, h, "closed-pc-room", "p-closed")

	pc1, err := h.ensurePeerConnection(participant, channelSender(make(chan string, 8)))
	if err != nil {
		t.Fatalf("ensurePeerConnection: %v", err)
	}
	// Close the wrapper directly; Close does not deregister it from the
	// handler registry, mirroring a connection that died between events.
	if err := pc1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := json.Marshal(AnswerRequest{
		ParticipantID:       "p-closed",
		TargetParticipantID: "p-closed",
		SDP:                 "v=0\r\n",
	})
	if err != nil {
		t.Fatalf("marshal answer request: %v", err)
	}

	conn := newHeadlessConn("p-closed", "closed-pc-room")
	if err := h.handleMessage(conn, room, participant, &Message{
		Type: MessageTypeAnswer,
		Data: data,
	}); err != nil {
		t.Fatalf("handleMessage(answer): %v", err)
	}

	messages := drainMessages(conn)
	if len(messages) != 1 || messages[0].Type != MessageTypeError {
		t.Fatalf("expected exactly one error response, got %+v", messages)
	}
	var resp ErrorResponse
	if err := messages[0].UnmarshalData(&resp); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if resp.Code != ErrorCodePeerConnectionClosed {
		t.Errorf("error code = %q, want %q", resp.Code, ErrorCodePeerConnectionClosed)
	}
}

// waitForMessageOnSender drains a signaling-sender sink until a message of
// the wanted type shows up or the deadline passes. Gathering runs
// asynchronously, so candidates may arrive before or after the offer.
func waitForMessageOnSender(t *testing.T, sink chan string, want string, context string) {
	t.Helper()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case msgType := <-sink:
			if msgType == want {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %q on %s", want, context)
		}
	}
}
