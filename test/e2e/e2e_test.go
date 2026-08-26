package e2e

import (
	"strings"
	"testing"
	"time"

	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/signaling"
)

// unmarshalInto decodes a message payload into out, failing the test on
// malformed server output.
func unmarshalInto(t *testing.T, msg *signaling.Message, out interface{}) {
	t.Helper()

	if err := msg.UnmarshalData(out); err != nil {
		t.Fatalf("unmarshal %s payload %s: %v", msg.Type, msg.Data, err)
	}
}

// TestTwoParticipantsControlPlaneFlow walks the full ordered control-plane
// lifecycle of two participants against the REAL mounted router: joins,
// presence, track publication/subscription, SFU-style offer→answer exchange,
// and ICE forwarding in both directions (client→server and the automatic
// server→client push of gathered candidates).
func TestTwoParticipantsControlPlaneFlow(t *testing.T) {
	ts := newE2EServer(t)

	alice := dialWS(t, ts, "e2e-room", "p-alice")

	// 1. First join: room_joined lists the joiner herself.
	msg := alice.receiveOfType(signaling.MessageTypeRoomJoined, "alice room_joined")
	var joined signaling.RoomJoinedResponse
	unmarshalInto(t, msg, &joined)
	if joined.ParticipantID != "p-alice" || joined.RoomID != "e2e-room" {
		t.Errorf("alice room_joined = %+v, want p-alice/e2e-room", joined)
	}
	if !containsParticipant(joined.Participants, "p-alice") {
		t.Errorf("alice room_joined participants = %+v, want it to include p-alice", joined.Participants)
	}

	// 2. Second participant joins.
	bob := dialWS(t, ts, "e2e-room", "p-bob")

	bobJoined := bob.receiveOfType(signaling.MessageTypeRoomJoined, "bob room_joined")
	var bobJoinedResp signaling.RoomJoinedResponse
	unmarshalInto(t, bobJoined, &bobJoinedResp)
	if bobJoinedResp.ParticipantID != "p-bob" {
		t.Errorf("bob room_joined = %+v, want p-bob", bobJoinedResp)
	}
	if !containsParticipant(bobJoinedResp.Participants, "p-alice") ||
		!containsParticipant(bobJoinedResp.Participants, "p-bob") {
		t.Errorf("bob room_joined participants = %+v, want both members", bobJoinedResp.Participants)
	}

	// Alice sees Bob's presence.
	presence := alice.receiveOfType(signaling.MessageTypeParticipantJoined, "alice participant_joined")
	var presenceNote signaling.ParticipantJoinedNotification
	unmarshalInto(t, presence, &presenceNote)
	if presenceNote.Participant.ID != "p-bob" {
		t.Errorf("participant_joined = %+v, want p-bob", presenceNote.Participant)
	}

	// 3. Alice publishes an audio track.
	alice.send(signaling.MessageTypePublishTrack, signaling.PublishTrackRequest{
		RoomID:        "e2e-room",
		ParticipantID: "p-alice",
		Track: signaling.TrackInfo{
			ID:     "audio-1",
			Kind:   "audio",
			Source: "microphone",
		},
	})
	published := alice.receiveOfType(signaling.MessageTypeTrackPublished, "alice track_published")
	var publishedResp signaling.TrackPublishedResponse
	unmarshalInto(t, published, &publishedResp)
	if publishedResp.TrackID != "audio-1" || publishedResp.Status != "success" {
		t.Errorf("track_published = %+v, want audio-1/success", publishedResp)
	}

	// ...and Bob learns about it.
	available := bob.receiveOfType(signaling.MessageTypeTrackAvailable, "bob track_available")
	var availableNote signaling.TrackAvailableNotification
	unmarshalInto(t, available, &availableNote)
	if availableNote.Track.ID != "audio-1" || availableNote.ParticipantID != "p-alice" {
		t.Errorf("track_available = %+v, want audio-1 from p-alice", availableNote)
	}

	// 4. Bob subscribes to the track.
	bob.send(signaling.MessageTypeSubscribeTrack, signaling.SubscribeTrackRequest{
		RoomID:        "e2e-room",
		ParticipantID: "p-alice",
		TrackID:       "audio-1",
	})
	subscribed := bob.receiveOfType(signaling.MessageTypeTrackSubscribed, "bob track_subscribed")
	var subscribedResp signaling.TrackSubscribedResponse
	unmarshalInto(t, subscribed, &subscribedResp)
	if subscribedResp.TrackID != "audio-1" || subscribedResp.Status != "success" {
		t.Errorf("track_subscribed = %+v, want audio-1/success", subscribedResp)
	}

	// 5. Offer relayed → real answer SDP received. Bob plays the browser:
	// a plain pion offer whose local description carries gathered host
	// candidates, sent through the WebSocket targeting Alice.
	offerPC := createLoopbackOfferPC(t)
	offer := createLoopbackOfferWithPC(t, offerPC)

	bob.send(signaling.MessageTypeOffer, signaling.OfferRequest{
		RoomID:              "e2e-room",
		ParticipantID:       "p-bob",
		TargetParticipantID: "p-alice",
		SDP:                 offer.SDP,
	})

	answerMsg := bob.receiveOfType(signaling.MessageTypeAnswer, "bob webrtc:answer")
	var answerNote signaling.AnswerNotification
	unmarshalInto(t, answerMsg, &answerNote)
	if answerNote.SourceParticipantID != "p-alice" {
		t.Errorf("answer source_participant_id = %q, want p-alice", answerNote.SourceParticipantID)
	}
	for _, fragment := range []string{"m=audio", "a=ice-ufrag"} {
		if !strings.Contains(answerNote.SDP, fragment) {
			t.Errorf("server-generated answer missing %q:\n%s", fragment, answerNote.SDP)
		}
	}
	// SFU-style semantics (Q-2): the server answers on behalf of the target;
	// the client's offer must never come back verbatim.
	if strings.TrimSpace(answerNote.SDP) == strings.TrimSpace(offer.SDP) {
		t.Error("answer equals the client offer; expected a server-generated SDP")
	}

	// Applying the answer on the offering side proves the SDP round-trips as
	// a valid remote description — the strongest offline contract available.
	if err := offerPC.SetRemoteDescription(pionwebrtc.SessionDescription{
		Type: pionwebrtc.SDPTypeAnswer,
		SDP:  answerNote.SDP,
	}); err != nil {
		t.Fatalf("offerer SetRemoteDescription(answer): %v\nanswer:\n%s", err, answerNote.SDP)
	}

	// 6. Automatic outbound push: while answering for Alice's server-side
	// peer connection, gathered local candidates are pushed over her live
	// WebSocket without any client action.
	waitFor(t, e2eTimeout, "pushed ICE candidate on alice's socket", func() bool {
		if err := alice.conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			t.Fatalf("SetReadDeadline (alice candidate): %v", err)
		}
		_, data, err := alice.conn.ReadMessage()
		if err != nil {
			return false // deadline tick; poll again
		}
		pushed, err := signaling.ParseMessage(data)
		if err != nil {
			t.Fatalf("parse pushed message %q: %v", data, err)
		}
		if pushed.Type != signaling.MessageTypeICECandidate {
			t.Fatalf("unexpected message while waiting for pushed candidate: type=%s data=%s",
				pushed.Type, data)
		}
		var note signaling.ICECandidateNotification
		unmarshalInto(t, pushed, &note)
		return note.Candidate != ""
	})

	// 7. Client→server ICE forwarding: Bob targets Alice's peer connection;
	// acceptance is silent — any failure would produce an error response.
	bob.send(signaling.MessageTypeICECandidate, signaling.ICECandidateRequest{
		RoomID:              "e2e-room",
		ParticipantID:       "p-bob",
		TargetParticipantID: "p-alice",
		Candidate:           "candidate:842163049 1 udp 1677729535 192.0.2.1 31102 typ srflx",
		SDPMid:              "0",
		SDPMLineIndex:       0,
	})
	bob.expectSilence("ice forwarding acknowledgement", 750*time.Millisecond)
}

// containsParticipant reports whether the ID appears in the list.
func containsParticipant(list []signaling.ParticipantInfo, id string) bool {
	for _, p := range list {
		if p.ID == id {
			return true
		}
	}
	return false
}

// createLoopbackOfferPC returns a STUN-free pion peer connection with one
// audio transceiver, mimicking a browser client.
func createLoopbackOfferPC(t *testing.T) *pionwebrtc.PeerConnection {
	t.Helper()

	pc, err := pionwebrtc.NewPeerConnection(pionwebrtc.Configuration{})
	if err != nil {
		t.Fatalf("NewPeerConnection(offerer): %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })

	if _, err := pc.AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("AddTransceiverFromKind: %v", err)
	}
	return pc
}

// createLoopbackOfferWithPC produces a complete offer SDP (local description
// awaited through the gathering-complete promise so host candidates are in).
func createLoopbackOfferWithPC(t *testing.T, pc *pionwebrtc.PeerConnection) pionwebrtc.SessionDescription {
	t.Helper()

	gathered := pionwebrtc.GatheringCompletePromise(pc)
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("SetLocalDescription(offer): %v", err)
	}
	<-gathered

	local := pc.LocalDescription()
	if local == nil {
		t.Fatal("no local description after gathering")
	}
	return *local
}
