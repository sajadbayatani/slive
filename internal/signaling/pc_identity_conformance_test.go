package signaling

// DIAG-PCID conformance: proves signaling routes by participant ID to exactly
// one PeerConnection object, shared by publisher and subscriber flows.
//
// Background: the hypothesis under test was that Slive keeps separate
// Publisher and Subscriber PeerConnections per participant and that
// participant-keyed signaling lookup may hit the wrong one. The Handler
// registry (peerConnections map[string]*PeerConnection, keyed by participant
// ID; ensurePeerConnection reuses-or-replaces) says otherwise. These tests pin
// that invariant with pointer identity through a full real-WebRTC flow.

import (
	"encoding/json"
	"testing"
	"time"

	pionwebrtc "github.com/pion/webrtc/v3"
)

// TestPCIdentity_SingleLivePCPerParticipant drives the complete
// publish → OnTrack → subscribe → subscriber-offer → answer → ICE flow with
// real pion PeerConnections and asserts every step touches the SAME
// *PeerConnection object for the participant.
func TestPCIdentity_SingleLivePCPerParticipant(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	const roomID = "pcid-room-1"

	room, publisher := joinParticipant(t, h, roomID, "pub-pcid")
	_, subscriber := joinParticipant(t, h, roomID, "sub-pcid")

	pubPC, err := h.ensurePeerConnection(publisher, channelSender(make(chan string, 16)))
	if err != nil {
		t.Fatalf("ensure pub PC: %v", err)
	}
	if pubPC.InstanceID() == "" {
		t.Fatal("publisher PC has no instance ID")
	}

	// Re-ensure must return the identical object (no duplicate PCs).
	pubPC2, err := h.ensurePeerConnection(publisher, channelSender(make(chan string, 16)))
	if err != nil {
		t.Fatalf("re-ensure pub PC: %v", err)
	}
	if pubPC2 != pubPC {
		t.Fatal("ensurePeerConnection created a second PC for the same participant")
	}

	subOffers := make(chan string, 8)
	subPC, err := h.ensurePeerConnection(subscriber, func(msgType string, data interface{}) error {
		if msgType == "webrtc:offer" {
			raw, _ := json.Marshal(data)
			var payload struct {
				SDP string `json:"sdp"`
			}
			_ = json.Unmarshal(raw, &payload)
			select {
			case subOffers <- payload.SDP:
			default:
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ensure sub PC: %v", err)
	}
	if subPC.InstanceID() == pubPC.InstanceID() {
		t.Fatal("distinct participants share a PC instance ID")
	}

	const trackID = "pcid-audio-1"
	payload, _ := json.Marshal(PublishTrackRequest{
		RoomID: roomID, ParticipantID: publisher.ID(),
		Track: TrackInfo{ID: trackID, Kind: "audio", Source: "microphone"},
	})
	if err := h.handleMessage(newHeadlessConn(publisher.ID(), roomID), room, publisher,
		&Message{Type: MessageTypePublishTrack, Data: payload}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Publisher browser negotiates with the server PC (mirrors handleOffer).
	pubBrowser, err := pionwebrtc.NewPeerConnection(orderingPionConfig())
	if err != nil {
		t.Fatalf("pubBrowser: %v", err)
	}
	t.Cleanup(func() { _ = pubBrowser.Close() })
	lt, _ := pionwebrtc.NewTrackLocalStaticRTP(
		pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		trackID, trackID+"-stream")
	if _, err := pubBrowser.AddTrack(lt); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}
	orderingWireICE(t, pubBrowser, pubPC.PionPeerConnection())
	pubOffer, _ := pubBrowser.CreateOffer(nil)
	_ = pubBrowser.SetLocalDescription(pubOffer)
	offerPayload, _ := json.Marshal(OfferRequest{
		RoomID: roomID, ParticipantID: publisher.ID(),
		TargetParticipantID: publisher.ID(), SDP: pubBrowser.LocalDescription().SDP,
	})
	offerConn := newHeadlessConn(publisher.ID(), roomID)
	if err := h.handleMessage(offerConn, room, publisher, &Message{Type: MessageTypeOffer, Data: offerPayload}); err != nil {
		t.Fatalf("handleOffer: %v", err)
	}
	var pubAnswerSDP string
	for _, m := range drainMessages(offerConn) {
		if m.Type == MessageTypeAnswer {
			var n AnswerNotification
			if err := m.UnmarshalData(&n); err == nil {
				pubAnswerSDP = n.SDP
			}
		}
	}
	if pubAnswerSDP == "" {
		t.Fatal("no publisher answer")
	}
	_ = pubBrowser.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeAnswer, SDP: pubAnswerSDP})

	// The handler registry must still reference the identical publisher object.
	if got := h.getPeerConnection(publisher.ID()); got != pubPC {
		t.Fatal("publisher PC object changed across offer/answer")
	}

	// Subscribe: the subscriber flow must use the subscriber's own (single) PC.
	subPayload, _ := json.Marshal(SubscribeTrackRequest{
		RoomID: roomID, ParticipantID: subscriber.ID(), TrackID: trackID,
	})
	if err := h.handleMessage(newHeadlessConn(subscriber.ID(), roomID), room, subscriber,
		&Message{Type: MessageTypeSubscribeTrack, Data: subPayload}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if got := h.getPeerConnection(subscriber.ID()); got != subPC {
		t.Fatal("subscriber PC object changed across subscribe")
	}
	// Publisher and subscriber participants resolve to DIFFERENT objects.
	if subPC == pubPC {
		t.Fatal("publisher and subscriber share one PC object across participants")
	}

	// Registry cardinality: exactly 2 PCs total, one per participant.
	h.peerConnectionsMutex.RLock()
	total := len(h.peerConnections)
	h.peerConnectionsMutex.RUnlock()
	if total != 2 {
		t.Fatalf("registered PCs = %d, want exactly 2 (one per participant)", total)
	}

	// One object carries BOTH the publisher remote track and the subscriber
	// local track — i.e. roles share the PC; there is no separate subscriber PC.
	subBrowser, err := pionwebrtc.NewPeerConnection(orderingPionConfig())
	if err != nil {
		t.Fatalf("subBrowser: %v", err)
	}
	t.Cleanup(func() { _ = subBrowser.Close() })
	orderingWireICE(t, subBrowser, subPC.PionPeerConnection())
	select {
	case offerSDP := <-subOffers:
		if err := subBrowser.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeOffer, SDP: offerSDP}); err != nil {
			t.Fatalf("sub SetRemote: %v", err)
		}
		answer, _ := subBrowser.CreateAnswer(nil)
		_ = subBrowser.SetLocalDescription(answer)
		ansPayload, _ := json.Marshal(AnswerRequest{
			RoomID: roomID, ParticipantID: subscriber.ID(),
			TargetParticipantID: subscriber.ID(), SDP: subBrowser.LocalDescription().SDP,
		})
		if err := h.handleMessage(newHeadlessConn(subscriber.ID(), roomID), room, subscriber,
			&Message{Type: MessageTypeAnswer, Data: ansPayload}); err != nil {
			t.Fatalf("handleAnswer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no subscriber offer pushed")
	}

	// Object identity after the full flow: still the same two objects.
	if got := h.getPeerConnection(publisher.ID()); got != pubPC {
		t.Fatal("publisher PC object changed after full flow")
	}
	if got := h.getPeerConnection(subscriber.ID()); got != subPC {
		t.Fatal("subscriber PC object changed after full flow")
	}
	// The subscriber flow's local (forwarded) track lives on the subscriber's
	// single PC — the same object that would receive its ICE/answer.
	if tr := subPC.GetLocalTrack(trackID); tr == nil {
		t.Fatal("subscriber PC missing forwarded local track")
	}
	t.Logf("[PCID] publisher=%s subscriber=%s single-PC-per-participant proven by pointer identity",
		pubPC.InstanceID(), subPC.InstanceID())
}

// TestPCIdentity_ReplacedAfterClose proves replacement (not duplication) when
// the existing PC becomes unusable: the registry holds the new object only.
func TestPCIdentity_ReplacedAfterClose(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	_, p := joinParticipant(t, h, "pcid-room-2", "p-replace")
	pc1, err := h.ensurePeerConnection(p, channelSender(make(chan string, 8)))
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	id1 := pc1.InstanceID()
	if err := pc1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	pc2, err := h.ensurePeerConnection(p, channelSender(make(chan string, 8)))
	if err != nil {
		t.Fatalf("re-ensure: %v", err)
	}
	if pc2 == pc1 {
		t.Fatal("closed PC was reused instead of replaced")
	}
	if pc2.InstanceID() == id1 {
		t.Fatal("replacement PC reuses instance ID")
	}
	h.peerConnectionsMutex.RLock()
	n := len(h.peerConnections)
	stored := h.peerConnections[p.ID()]
	h.peerConnectionsMutex.RUnlock()
	if n != 1 || stored != pc2 {
		t.Fatalf("registry holds %d PCs, want exactly the replacement", n)
	}
	t.Logf("[PCID] replaced %s -> %s, registry holds exactly one", id1, pc2.InstanceID())
}
