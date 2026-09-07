package signaling

// Temporary reproduction probe (kept as a lifecycle regression test): an
// egress TrackLocal whose subscriber offer was never answered must
// void-drain (writer consumes into the unbound track, zero drops). If drops
// mount while written stalls, the writer is stuck — the room-1skmyz4sms
// queue_dropped signature.
import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	pionwebrtc "github.com/pion/webrtc/v3"
)

func TestUnansweredOfferEgressVoidDrains(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	roomID := "void-drain-room"
	room, pub := joinParticipant(t, h, roomID, "pub-void")
	_, sub := joinParticipant(t, h, roomID, "sub-void")

	pubOffers := make(chan string, 8)
	subOffers := make(chan string, 8)
	tapOffers := func(ch chan string) func(string, interface{}) error {
		return func(msgType string, data interface{}) error {
			if msgType == "webrtc:offer" {
				raw, _ := json.Marshal(data)
				var payload struct {
					SDP string `json:"sdp"`
				}
				_ = json.Unmarshal(raw, &payload)
				select {
				case ch <- payload.SDP:
				default:
				}
			}
			return nil
		}
	}
	pubPC, err := h.ensurePeerConnection(pub, tapOffers(pubOffers))
	if err != nil {
		t.Fatalf("ensure pub PC: %v", err)
	}
	if _, err := h.ensurePeerConnection(sub, tapOffers(subOffers)); err != nil {
		t.Fatalf("ensure sub PC: %v", err)
	}

	// Publisher publishes one video track (signaled ID).
	videoID := "void-video-sig"
	pubPayload, _ := json.Marshal(PublishTrackRequest{
		RoomID: roomID, ParticipantID: pub.ID(),
		Track: TrackInfo{ID: videoID, Kind: "video", Source: "camera"},
	})
	if err := h.handleMessage(newHeadlessConn(pub.ID(), roomID), room, pub,
		&Message{Type: MessageTypePublishTrack, Data: pubPayload}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Subscriber subscribes BEFORE any offer exists: provisional egress +
	// exactly one server offer, which the test deliberately never answers.
	subPayload, _ := json.Marshal(SubscribeTrackRequest{
		RoomID: roomID, ParticipantID: sub.ID(), TrackID: videoID,
	})
	if err := h.handleMessage(newHeadlessConn(sub.ID(), roomID), room, sub,
		&Message{Type: MessageTypeSubscribeTrack, Data: subPayload}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case <-subOffers:
	case <-time.After(10 * time.Second):
		t.Fatal("no subscriber offer emitted")
	}

	// Publisher browser completes publish with real media (VP8-loopback).
	browser, err := pionwebrtc.NewPeerConnection(orderingPionConfig())
	if err != nil {
		t.Fatalf("browser pc: %v", err)
	}
	t.Cleanup(func() { _ = browser.Close() })
	lt, err := pionwebrtc.NewTrackLocalStaticRTP(
		pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeVP8, ClockRate: 90000},
		"void-video-wire", "void-stream")
	if err != nil {
		t.Fatalf("browser local: %v", err)
	}
	if _, err := browser.AddTrack(lt); err != nil {
		t.Fatalf("browser AddTrack: %v", err)
	}
	orderingWireICE(t, browser, pubPC.PionPeerConnection())
	offer, err := browser.CreateOffer(nil)
	if err != nil {
		t.Fatalf("browser offer: %v", err)
	}
	if err := browser.SetLocalDescription(offer); err != nil {
		t.Fatalf("browser SetLocal: %v", err)
	}
	offerPayload, _ := json.Marshal(OfferRequest{
		RoomID: roomID, ParticipantID: pub.ID(),
		TargetParticipantID: pub.ID(), SDP: browser.LocalDescription().SDP,
	})
	offerConn := newHeadlessConn(pub.ID(), roomID)
	if err := h.handleMessage(offerConn, room, pub,
		&Message{Type: MessageTypeOffer, Data: offerPayload}); err != nil {
		t.Fatalf("handleOffer: %v", err)
	}
	var answerSDP string
	for _, m := range drainMessages(offerConn) {
		if m.Type == MessageTypeAnswer {
			var n AnswerNotification
			if err := m.UnmarshalData(&n); err == nil {
				answerSDP = n.SDP
			}
		}
	}
	if answerSDP == "" {
		t.Fatal("no publisher answer")
	}
	if err := browser.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeAnswer, SDP: answerSDP}); err != nil {
		t.Fatalf("browser SetRemote(answer): %v", err)
	}
	if !orderingWait(t, "pub ICE connected", 10*time.Second, func() bool {
		st := browser.ICEConnectionState()
		return st == pionwebrtc.ICEConnectionStateConnected || st == pionwebrtc.ICEConnectionStateCompleted
	}) {
		t.Fatal("publisher ICE never connected")
	}
	// Pump producer RTP.
	stopPump := make(chan struct{})
	defer close(stopPump)
	var sent atomic.Uint64
	go func() {
		seq := uint16(1)
		ts := uint32(90000)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopPump:
				return
			case <-tick.C:
				pkt := &rtp.Packet{
					Header: rtp.Header{Version: 2, PayloadType: 96,
						SequenceNumber: seq, Timestamp: ts, SSRC: 0xBEEF},
					Payload: make([]byte, 120),
				}
				if err := lt.WriteRTP(pkt); err == nil {
					sent.Add(1)
				}
				seq++
				ts += 3000
			}
		}
	}()
	fw := h.getForwarder(videoID)
	if fw == nil {
		t.Fatal("no forwarder")
	}
	if !orderingWait(t, "forwarder remote-backed", 15*time.Second, func() bool {
		pt := fw.PublisherTrack()
		return pt != nil && pt.IsRemote()
	}) {
		t.Fatal("forwarder never became remote-backed")
	}
	// Producer must be flowing into the forwarder.
	if !orderingWait(t, "forwarder receiving", 10*time.Second, func() bool {
		return fw.PacketsReceived() >= 20
	}) {
		t.Fatalf("forwarder received=%d, producer not flowing", fw.PacketsReceived())
	}
	// Observe the unnegotiated egress for 3s: theory says void-drain
	// (written tracks forwarded, dropped stays 0).
	time.Sleep(3 * time.Second)
	recv, fwd, written, dropped, _, _, _, _ := fw.DiagSnapshot()
	t.Logf("snapshot recv=%d fwd=%d written=%d dropped=%d", recv, fwd, written, dropped)
	if dropped != 0 {
		t.Errorf("BUG REPRODUCED: dropped=%d on unnegotiated egress (want 0 void-drain)", dropped)
	}
	if written < fwd {
		t.Errorf("BUG REPRODUCED: written=%d < forwarded=%d (writer stuck)", written, fwd)
	}
}

// TestAnsweredThenUnansweredOfferEgress bisects the room-1skmyz4sms stuck
// queue: offer#1 (audio) negotiated, then offer#2 (video) left unanswered
// while the video producer pumps. If the video egress sticks here, the
// multi-offer state (not the reconcile) is the trigger.
func TestAnsweredThenUnansweredOfferEgress(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	roomID := "p2-drain-room"
	room, pub := joinParticipant(t, h, roomID, "pub-p2")
	_, sub := joinParticipant(t, h, roomID, "sub-p2")

	answerSink := make(chan string, 8) // captured server offers
	tapOffers := func(msgType string, data interface{}) error {
		if msgType == "webrtc:offer" {
			raw, _ := json.Marshal(data)
			var payload struct {
				SDP string `json:"sdp"`
			}
			_ = json.Unmarshal(raw, &payload)
			select {
			case answerSink <- payload.SDP:
			default:
			}
		}
		return nil
	}
	pubPC, err := h.ensurePeerConnection(pub, tapOffers)
	if err != nil {
		t.Fatalf("ensure pub PC: %v", err)
	}
	subPC, err := h.ensurePeerConnection(sub, tapOffers)
	if err != nil {
		t.Fatalf("ensure sub PC: %v", err)
	}
	_ = pubPC
	_ = subPC

	audioID, videoID := "p2-audio-sig", "p2-video-sig"
	for _, tc := range []struct{ id, kind, source string }{
		{audioID, "audio", "microphone"},
		{videoID, "video", "camera"},
	} {
		payload, _ := json.Marshal(PublishTrackRequest{
			RoomID: roomID, ParticipantID: pub.ID(),
			Track: TrackInfo{ID: tc.id, Kind: tc.kind, Source: tc.source},
		})
		if err := h.handleMessage(newHeadlessConn(pub.ID(), roomID), room, pub,
			&Message{Type: MessageTypePublishTrack, Data: payload}); err != nil {
			t.Fatalf("publish %s: %v", tc.id, err)
		}
	}

	// Loopback subscriber browser: answers offer#1, ignores offer#2.
	browser, err := pionwebrtc.NewPeerConnection(orderingPionConfig())
	if err != nil {
		t.Fatalf("browser pc: %v", err)
	}
	t.Cleanup(func() { _ = browser.Close() })

	subscribe := func(trackID string) {
		payload, _ := json.Marshal(SubscribeTrackRequest{
			RoomID: roomID, ParticipantID: sub.ID(), TrackID: trackID,
		})
		if err := h.handleMessage(newHeadlessConn(sub.ID(), roomID), room, sub,
			&Message{Type: MessageTypeSubscribeTrack, Data: payload}); err != nil {
			t.Fatalf("subscribe %s: %v", trackID, err)
		}
	}
	waitOffer := func() string {
		select {
		case sdp := <-answerSink:
			return sdp
		case <-time.After(10 * time.Second):
			t.Fatal("no server offer emitted")
			return ""
		}
	}
	answerOffer := func(offerSDP string) {
		if err := browser.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeOffer, SDP: offerSDP}); err != nil {
			t.Fatalf("browser SetRemote: %v", err)
		}
		ans, err := browser.CreateAnswer(nil)
		if err != nil {
			t.Fatalf("browser CreateAnswer: %v", err)
		}
		if err := browser.SetLocalDescription(ans); err != nil {
			t.Fatalf("browser SetLocal: %v", err)
		}
		ansPayload, _ := json.Marshal(AnswerRequest{
			RoomID: roomID, ParticipantID: sub.ID(),
			TargetParticipantID: sub.ID(), SDP: browser.LocalDescription().SDP,
		})
		if err := h.handleMessage(newHeadlessConn(sub.ID(), roomID), room, sub,
			&Message{Type: MessageTypeAnswer, Data: ansPayload}); err != nil {
			t.Fatalf("handleAnswer: %v", err)
		}
	}

	subscribe(audioID)
	answerOffer(waitOffer()) // offer#1 answered: mid:0 audio negotiated

	// Publisher browser completes publish with real audio+video media.
	pubBrowser, err := pionwebrtc.NewPeerConnection(orderingPionConfig())
	if err != nil {
		t.Fatalf("pub browser pc: %v", err)
	}
	t.Cleanup(func() { _ = pubBrowser.Close() })
	mkLocal := func(mime string, clock uint32, pt uint8, id string) *pionwebrtc.TrackLocalStaticRTP {
		lt, err := pionwebrtc.NewTrackLocalStaticRTP(
			pionwebrtc.RTPCodecCapability{MimeType: mime, ClockRate: clock, Channels: 2}, id, id+"-stream")
		if err != nil {
			t.Fatalf("pub local %s: %v", id, err)
		}
		if _, err := pubBrowser.AddTrack(lt); err != nil {
			t.Fatalf("pub AddTrack %s: %v", id, err)
		}
		return lt
	}
	audioLT := mkLocal(pionwebrtc.MimeTypeOpus, 48000, 111, "p2-audio-wire")
	videoLT := mkLocal(pionwebrtc.MimeTypeVP8, 90000, 96, "p2-video-wire")
	orderingWireICE(t, pubBrowser, pubPC.PionPeerConnection())
	pubOffer, err := pubBrowser.CreateOffer(nil)
	if err != nil {
		t.Fatalf("pub offer: %v", err)
	}
	if err := pubBrowser.SetLocalDescription(pubOffer); err != nil {
		t.Fatalf("pub SetLocal: %v", err)
	}
	offerPayload, _ := json.Marshal(OfferRequest{
		RoomID: roomID, ParticipantID: pub.ID(),
		TargetParticipantID: pub.ID(), SDP: pubBrowser.LocalDescription().SDP,
	})
	offerConn := newHeadlessConn(pub.ID(), roomID)
	if err := h.handleMessage(offerConn, room, pub,
		&Message{Type: MessageTypeOffer, Data: offerPayload}); err != nil {
		t.Fatalf("handleOffer: %v", err)
	}
	var pubAnswer string
	for _, m := range drainMessages(offerConn) {
		if m.Type == MessageTypeAnswer {
			var n AnswerNotification
			if err := m.UnmarshalData(&n); err == nil {
				pubAnswer = n.SDP
			}
		}
	}
	if pubAnswer == "" {
		t.Fatal("no publisher answer")
	}
	if err := pubBrowser.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeAnswer, SDP: pubAnswer}); err != nil {
		t.Fatalf("pub SetRemote(answer): %v", err)
	}
	if !orderingWait(t, "pub ICE connected", 10*time.Second, func() bool {
		st := pubBrowser.ICEConnectionState()
		return st == pionwebrtc.ICEConnectionStateConnected || st == pionwebrtc.ICEConnectionStateCompleted
	}) {
		t.Fatal("publisher ICE never connected")
	}
	// Pump both producer tracks.
	stopPump := make(chan struct{})
	defer close(stopPump)
	pump := func(lt *pionwebrtc.TrackLocalStaticRTP, pt uint8, ssrc uint32) {
		go func() {
			seq := uint16(1)
			ts := uint32(90000)
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-stopPump:
					return
				case <-tick.C:
					pkt := &rtp.Packet{
						Header: rtp.Header{Version: 2, PayloadType: pt,
							SequenceNumber: seq, Timestamp: ts, SSRC: ssrc},
						Payload: make([]byte, 120),
					}
					_ = lt.WriteRTP(pkt)
					seq++
					ts += 160
				}
			}
		}()
	}
	pump(audioLT, 111, 0xA001)
	pump(videoLT, 96, 0xA002)

	// Now subscribe video: offer#2 goes out and is deliberately NEVER
	// answered (room geometry). Producer keeps pumping both tracks.
	subscribe(videoID)
	select {
	case <-answerSink:
	case <-time.After(10 * time.Second):
		t.Fatal("no second server offer emitted")
	}
	time.Sleep(3 * time.Second)
	for _, tc := range []struct {
		id   string
		want bool // want drops==0 (void-drain theory)
	}{
		{audioID, true},
		{videoID, true},
	} {
		fw := h.getForwarder(tc.id)
		if fw == nil {
			t.Fatalf("no forwarder %s", tc.id)
		}
		recv, fwd, written, dropped, _, _, _, _ := fw.DiagSnapshot()
		t.Logf("forwarder %s snapshot recv=%d fwd=%d written=%d dropped=%d", tc.id, recv, fwd, written, dropped)
		if tc.want && dropped != 0 {
			t.Errorf("BUG REPRODUCED on %s: dropped=%d (want 0)", tc.id, dropped)
		}
		if tc.want && fwd > 0 && written < fwd {
			t.Errorf("BUG REPRODUCED on %s: written=%d < forwarded=%d (writer stuck)", tc.id, written, fwd)
		}
	}
}
