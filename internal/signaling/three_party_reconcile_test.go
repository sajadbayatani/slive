package signaling

// Room-1skmyz4sms geometry: first publisher (VP8 video), second publisher
// (H264 video), late subscriber on one PC subscribing to both while the
// second publisher's swap+reconcile lands mid-flight. Every browser answers
// every offer here (cooperative loopback): the test asserts the SERVER side
// emits all required offers (especially to a stable, long-published PC that
// only ever subscribed), renegotiates reconciled egress, and delivers RTP
// end to end. A missing server offer with a live subscriber entry is the
// regression signature.
import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pion/rtp"
	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

type threePartyPeer struct {
	part    *domain.Participant
	browser *pionwebrtc.PeerConnection
	offers  chan string
	locals  []*pionwebrtc.TrackLocalStaticRTP
	specs   []glareTrackSpec
	dc      orderingCounters
	stop    chan struct{}
}

func TestThreePartyReconcileDelivers(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	roomID := "three-party-room"
	room, pChrome := joinParticipant(t, h, roomID, "p-chrome")
	_, pSaf2 := joinParticipant(t, h, roomID, "p-saf2")
	_, pLate := joinParticipant(t, h, roomID, "p-late")

	mkSpecs := func(prefix string, videoMime string, ssrcBase uint32) []glareTrackSpec {
		clock := uint32(90000)
		if videoMime == pionwebrtc.MimeTypeOpus {
			clock = 48000
		}
		return []glareTrackSpec{
			{signaledID: prefix + "-audio", kind: "audio", source: "microphone", pt: 111, ssrc: ssrcBase,
				cap: pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}},
			{signaledID: prefix + "-video", kind: "video", source: "camera", pt: 96, ssrc: ssrcBase + 1,
				cap: pionwebrtc.RTPCodecCapability{MimeType: videoMime, ClockRate: clock}},
		}
	}
	chromeSpecs := mkSpecs("tp-chrome", pionwebrtc.MimeTypeVP8, 0xC000)
	saf2Specs := mkSpecs("tp-saf2", pionwebrtc.MimeTypeH264, 0xD000)

	tap := func(ch chan string) func(string, interface{}) error {
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
	peers := map[string]*threePartyPeer{}
	setup := func(part *domain.Participant, specs []glareTrackSpec) *threePartyPeer {
		p := &threePartyPeer{part: part, specs: specs, offers: make(chan string, 16), stop: make(chan struct{})}
		t.Cleanup(func() { close(p.stop) })
		pc, err := h.ensurePeerConnection(part, tap(p.offers))
		if err != nil {
			t.Fatalf("ensure PC %s: %v", part.ID(), err)
		}
		_ = pc
		browser, err := pionwebrtc.NewPeerConnection(orderingPionConfig())
		if err != nil {
			t.Fatalf("browser %s: %v", part.ID(), err)
		}
		t.Cleanup(func() { _ = browser.Close() })
		p.browser = browser
		for _, s := range specs {
			lt, err := pionwebrtc.NewTrackLocalStaticRTP(s.cap, s.signaledID+"-wire", s.signaledID+"-stream")
			if err != nil {
				t.Fatalf("local %s: %v", s.signaledID, err)
			}
			if _, err := browser.AddTrack(lt); err != nil {
				t.Fatalf("browser AddTrack %s: %v", s.signaledID, err)
			}
			p.locals = append(p.locals, lt)
		}
		serverPC := h.getPeerConnection(part.ID())
		orderingWireICE(t, browser, serverPC.PionPeerConnection())
		browser.OnTrack(func(tr *pionwebrtc.TrackRemote, _ *pionwebrtc.RTPReceiver) {
			p.dc.ontrack.Add(1)
			go func() {
				buf := make([]byte, 1600)
				for {
					if _, _, err := tr.Read(buf); err != nil {
						return
					}
					p.dc.received.Add(1)
				}
			}()
		})
		peers[part.ID()] = p
		return p
	}
	chrome := setup(pChrome, chromeSpecs)
	saf2 := setup(pSaf2, saf2Specs)
	late := setup(pLate, nil)

	publish := func(p *threePartyPeer) {
		for _, s := range p.specs {
			payload, _ := json.Marshal(PublishTrackRequest{
				RoomID: roomID, ParticipantID: p.part.ID(),
				Track: TrackInfo{ID: s.signaledID, Kind: s.kind, Source: s.source},
			})
			if err := h.handleMessage(newHeadlessConn(p.part.ID(), roomID), room, p.part,
				&Message{Type: MessageTypePublishTrack, Data: payload}); err != nil {
				t.Fatalf("publish %s: %v", s.signaledID, err)
			}
		}
	}
	subscribe := func(p *threePartyPeer, trackID string) {
		payload, _ := json.Marshal(SubscribeTrackRequest{
			RoomID: roomID, ParticipantID: p.part.ID(), TrackID: trackID,
		})
		if err := h.handleMessage(newHeadlessConn(p.part.ID(), roomID), room, p.part,
			&Message{Type: MessageTypeSubscribeTrack, Data: payload}); err != nil {
			t.Fatalf("subscribe %s: %v", trackID, err)
		}
	}
	publishOffer := func(p *threePartyPeer) {
		offer, err := p.browser.CreateOffer(nil)
		if err != nil {
			t.Fatalf("%s browser offer: %v", p.part.ID(), err)
		}
		if err := p.browser.SetLocalDescription(offer); err != nil {
			t.Fatalf("%s browser SetLocal: %v", p.part.ID(), err)
		}
		payload, _ := json.Marshal(OfferRequest{
			RoomID: roomID, ParticipantID: p.part.ID(),
			TargetParticipantID: p.part.ID(), SDP: p.browser.LocalDescription().SDP,
		})
		conn := newHeadlessConn(p.part.ID(), roomID)
		if err := h.handleMessage(conn, room, p.part,
			&Message{Type: MessageTypeOffer, Data: payload}); err != nil {
			t.Fatalf("%s handleOffer: %v", p.part.ID(), err)
		}
		var answerSDP string
		for _, m := range drainMessages(conn) {
			if m.Type == MessageTypeAnswer {
				var n AnswerNotification
				if err := m.UnmarshalData(&n); err == nil {
					answerSDP = n.SDP
				}
			}
			if m.Type == MessageTypeError {
				var er ErrorResponse
				if err := m.UnmarshalData(&er); err == nil {
					t.Fatalf("%s handleOffer error: %s", p.part.ID(), er.Error)
				}
			}
		}
		if answerSDP == "" {
			t.Fatalf("%s got no publish answer", p.part.ID())
		}
		if err := p.browser.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeAnswer, SDP: answerSDP}); err != nil {
			t.Fatalf("%s browser SetRemote(answer): %v", p.part.ID(), err)
		}
	}
	// completeOffers answers captured server offers until quiet.
	completeOffers := func(p *threePartyPeer) int {
		consumed := 0
		quiet := time.Now()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case offerSDP := <-p.offers:
				if err := p.browser.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeOffer, SDP: offerSDP}); err != nil {
					t.Fatalf("%s SetRemote(server offer): %v", p.part.ID(), err)
				}
				answer, err := p.browser.CreateAnswer(nil)
				if err != nil {
					t.Fatalf("%s CreateAnswer: %v", p.part.ID(), err)
				}
				if err := p.browser.SetLocalDescription(answer); err != nil {
					t.Fatalf("%s SetLocal: %v", p.part.ID(), err)
				}
				ansPayload, _ := json.Marshal(AnswerRequest{
					RoomID: roomID, ParticipantID: p.part.ID(),
					TargetParticipantID: p.part.ID(), SDP: p.browser.LocalDescription().SDP,
				})
				if err := h.handleMessage(newHeadlessConn(p.part.ID(), roomID), room, p.part,
					&Message{Type: MessageTypeAnswer, Data: ansPayload}); err != nil {
					t.Fatalf("%s handleAnswer: %v", p.part.ID(), err)
				}
				consumed++
				quiet = time.Now()
			case <-time.After(200 * time.Millisecond):
				if time.Since(quiet) >= time.Second {
					return consumed
				}
			}
		}
		t.Fatalf("%s offers never quiet", p.part.ID())
		return consumed
	}
	pump := func(p *threePartyPeer) {
		for idx, lt := range p.locals {
			go func(i int, track *pionwebrtc.TrackLocalStaticRTP) {
				seq := uint16(1 + i*1000)
				ts := uint32(90000 + i*100000)
				tick := time.NewTicker(10 * time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-p.stop:
						return
					case <-tick.C:
						pkt := &rtp.Packet{
							Header: rtp.Header{Version: 2, PayloadType: p.specs[i].pt,
								SequenceNumber: seq, Timestamp: ts, SSRC: p.specs[i].ssrc},
							Payload: make([]byte, 120),
						}
						_ = track.WriteRTP(pkt)
						seq++
						ts += 160
					}
				}
			}(idx, lt)
		}
	}
	waitICE := func(p *threePartyPeer) {
		if !orderingWait(t, p.part.ID()+" ICE connected", 10*time.Second, func() bool {
			st := p.browser.ICEConnectionState()
			return st == pionwebrtc.ICEConnectionStateConnected || st == pionwebrtc.ICEConnectionStateCompleted
		}) {
			t.Fatalf("%s ICE never connected", p.part.ID())
		}
	}
	waitRemote := func(trackID string) {
		if !orderingWait(t, "forwarder remote "+trackID, 15*time.Second, func() bool {
			fw := h.getForwarder(trackID)
			return fw != nil && fw.PublisherTrack() != nil && fw.PublisherTrack().IsRemote()
		}) {
			t.Fatalf("forwarder %s never remote-backed", trackID)
		}
	}

	// ---- Chrome publishes first and completes (stable, no subscriptions). ----
	publish(chrome)
	publishOffer(chrome)
	waitICE(chrome)
	pump(chrome)
	for _, s := range chromeSpecs {
		waitRemote(s.signaledID)
	}

	// ---- Late joins: publishes own (nothing) + subscribes to chrome A/V.
	// (Late has no local tracks in this test; it only subscribes.)
	for _, s := range chromeSpecs {
		subscribe(late, s.signaledID)
	}
	if got := completeOffers(late); got < 1 {
		t.Fatalf("late consumed %d offers for chrome tracks, want >=1", got)
	}
	// ---- Safari2 publishes H264 A/V + completes; late subscribes explicitly
	// while the already-connected Chrome PC is attached automatically.
	publish(saf2)
	publishOffer(saf2)
	waitICE(saf2)
	pump(saf2)
	for _, s := range saf2Specs {
		waitRemote(s.signaledID)
	}
	for _, s := range saf2Specs {
		subscribe(late, s.signaledID)
	}
	// Late's incremental offer must arrive (re-offer + new m-lines).
	if got := completeOffers(late); got < 1 {
		t.Fatalf("late consumed %d offers for saf2 tracks, want >=1", got)
	}
	// THE room regression: chrome's stable PC must ALSO be offered its new
	// subscription (reconcile rebuilds its egress; the offer carries it).
	if got := completeOffers(chrome); got < 1 {
		t.Fatalf("REGRESSION: chrome consumed %d offers for saf2 tracks, want >=1 (stable PC got no subscriber offer)", got)
	}

	// ---- Media must flow end to end, incl. through reconciled egress. ----
	if !orderingWait(t, "late ontrack x4", 20*time.Second, func() bool {
		return late.dc.ontrack.Load() >= 4
	}) {
		t.Fatalf("late ontrack=%d, want >=4", late.dc.ontrack.Load())
	}
	if !orderingWait(t, "chrome ontrack x2", 20*time.Second, func() bool {
		return chrome.dc.ontrack.Load() >= 2
	}) {
		t.Fatalf("chrome ontrack=%d, want >=2 (reconciled egress never negotiated?)", chrome.dc.ontrack.Load())
	}
	if !orderingWait(t, "late RTP x40", 20*time.Second, func() bool {
		return late.dc.received.Load() >= 40
	}) {
		t.Fatalf("late received=%d, want >=40", late.dc.received.Load())
	}
	if !orderingWait(t, "chrome RTP x20", 20*time.Second, func() bool {
		return chrome.dc.received.Load() >= 20
	}) {
		t.Fatalf("chrome received=%d, want >=20 (reconciled H264 egress delivers nothing)", chrome.dc.received.Load())
	}
	// No stuck queues anywhere: every forwarded packet must be written.
	for _, s := range append(append([]glareTrackSpec{}, chromeSpecs...), saf2Specs...) {
		fw := h.getForwarder(s.signaledID)
		if fw == nil {
			t.Fatalf("no forwarder %s", s.signaledID)
		}
		recv, fwd, written, dropped, _, _, _, _ := fw.DiagSnapshot()
		t.Logf("forwarder %s recv=%d fwd=%d written=%d dropped=%d", s.signaledID, recv, fwd, written, dropped)
		if fwd > 0 && written < fwd {
			t.Errorf("REGRESSION %s: written=%d < forwarded=%d (writer stuck)", s.signaledID, written, fwd)
		}
	}
}
