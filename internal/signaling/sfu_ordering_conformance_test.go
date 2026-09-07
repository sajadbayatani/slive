package signaling

// Conformance tests for publish_track / WebRTC OnTrack ordering robustness.
//
// Exactly one logical forwarder must exist per published track, it must wrap
// the real TrackRemote, and subscribers attached under the signaled ID must
// receive RTP — regardless of message ordering and regardless of whether the
// browser wire track ID matches the signaled track ID.
//
// These tests drive REAL pion PeerConnections (DTLS/SRTP/RTP over loopback)
// through the actual Handler + TrackForwarder path and assert on the
// diagnostic packet counters (PacketsReceived/Forwarded/Written).

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	pionwebrtc "github.com/pion/webrtc/v3"
)

type orderingSpec struct {
	signaledID string
	wireID     string
	kind       string // "audio" | "video"
	source     string
	pt         uint8
	cap        pionwebrtc.RTPCodecCapability
}

type orderingCounters struct {
	received atomic.Uint64
	ontrack  atomic.Uint64
}

func orderingPionConfig() pionwebrtc.Configuration {
	return pionwebrtc.Configuration{
		ICEServers:   []pionwebrtc.ICEServer{},
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	}
}

func orderingWireICE(t *testing.T, a *pionwebrtc.PeerConnection, b *pionwebrtc.PeerConnection) {
	t.Helper()
	a.OnICECandidate(func(c *pionwebrtc.ICECandidate) {
		if c == nil {
			return
		}
		_ = b.AddICECandidate(c.ToJSON())
	})
	b.OnICECandidate(func(c *pionwebrtc.ICECandidate) {
		if c == nil {
			return
		}
		_ = a.AddICECandidate(c.ToJSON())
	})
}

func orderingWait(t *testing.T, what string, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Logf("TIMEOUT waiting for %s after %s", what, timeout)
	return false
}

// runOrderingCase executes one publish/OnTrack ordering scenario end to end.
func runOrderingCase(t *testing.T, name, roomID string, kinds []string, publishFirst, matchIDs, subscribeEarly bool) {
	t.Helper()
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })

	room, publisher := joinParticipant(t, h, roomID, "pub-"+name)
	_, subscriber := joinParticipant(t, h, roomID, "sub-"+name)

	subOffers := make(chan string, 8)
	pubPC, err := h.ensurePeerConnection(publisher, channelSender(make(chan string, 16)))
	if err != nil {
		t.Fatalf("ensure pub PC: %v", err)
	}
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
	_ = subPC

	var specs []orderingSpec
	for i, k := range kinds {
		sig := strings.ReplaceAll(name+"-"+k, "_", "-") + "-sig"
		wire := sig
		if !matchIDs {
			wire = sig + "-wire-uuid"
		}
		if k == "audio" {
			specs = append(specs, orderingSpec{
				signaledID: sig, wireID: wire, kind: "audio", source: "microphone", pt: 111,
				cap: pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
			})
		} else {
			specs = append(specs, orderingSpec{
				signaledID: sig, wireID: wire, kind: "video", source: "camera", pt: 96,
				cap: pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeVP8, ClockRate: 90000},
			})
		}
		_ = i
	}

	publishAll := func() {
		for _, s := range specs {
			payload, _ := json.Marshal(PublishTrackRequest{
				RoomID: roomID, ParticipantID: publisher.ID(),
				Track: TrackInfo{ID: s.signaledID, Kind: s.kind, Source: s.source},
			})
			conn := newHeadlessConn(publisher.ID(), roomID)
			if err := h.handleMessage(conn, room, publisher, &Message{Type: MessageTypePublishTrack, Data: payload}); err != nil {
				t.Fatalf("publish %s: %v", s.signaledID, err)
			}
		}
	}

	// Subscriber browser PC; completes every server offer via handleAnswer.
	subBrowser, err := pionwebrtc.NewPeerConnection(orderingPionConfig())
	if err != nil {
		t.Fatalf("subBrowser: %v", err)
	}
	t.Cleanup(func() { _ = subBrowser.Close() })
	orderingWireICE(t, subBrowser, subPC.PionPeerConnection())
	var dc orderingCounters
	subBrowser.OnTrack(func(tr *pionwebrtc.TrackRemote, recv *pionwebrtc.RTPReceiver) {
		dc.ontrack.Add(1)
		go func() {
			buf := make([]byte, 1600)
			for {
				n, _, err := tr.Read(buf)
				if err != nil {
					return
				}
				_ = n
				dc.received.Add(1)
			}
		}()
	})

	subscribeAll := func() {
		for _, s := range specs {
			payload, _ := json.Marshal(SubscribeTrackRequest{
				RoomID: roomID, ParticipantID: subscriber.ID(), TrackID: s.signaledID,
			})
			conn := newHeadlessConn(subscriber.ID(), roomID)
			if err := h.handleMessage(conn, room, subscriber, &Message{Type: MessageTypeSubscribeTrack, Data: payload}); err != nil {
				t.Fatalf("subscribe %s: %v", s.signaledID, err)
			}
		}
	}
	// completeSubOffers answers server offers until the offer stream goes
	// quiet (and, when requireMedia is set, until every expected track fired
	// ontrack). It does not assume one offer per track: back-to-back
	// subscribes may be coalesced by negotiation into a single offer carrying
	// several m-lines. Note pion fires the browser OnTrack only once RTP
	// actually arrives, so the early-subscribe path must not wait for ontrack
	// (no publisher RTP exists yet) — quiet alone is the exit signal there.
	completeSubOffers := func(wantTracks int, requireMedia bool) int {
		consumed := 0
		quiet := time.Now()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case offerSDP := <-subOffers:
				if err := subBrowser.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeOffer, SDP: offerSDP}); err != nil {
					t.Fatalf("sub SetRemote: %v", err)
				}
				answer, err := subBrowser.CreateAnswer(nil)
				if err != nil {
					t.Fatalf("sub CreateAnswer: %v", err)
				}
				if err := subBrowser.SetLocalDescription(answer); err != nil {
					t.Fatalf("sub SetLocal: %v", err)
				}
				ansPayload, _ := json.Marshal(AnswerRequest{
					RoomID: roomID, ParticipantID: subscriber.ID(),
					TargetParticipantID: subscriber.ID(), SDP: subBrowser.LocalDescription().SDP,
				})
				ansConn := newHeadlessConn(subscriber.ID(), roomID)
				if err := h.handleMessage(ansConn, room, subscriber, &Message{Type: MessageTypeAnswer, Data: ansPayload}); err != nil {
					t.Fatalf("handleAnswer: %v", err)
				}
				consumed++
				quiet = time.Now()
			case <-time.After(200 * time.Millisecond):
				if time.Since(quiet) >= time.Second && (!requireMedia || dc.ontrack.Load() >= uint64(wantTracks)) {
					return consumed
				}
			}
		}
		return consumed
	}

	if publishFirst {
		publishAll()
	}
	if subscribeEarly {
		subscribeAll()
		if got := completeSubOffers(len(specs), false); got < 1 {
			t.Fatalf("early sub offers: consumed %d, want >=1", got)
		}
	}

	// Publisher browser PC negotiates with the server PC (mirrors handleOffer).
	pubBrowser, err := pionwebrtc.NewPeerConnection(orderingPionConfig())
	if err != nil {
		t.Fatalf("pubBrowser: %v", err)
	}
	t.Cleanup(func() { _ = pubBrowser.Close() })
	var pubLocals []*pionwebrtc.TrackLocalStaticRTP
	for _, s := range specs {
		lt, err := pionwebrtc.NewTrackLocalStaticRTP(s.cap, s.wireID, s.wireID+"-stream")
		if err != nil {
			t.Fatalf("pub local: %v", err)
		}
		if _, err := pubBrowser.AddTrack(lt); err != nil {
			t.Fatalf("pubBrowser.AddTrack: %v", err)
		}
		pubLocals = append(pubLocals, lt)
	}
	orderingWireICE(t, pubBrowser, pubPC.PionPeerConnection())
	pubOffer, err := pubBrowser.CreateOffer(nil)
	if err != nil {
		t.Fatalf("pub CreateOffer: %v", err)
	}
	if err := pubBrowser.SetLocalDescription(pubOffer); err != nil {
		t.Fatalf("pub SetLocal: %v", err)
	}
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
		t.Fatalf("no publisher answer from handleOffer")
	}
	if err := pubBrowser.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeAnswer, SDP: pubAnswerSDP}); err != nil {
		t.Fatalf("pub SetRemote(answer): %v", err)
	}
	if !orderingWait(t, "pub ICE connected", 10*time.Second, func() bool {
		st := pubBrowser.ICEConnectionState()
		return st == pionwebrtc.ICEConnectionStateConnected || st == pionwebrtc.ICEConnectionStateCompleted
	}) {
		t.Fatalf("publisher ICE never connected")
	}

	// Pump RTP from the publisher browser. The pump starts before any
	// OnTrack wait: pion fires the server-side OnTrack only once RTP starts
	// arriving, so waiting for the wire forwarder first would deadlock.
	var sent atomic.Uint64
	stopPump := make(chan struct{})
	defer close(stopPump)
	for idx, lt := range pubLocals {
		go func(i int, track *pionwebrtc.TrackLocalStaticRTP) {
			seq := uint16(1 + i*1000)
			ts := uint32(90000 + i*100000)
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-stopPump:
					return
				case <-tick.C:
					pkt := &rtp.Packet{
						Header: rtp.Header{Version: 2, PayloadType: specs[i].pt,
							SequenceNumber: seq, Timestamp: ts, SSRC: uint32(0xE000 + i)},
						Payload: make([]byte, 120),
					}
					if err := track.WriteRTP(pkt); err == nil {
						sent.Add(1)
					}
					seq++
					ts += 160
				}
			}
		}(idx, lt)
	}

	if !publishFirst {
		// Ordering B: OnTrack must have fired (wire forwarder exists) before publish.
		for _, s := range specs {
			s := s
			if !orderingWait(t, "wire forwarder "+s.wireID, 15*time.Second, func() bool {
				return h.getForwarder(s.wireID) != nil
			}) {
				t.Fatalf("wire forwarder %s never appeared", s.wireID)
			}
		}
		publishAll()
	}

	if !subscribeEarly {
		subscribeAll()
		completeSubOffers(len(specs), true)
	}
	orderingWait(t, "sub ICE connected", 10*time.Second, func() bool {
		st := subBrowser.ICEConnectionState()
		return st == pionwebrtc.ICEConnectionStateConnected || st == pionwebrtc.ICEConnectionStateCompleted
	})

	// Assert media delivery per signaled track with deadlines (robust to DTLS timing).
	// Inbound RTP is awaited first: it implies OnTrack fired and any
	// publish-path adoption already ran, so IsRemote must hold afterwards.
	for _, s := range specs {
		s := s
		fw := h.getForwarder(s.signaledID)
		if fw == nil {
			t.Fatalf("no forwarder for signaled track %s", s.signaledID)
		}
		if !orderingWait(t, "recv "+s.signaledID, 15*time.Second, func() bool { return fw.PacketsReceived() >= 10 }) {
			t.Fatalf("forwarder %s recv=%d, want >=10", s.signaledID, fw.PacketsReceived())
		}
		if !fw.PublisherTrack().IsRemote() {
			t.Fatalf("forwarder %s publisher is not the real TrackRemote", s.signaledID)
		}
		if got := fw.SubscriberCount(); got != 1 {
			t.Fatalf("forwarder %s subscribers = %d, want 1", s.signaledID, got)
		}
		if !orderingWait(t, "fwd "+s.signaledID, 15*time.Second, func() bool { return fw.PacketsForwarded() >= 10 }) {
			t.Fatalf("forwarder %s fwd=%d, want >=10", s.signaledID, fw.PacketsForwarded())
		}
		if !orderingWait(t, "written "+s.signaledID, 15*time.Second, func() bool { return fw.PacketsWritten() >= 10 }) {
			t.Fatalf("forwarder %s written=%d, want >=10", s.signaledID, fw.PacketsWritten())
		}
		if !matchIDs && h.getForwarder(s.wireID) != nil {
			t.Fatalf("orphan wire-ID forwarder %s still registered", s.wireID)
		}
	}
	if !orderingWait(t, "subscriber RTP", 20*time.Second, func() bool {
		return dc.received.Load() >= uint64(10*len(specs))
	}) {
		t.Fatalf("subscriber received=%d, want >=%d", dc.received.Load(), 10*len(specs))
	}
	if !orderingWait(t, "subscriber ontrack", 10*time.Second, func() bool {
		return dc.ontrack.Load() >= uint64(len(specs))
	}) {
		t.Fatalf("subscriber ontrack=%d, want >=%d", dc.ontrack.Load(), len(specs))
	}

	// Exactly one forwarder per published track must remain registered.
	h.trackForwardersMutex.RLock()
	total := len(h.trackForwarders)
	h.trackForwardersMutex.RUnlock()
	if total != len(specs) {
		t.Fatalf("registered forwarders = %d, want exactly %d (no duplicates/orphans)", total, len(specs))
	}

	for _, s := range specs {
		fw := h.getForwarder(s.signaledID)
		recv, fwd, written, dropped, seq, _, ssrc, pt := fw.DiagSnapshot()
		t.Logf("[CONFORM %s] track=%s kind=%s subs=%d sent=%d recv=%d fwd=%d written=%d dropped=%d subRecv=%d ontrack=%d seq=%d pt=%d ssrc=%d",
			name, s.signaledID, s.kind, fw.SubscriberCount(), sent.Load(), recv, fwd, written, dropped,
			dc.received.Load(), dc.ontrack.Load(), seq, pt, ssrc)
	}
}

// Test 1: publish_track first + matching IDs (audio).
func TestOrdering_PublishFirstMatchIDs(t *testing.T) {
	runOrderingCase(t, "t1", "order-room-t1", []string{"audio"}, true, true, false)
}

// Test 2: OnTrack first + matching IDs (video).
func TestOrdering_OnTrackFirstMatchIDs(t *testing.T) {
	runOrderingCase(t, "t2", "order-room-t2", []string{"video"}, false, true, false)
}

// Test 3: publish_track first + mismatched IDs (audio + video).
func TestOrdering_PublishFirstMismatchIDs(t *testing.T) {
	runOrderingCase(t, "t3", "order-room-t3", []string{"audio", "video"}, true, false, false)
}

// Test 4: OnTrack first + mismatched IDs (audio + video) — the reported bug.
func TestOrdering_OnTrackFirstMismatchIDs(t *testing.T) {
	runOrderingCase(t, "t4", "order-room-t4", []string{"audio", "video"}, false, false, false)
}

// Early subscription: subscribe before any publisher RTP (audio).
func TestOrdering_EarlySubscribe(t *testing.T) {
	runOrderingCase(t, "t5", "order-room-t5", []string{"audio"}, true, true, true)
}
