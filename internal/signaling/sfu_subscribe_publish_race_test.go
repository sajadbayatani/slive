package signaling

// Late-joiner publish/subscribe race (Safari glare) end-to-end.
//
// Geometry under test (Flow C): the late joiner publishes its own audio/video
// AND subscribes to the first publisher's tracks on the SAME server-side
// PeerConnection. Before the negotiation gate, the subscribe-driven server
// offer could strand the PC in have-local-offer when the browser's publisher
// offer arrived -> SetRemoteDescription(offer) rejected with
// InvalidModificationError, the publisher TrackRemote never materialized, and
// the first participant got ontrack with zero RTP.
//
// Required post-fix behavior: the browser publisher offer is answered first
// (stable), exactly the coalesced subscriber negotiation follows as server
// offer(s), both directions carry RTP, and no InvalidModificationError is
// reported anywhere.
//
// These tests drive REAL pion browsers (DTLS/SRTP/RTP over loopback) through
// the actual Handler path, mirroring sfu_ordering_conformance_test.go.

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

type glareTrackSpec struct {
	signaledID string
	kind       string
	source     string
	pt         uint8
	ssrc       uint32
	cap        pionwebrtc.RTPCodecCapability
}

func glareSpecs(prefix string, ssrcBase uint32) []glareTrackSpec {
	return []glareTrackSpec{
		{signaledID: prefix + "-audio", kind: "audio", source: "microphone", pt: 111, ssrc: ssrcBase,
			cap: pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}},
		{signaledID: prefix + "-video", kind: "video", source: "camera", pt: 96, ssrc: ssrcBase + 1,
			cap: pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeVP8, ClockRate: 90000}},
	}
}

// glareBrowser is a loopback browser PC with track locals, ontrack/reads
// accounting, and an RTP pump.
type glareBrowser struct {
	t        *testing.T
	pc       *pionwebrtc.PeerConnection
	locals   []*pionwebrtc.TrackLocalStaticRTP
	specs    []glareTrackSpec
	ontrack  atomic.Uint64
	received atomic.Uint64
	sent     atomic.Uint64
	stop     chan struct{}
}

func newGlareBrowser(t *testing.T, specs []glareTrackSpec) *glareBrowser {
	t.Helper()
	b := &glareBrowser{t: t, specs: specs, stop: make(chan struct{})}
	t.Cleanup(func() { close(b.stop); _ = b.pc.Close() })
	pc, err := pionwebrtc.NewPeerConnection(orderingPionConfig())
	if err != nil {
		t.Fatalf("browser pc: %v", err)
	}
	b.pc = pc
	for _, s := range specs {
		lt, err := pionwebrtc.NewTrackLocalStaticRTP(s.cap, s.signaledID, s.signaledID+"-stream")
		if err != nil {
			t.Fatalf("browser local %s: %v", s.signaledID, err)
		}
		if _, err := pc.AddTrack(lt); err != nil {
			t.Fatalf("browser AddTrack %s: %v", s.signaledID, err)
		}
		b.locals = append(b.locals, lt)
	}
	pc.OnTrack(func(tr *pionwebrtc.TrackRemote, _ *pionwebrtc.RTPReceiver) {
		b.ontrack.Add(1)
		go func() {
			buf := make([]byte, 1600)
			for {
				if _, _, err := tr.Read(buf); err != nil {
					return
				}
				b.received.Add(1)
			}
		}()
	})
	return b
}

func (b *glareBrowser) pump() {
	b.t.Helper()
	for idx, lt := range b.locals {
		go func(i int, track *pionwebrtc.TrackLocalStaticRTP) {
			seq := uint16(1 + i*1000)
			ts := uint32(90000 + i*100000)
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-b.stop:
					return
				case <-tick.C:
					pkt := &rtp.Packet{
						Header: rtp.Header{Version: 2, PayloadType: b.specs[i].pt,
							SequenceNumber: seq, Timestamp: ts, SSRC: b.specs[i].ssrc},
						Payload: make([]byte, 120),
					}
					if err := track.WriteRTP(pkt); err == nil {
						b.sent.Add(1)
					}
					seq++
					ts += 160
				}
			}
		}(idx, lt)
	}
}

// runGlareLateJoiner executes the full Chrome-first/Safari-late scenario with
// the given identities: first publishes + completes, late joins and races
// publish against subscribe on one PC, then both directions must carry RTP.
func runGlareLateJoiner(t *testing.T, roomID, firstID, lateID string) {
	t.Helper()
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })

	room, first := joinParticipant(t, h, roomID, firstID)
	_, late := joinParticipant(t, h, roomID, lateID)

	firstSpecs := glareSpecs("glare-first", 0xA000)
	lateSpecs := glareSpecs("glare-late", 0xB000)
	// Match the production failure: the late publisher advertises H264 while
	// the subscriber is initially attached to a provisional VP8 egress.
	lateSpecs[1].cap.MimeType = pionwebrtc.MimeTypeH264

	firstOffers := make(chan string, 8)
	lateOffers := make(chan string, 8)
	offerTap := func(ch chan string) func(string, interface{}) error {
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
	firstPC, err := h.ensurePeerConnection(first, offerTap(firstOffers))
	if err != nil {
		t.Fatalf("ensure first PC: %v", err)
	}
	latePC, err := h.ensurePeerConnection(late, offerTap(lateOffers))
	if err != nil {
		t.Fatalf("ensure late PC: %v", err)
	}
	var errConns []*Connection
	trackErrConns := func(c *Connection) { errConns = append(errConns, c) }
	publish := func(p *domain.Participant, specs []glareTrackSpec) {
		for _, s := range specs {
			payload, _ := json.Marshal(PublishTrackRequest{
				RoomID: roomID, ParticipantID: p.ID(),
				Track: TrackInfo{ID: s.signaledID, Kind: s.kind, Source: s.source},
			})
			conn := newHeadlessConn(p.ID(), roomID)
			trackErrConns(conn)
			if err := h.handleMessage(conn, room, p, &Message{Type: MessageTypePublishTrack, Data: payload}); err != nil {
				t.Fatalf("publish %s: %v", s.signaledID, err)
			}
		}
	}
	subscribe := func(p *domain.Participant, trackID string) {
		payload, _ := json.Marshal(SubscribeTrackRequest{
			RoomID: roomID, ParticipantID: p.ID(), TrackID: trackID,
		})
		conn := newHeadlessConn(p.ID(), roomID)
		trackErrConns(conn)
		if err := h.handleMessage(conn, room, p, &Message{Type: MessageTypeSubscribeTrack, Data: payload}); err != nil {
			t.Fatalf("subscribe %s: %v", trackID, err)
		}
	}
	// sendOffer drives a browser publish offer through handleOffer and
	// returns the headless conn (holding answer/error responses).
	sendOffer := func(p *domain.Participant, sdp string) *Connection {
		payload, _ := json.Marshal(OfferRequest{
			RoomID: roomID, ParticipantID: p.ID(),
			TargetParticipantID: p.ID(), SDP: sdp,
		})
		conn := newHeadlessConn(p.ID(), roomID)
		trackErrConns(conn)
		if err := h.handleMessage(conn, room, p, &Message{Type: MessageTypeOffer, Data: payload}); err != nil {
			t.Fatalf("handleOffer: %v", err)
		}
		return conn
	}
	answerOf := func(conn *Connection) string {
		for _, m := range drainMessages(conn) {
			if m.Type == MessageTypeAnswer {
				var n AnswerNotification
				if err := m.UnmarshalData(&n); err == nil {
					return n.SDP
				}
			}
		}
		return ""
	}
	// completeOffers answers every captured server offer with the given
	// browser until the stream goes quiet; returns offers consumed.
	completeOffers := func(browser *pionwebrtc.PeerConnection, ch chan string, who string) int {
		consumed := 0
		quiet := time.Now()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case offerSDP := <-ch:
				if err := browser.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeOffer, SDP: offerSDP}); err != nil {
					t.Fatalf("%s SetRemote(server offer): %v", who, err)
				}
				answer, err := browser.CreateAnswer(nil)
				if err != nil {
					t.Fatalf("%s CreateAnswer: %v", who, err)
				}
				if err := browser.SetLocalDescription(answer); err != nil {
					t.Fatalf("%s SetLocal: %v", who, err)
				}
				ansPayload, _ := json.Marshal(AnswerRequest{
					RoomID: roomID, ParticipantID: who,
					TargetParticipantID: who, SDP: browser.LocalDescription().SDP,
				})
				var target *domain.Participant
				if who == first.ID() {
					target = first
				} else {
					target = late
				}
				ansConn := newHeadlessConn(who, roomID)
				trackErrConns(ansConn)
				if err := h.handleMessage(ansConn, room, target, &Message{Type: MessageTypeAnswer, Data: ansPayload}); err != nil {
					t.Fatalf("%s handleAnswer: %v", who, err)
				}
				consumed++
				quiet = time.Now()
			case <-time.After(200 * time.Millisecond):
				if time.Since(quiet) >= time.Second {
					return consumed
				}
			}
		}
		t.Fatalf("%s offers never went quiet", who)
		return consumed
	}

	// ---- FIRST publishes and completes (normal first publisher). ----
	publish(first, firstSpecs)
	firstBrowser := newGlareBrowser(t, firstSpecs)
	orderingWireICE(t, firstBrowser.pc, firstPC.PionPeerConnection())
	pubOffer, err := firstBrowser.pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("first browser offer: %v", err)
	}
	if err := firstBrowser.pc.SetLocalDescription(pubOffer); err != nil {
		t.Fatalf("first browser SetLocal: %v", err)
	}
	offerConn := sendOffer(first, firstBrowser.pc.LocalDescription().SDP)
	pubAnswer := answerOf(offerConn)
	if pubAnswer == "" {
		t.Fatal("first publisher got no answer (name the failure, don't mask it)")
	}
	if err := firstBrowser.pc.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeAnswer, SDP: pubAnswer}); err != nil {
		t.Fatalf("first browser SetRemote(answer): %v", err)
	}
	// Normal first publisher: no server offer should exist (nothing subscribed).
	select {
	case sdp := <-firstOffers:
		t.Fatalf("unexpected server offer to first publisher before any subscribe (sdp %.40s...)", sdp)
	case <-time.After(500 * time.Millisecond):
	}
	if !orderingWait(t, "first ICE connected", 10*time.Second, func() bool {
		st := firstBrowser.pc.ICEConnectionState()
		return st == pionwebrtc.ICEConnectionStateConnected || st == pionwebrtc.ICEConnectionStateCompleted
	}) {
		t.Fatal("first publisher ICE never connected")
	}
	firstBrowser.pump()
	for _, s := range firstSpecs {
		s := s
		if !orderingWait(t, "first forwarder remote "+s.signaledID, 15*time.Second, func() bool {
			fw := h.getForwarder(s.signaledID)
			return fw != nil && fw.PublisherTrack() != nil && fw.PublisherTrack().IsRemote()
		}) {
			t.Fatalf("first forwarder %s never became remote-backed", s.signaledID)
		}
	}

	// ---- LATE joins with the bot1 production ordering: browser offer
	// FIRST, publish_track lands only after the answer (stale-expectation
	// geometry), then subscribes. Under the removed publish expectation this
	// used to deadlock every later subscriber offer; subscribes made after
	// a completed publish must now offer at once.
	lateBrowser := newGlareBrowser(t, lateSpecs)
	orderingWireICE(t, lateBrowser.pc, latePC.PionPeerConnection())
	lateOffer, err := lateBrowser.pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("late browser offer: %v", err)
	}
	if err := lateBrowser.pc.SetLocalDescription(lateOffer); err != nil {
		t.Fatalf("late browser SetLocal: %v", err)
	}
	lateOfferConn := sendOffer(late, lateBrowser.pc.LocalDescription().SDP)
	lateAnswer := answerOf(lateOfferConn)
	if lateAnswer == "" {
		t.Fatal("late publisher got no answer on stable PC: signaling broken")
	}
	for _, m := range drainMessages(lateOfferConn) {
		if m.Type == MessageTypeError {
			var er ErrorResponse
			_ = m.UnmarshalData(&er)
			t.Fatalf("late handleOffer error: %s / %s", er.Code, er.Error)
		}
	}
	if err := lateBrowser.pc.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeAnswer, SDP: lateAnswer}); err != nil {
		t.Fatalf("late browser SetRemote(answer): %v", err)
	}
	// publish_track lands here (its offer already completed); subscribes
	// after this point must drive server offers immediately.
	publish(late, lateSpecs)
	// The already-connected first participant receives track_available and
	// subscribes before the late publisher's first RTP packet. This is the
	// production ordering that used to attach a provisional VP8 egress and
	// later replace it with the real H264 egress without delivering video.
	for _, s := range lateSpecs {
		subscribe(first, s.signaledID)
	}
	for _, s := range lateSpecs {
		if firstPC.GetLocalTrack(s.signaledID) != nil {
			t.Fatalf("early reverse subscribe attached provisional track %s before publisher OnTrack", s.signaledID)
		}
	}
	select {
	case offerSDP := <-firstOffers:
		t.Fatalf("early reverse subscribe emitted server offer before publisher OnTrack: %.40s...", offerSDP)
	case <-time.After(300 * time.Millisecond):
	}
	for _, s := range firstSpecs {
		subscribe(late, s.signaledID)
	}
	// Subscribes after a completed publish drive server offers at once: the
	// stale-expectation deadlock this test guards would leave zero offers.
	if got := completeOffers(lateBrowser.pc, lateOffers, late.ID()); got < 1 {
		t.Fatalf("late consumed %d server offers, want >=1 (post-publish subscribes must offer)", got)
	}
	if !orderingWait(t, "late ICE connected", 10*time.Second, func() bool {
		st := lateBrowser.pc.ICEConnectionState()
		return st == pionwebrtc.ICEConnectionStateConnected || st == pionwebrtc.ICEConnectionStateCompleted
	}) {
		t.Fatal("late publisher ICE never connected")
	}
	lateBrowser.pump()
	for _, s := range lateSpecs {
		s := s
		if !orderingWait(t, "late forwarder remote "+s.signaledID, 15*time.Second, func() bool {
			fw := h.getForwarder(s.signaledID)
			return fw != nil && fw.PublisherTrack() != nil && fw.PublisherTrack().IsRemote()
		}) {
			t.Fatalf("late forwarder %s never became remote-backed (publisher media never arrived)", s.signaledID)
		}
	}

	// ---- LATE publishes and the already-connected FIRST PC is attached by
	// the server: late -> first media. The explicit subscribe above models
	// the client request; the OnTrack callback must not create a second
	// subscription when the real publisher arrives. The offer below must be
	// the first offer for these tracks and must use the authoritative codec. ----
	if got := completeOffers(firstBrowser.pc, firstOffers, first.ID()); got < 1 {
		t.Fatalf("first consumed %d automatic server offers for late tracks, want >=1", got)
	}

	// ---- Media both directions. ----
	if !orderingWait(t, "late browser ontrack", 15*time.Second, func() bool {
		return lateBrowser.ontrack.Load() >= 2
	}) {
		t.Fatalf("late browser ontrack=%d, want >=2 (first->late media missing)", lateBrowser.ontrack.Load())
	}
	if !orderingWait(t, "first browser ontrack", 15*time.Second, func() bool {
		return firstBrowser.ontrack.Load() >= 2
	}) {
		t.Fatalf("first browser ontrack=%d, want >=2 (late->first media missing: ontrack without RTP)", firstBrowser.ontrack.Load())
	}
	if !orderingWait(t, "first->late RTP", 20*time.Second, func() bool {
		return lateBrowser.received.Load() >= 20
	}) {
		t.Fatalf("late browser received=%d, want >=20", lateBrowser.received.Load())
	}
	if !orderingWait(t, "late->first RTP", 20*time.Second, func() bool {
		return firstBrowser.received.Load() >= 20
	}) {
		t.Fatalf("first browser received=%d, want >=20 (the reported bug: ontrack with zero RTP)", firstBrowser.received.Load())
	}

	// ---- No glare reported anywhere. ----
	for _, c := range errConns {
		for _, m := range drainMessages(c) {
			if m.Type != MessageTypeError {
				continue
			}
			var er ErrorResponse
			if err := m.UnmarshalData(&er); err == nil {
				if strings.Contains(er.Error, "InvalidModificationError") {
					t.Errorf("glare leaked to client (req %s): %s", er.RequestType, er.Error)
				}
			}
		}
	}
}

// TestGlare_ChromeFirstSafariLate is the reported production scenario.
func TestGlare_ChromeFirstSafariLate(t *testing.T) {
	runGlareLateJoiner(t, "glare-room-1", "u-chrome", "u-safari")
}

// TestGlare_ReverseOrder proves the same guarantees with roles swapped.
func TestGlare_ReverseOrder(t *testing.T) {
	runGlareLateJoiner(t, "glare-room-2", "u-safari", "u-chrome")
}

// runGlareMutualRace covers the subscribe-first ordering at signaling level:
// the late joiner's subscribes drive a server offer that is still
// outstanding when its publisher offer arrives (true SDP glare). Slive must
// store the browser offer (never InvalidModificationError), complete the
// subscriber exchange via a helper (loopback pion cannot roll back like a
// real browser), then answer the stored publish offer with priority. No ICE
// or RTP is involved: this test proves the negotiation contract (offer
// counts, MID uniqueness, stable convergence, error silence).
// runGlareMutualRace covers the subscribe-first ordering at signaling level:
// the late joiner's first subscribe drives a server offer (still
// outstanding when the second subscribe lands, so it coalesces), then the
// publisher offer crosses it and is stored (never InvalidModificationError).
// A helper completes the subscriber exchanges (loopback pion cannot roll
// back like a real browser); the stored offer is answered with priority and
// the late browser itself answers the second subscriber offer. No ICE/RTP:
// media convergence is covered by runGlareLateJoiner and the lifecycle
// units instead.
//
// Determinism note: subscriptions are staged (audio, await offer, then
// video) so each server offer's content is ordered by construction — the
// sync drive and pion's async echo may race to initiate the first offer,
// but either initiator produces the identical audio-only offer.
func runGlareMutualRace(t *testing.T, roomID, firstID, lateID string) {
	t.Helper()
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })

	room, first := joinParticipant(t, h, roomID, firstID)
	_, late := joinParticipant(t, h, roomID, lateID)

	firstSpecs := glareSpecs("race-first", 0xC000)
	lateSpecs := glareSpecs("race-late", 0xD000)

	firstOffers := make(chan string, 8)
	lateOffers := make(chan string, 8)
	lateAnswers := make(chan string, 8)
	tap := func(offers, answers chan string) func(string, interface{}) error {
		return func(msgType string, data interface{}) error {
			raw, _ := json.Marshal(data)
			var payload struct {
				SDP string `json:"sdp"`
			}
			_ = json.Unmarshal(raw, &payload)
			switch msgType {
			case "webrtc:offer":
				select {
				case offers <- payload.SDP:
				default:
				}
			case "webrtc:answer":
				select {
				case answers <- payload.SDP:
				default:
				}
			}
			return nil
		}
	}
	if _, err := h.ensurePeerConnection(first, tap(firstOffers, nil)); err != nil {
		t.Fatalf("ensure first PC: %v", err)
	}
	if _, err := h.ensurePeerConnection(late, tap(lateOffers, lateAnswers)); err != nil {
		t.Fatalf("ensure late PC: %v", err)
	}
	var errConns []*Connection
	msg := func(p *domain.Participant, typ MessageType, data interface{}) *Connection {
		payload, _ := json.Marshal(data)
		conn := newHeadlessConn(p.ID(), roomID)
		errConns = append(errConns, conn)
		if err := h.handleMessage(conn, room, p, &Message{Type: typ, Data: payload}); err != nil {
			t.Fatalf("%s %s: %v", p.ID(), typ, err)
		}
		return conn
	}
	waitOffer := func(ch chan string, what string) string {
		select {
		case sdp := <-ch:
			return sdp
		case <-time.After(15 * time.Second):
			t.Fatalf("no server offer for %s", what)
			return ""
		}
	}

	// First publishes (domain only; no browser needed for negotiation).
	for _, s := range firstSpecs {
		msg(first, MessageTypePublishTrack, PublishTrackRequest{
			RoomID: roomID, ParticipantID: first.ID(),
			Track: TrackInfo{ID: s.signaledID, Kind: s.kind, Source: s.source},
		})
	}
	// Late publishes then subscribes AUDIO ONLY. Whoever initiates (sync
	// drive or async echo), the first offer can only cover audio: video is
	// not subscribed yet, so its content is deterministic.
	for _, s := range lateSpecs {
		msg(late, MessageTypePublishTrack, PublishTrackRequest{
			RoomID: roomID, ParticipantID: late.ID(),
			Track: TrackInfo{ID: s.signaledID, Kind: s.kind, Source: s.source},
		})
	}
	msg(late, MessageTypeSubscribeTrack, SubscribeTrackRequest{
		RoomID: roomID, ParticipantID: late.ID(), TrackID: firstSpecs[0].signaledID,
	})
	offer1SDP := waitOffer(lateOffers, "late audio subscribe")
	assertUniqueMIDs(t, offer1SDP)

	// Video subscribes while offer#1 is outstanding: must coalesce (no
	// second offer until #1 completes).
	msg(late, MessageTypeSubscribeTrack, SubscribeTrackRequest{
		RoomID: roomID, ParticipantID: late.ID(), TrackID: firstSpecs[1].signaledID,
	})
	select {
	case extra := <-lateOffers:
		t.Fatalf("second server offer while first outstanding (sdp %.40s...); want coalescing", extra)
	case <-time.After(500 * time.Millisecond):
	}

	// Late browser offers behind the outstanding server offer: stored, not
	// failed, not answered yet.
	lateBrowser, err := pionwebrtc.NewPeerConnection(orderingPionConfig())
	if err != nil {
		t.Fatalf("late browser pc: %v", err)
	}
	t.Cleanup(func() { _ = lateBrowser.Close() })
	for _, s := range lateSpecs {
		lt, err := pionwebrtc.NewTrackLocalStaticRTP(s.cap, s.signaledID+"-wire", s.signaledID+"-stream")
		if err != nil {
			t.Fatalf("late local %s: %v", s.signaledID, err)
		}
		if _, err := lateBrowser.AddTrack(lt); err != nil {
			t.Fatalf("late browser AddTrack: %v", err)
		}
	}
	browserOffer, err := lateBrowser.CreateOffer(nil)
	if err != nil {
		t.Fatalf("late browser offer: %v", err)
	}
	if err := lateBrowser.SetLocalDescription(browserOffer); err != nil {
		t.Fatalf("late browser SetLocal: %v", err)
	}
	offerConn := msg(late, MessageTypeOffer, OfferRequest{
		RoomID: roomID, ParticipantID: late.ID(),
		TargetParticipantID: late.ID(), SDP: lateBrowser.LocalDescription().SDP,
	})
	for _, m := range drainMessages(offerConn) {
		switch m.Type {
		case MessageTypeAnswer:
			t.Fatal("late publisher answered inline behind outstanding server offer; want deferral")
		case MessageTypeError:
			var er ErrorResponse
			_ = m.UnmarshalData(&er)
			t.Fatalf("late handleOffer error (glare must never surface): %s / %s", er.Code, er.Error)
		}
	}

	// Helper completes offer#1 (loopback pion cannot roll back its own
	// outstanding offer like a real browser). The flush answers the stored
	// publish offer with priority, then emits offer#2 for the video sender.
	helper, err := pionwebrtc.NewPeerConnection(orderingPionConfig())
	if err != nil {
		t.Fatalf("helper pc: %v", err)
	}
	t.Cleanup(func() { _ = helper.Close() })
	if _, err := helper.AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("helper transceiver: %v", err)
	}
	if err := helper.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeOffer, SDP: offer1SDP}); err != nil {
		t.Fatalf("helper SetRemote(server offer): %v", err)
	}
	answer, err := helper.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("helper CreateAnswer: %v", err)
	}
	if err := helper.SetLocalDescription(answer); err != nil {
		t.Fatalf("helper SetLocal: %v", err)
	}
	msg(late, MessageTypeAnswer, AnswerRequest{
		RoomID: roomID, ParticipantID: late.ID(),
		TargetParticipantID: late.ID(), SDP: helper.LocalDescription().SDP,
	})

	// The stored publish offer is answered with priority over the wire.
	select {
	case deferredSDP := <-lateAnswers:
		if err := lateBrowser.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeAnswer, SDP: deferredSDP}); err != nil {
			t.Fatalf("late browser SetRemote(deferred answer): %v", err)
		}
		if st := lateBrowser.SignalingState(); st != pionwebrtc.SignalingStateStable {
			t.Fatalf("late browser state = %s, want stable", st)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("deferred publish answer never delivered: stored offer lost")
	}

	// Offer#2 (audio re-offer + video) must follow for the video sender, and
	// the now-stable late browser answers it itself: full convergence with
	// both browsers' participation, no helper needed anymore.
	offer2SDP := waitOffer(lateOffers, "late video coalesced offer")
	assertUniqueMIDs(t, offer2SDP)
	if offer2SDP == offer1SDP {
		t.Fatal("second server offer identical to first; want re-offer covering the video sender")
	}
	if err := lateBrowser.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeOffer, SDP: offer2SDP}); err != nil {
		t.Fatalf("late browser SetRemote(server offer #2): %v", err)
	}
	answer2, err := lateBrowser.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("late browser CreateAnswer #2: %v", err)
	}
	if err := lateBrowser.SetLocalDescription(answer2); err != nil {
		t.Fatalf("late browser SetLocal #2: %v", err)
	}
	msg(late, MessageTypeAnswer, AnswerRequest{
		RoomID: roomID, ParticipantID: late.ID(),
		TargetParticipantID: late.ID(), SDP: lateBrowser.LocalDescription().SDP,
	})

	// Convergence: Slive PC stable with an empty gate, exactly two server
	// offers (one per subscribe generation — a third would mean lost
	// coalescing), no glare surfaced.
	deadline := time.Now().Add(5 * time.Second)
	for {
		st := h.getPeerConnection(late.ID()).PionPeerConnection().SignalingState()
		if st == pionwebrtc.SignalingStateStable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("late PC state = %s, want stable", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case extra := <-lateOffers:
		t.Fatalf("third server offer with no new senders (sdp %.40s...); want coalescing", extra)
	case <-time.After(500 * time.Millisecond):
	}
	for _, c := range errConns {
		for _, m := range drainMessages(c) {
			if m.Type != MessageTypeError {
				continue
			}
			var er ErrorResponse
			if err := m.UnmarshalData(&er); err == nil {
				if strings.Contains(er.Error, "InvalidModificationError") {
					t.Errorf("glare leaked to client (req %s): %s", er.RequestType, er.Error)
				}
			}
		}
	}
}

// assertUniqueMIDs fails the test if any a=mid value repeats within one SDP:
// duplicate MIDs across roles corrupt pion's findByMid answer matching and
// are rejected by strict browsers, silently killing later negotiation.
func assertUniqueMIDs(t *testing.T, sdp string) {
	t.Helper()
	seen := map[string]int{}
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "a=mid:") {
			continue
		}
		mid := strings.TrimPrefix(line, "a=mid:")
		seen[mid]++
		if seen[mid] > 1 {
			t.Fatalf("duplicate MID %q in SDP (cross-role collision)", mid)
		}
	}
	if len(seen) == 0 {
		t.Fatal("SDP carries no a=mid lines")
	}
}

// TestGlare_MutualRaceDeferral is the subscribe-first ordering at signaling
// level: subscribes drive an outstanding server offer, the publisher offer
// crosses it and is stored, a helper completes the subscriber exchange, and
// the stored offer is answered with priority. No ICE/RTP: loopback pion
// cannot roll back, so media convergence is covered by runGlareLateJoiner
// and the lifecycle units instead.
func TestGlare_MutualRaceDeferral(t *testing.T) {
	runGlareMutualRace(t, "glare-room-3", "u-chrome", "u-safari")
}
