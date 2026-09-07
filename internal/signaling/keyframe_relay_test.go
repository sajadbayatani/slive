package signaling

import (
	"testing"
	"time"

	pionrtcp "github.com/pion/rtcp"
	"github.com/pion/rtp"
	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

// keyframeRelayFixture wires one publisher (raw pion "browser" streaming VP8
// to a Slive publisher PC via real in-process ICE/DTLS) and one Slive
// subscriber PC attached to the auto-created forwarder. The subscriber
// browser is synthesized: tests craft PLI/FIR with the subscriber-egress
// SSRC and call relayKeyframeRequest directly, then observe what the real
// publisher browser receives on its RTPSender.
type keyframeRelayFixture struct {
	h           *Handler
	pubPC       *webrtc.PeerConnection
	subPC       *webrtc.PeerConnection
	pubBrowser  *pionwebrtc.PeerConnection
	pubSender   *pionwebrtc.RTPSender
	fw          *webrtc.TrackForwarder
	trackID     string
	publisherID string
	ingressSSRC uint32
	egressSSRC  uint32
	// rtcpCh carries every RTCP batch arriving at the publisher browser.
	// A single pump goroutine feeds it: concurrent ReadRTCP calls on one
	// RTPSender would split packets across readers and flake the tests.
	rtcpCh chan pollResult
}

type pollResult struct {
	pkts []pionrtcp.Packet
	err  error
}

func newKeyframeRelayFixture(t *testing.T, roomID, pubID, subID, trackID string) *keyframeRelayFixture {
	t.Helper()
	h := newTestHandler()
	room, pub := joinParticipant(t, h, roomID, pubID)
	_, sub := joinParticipant(t, h, roomID, subID)

	pubPC, err := h.ensurePeerConnection(pub, nil)
	if err != nil {
		t.Fatalf("ensurePeerConnection publisher: %v", err)
	}
	subPC, err := h.ensurePeerConnection(sub, nil)
	if err != nil {
		t.Fatalf("ensurePeerConnection subscriber: %v", err)
	}
	t.Cleanup(func() { _ = pubPC.Close(); _ = subPC.Close() })

	// Mirror publish: domain track owned by the publisher and registered in
	// the room so OnTrack maps the incoming TrackRemote onto trackID.
	dt, err := domain.NewTrack(trackID, domain.TrackKindVideo, domain.TrackSourceCamera)
	if err != nil {
		t.Fatalf("domain.NewTrack: %v", err)
	}
	dt.SetPublisher(pub)
	dt.Publish()
	if err := room.PublishTrack(dt); err != nil {
		t.Fatalf("room.PublishTrack: %v", err)
	}
	if err := pub.PublishTrack(dt); err != nil {
		t.Fatalf("participant.PublishTrack: %v", err)
	}

	// Raw pion publisher browser (STUN-free, host candidates only).
	browser, err := pionwebrtc.NewPeerConnection(pionwebrtc.Configuration{})
	if err != nil {
		t.Fatalf("browser NewPeerConnection: %v", err)
	}
	t.Cleanup(func() { _ = browser.Close() })
	wireTrack, err := pionwebrtc.NewTrackLocalStaticRTP(
		pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeVP8, ClockRate: 90000},
		"wire-"+trackID, "stream-"+trackID,
	)
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}
	pubSender, err := browser.AddTrack(wireTrack)
	if err != nil {
		t.Fatalf("browser.AddTrack: %v", err)
	}
	// Single RTCP pump for the whole fixture (see rtcpCh).
	rtcpCh := startRTCPPump(t, pubSender)

	offer, err := browser.CreateOffer(nil)
	if err != nil {
		t.Fatalf("browser.CreateOffer: %v", err)
	}
	if err := browser.SetLocalDescription(offer); err != nil {
		t.Fatalf("browser.SetLocalDescription: %v", err)
	}
	<-pionwebrtc.GatheringCompletePromise(browser)
	answer, err := pubPC.CreateAnswer(webrtc.NewSessionDescription(browser.LocalDescription()))
	if err != nil {
		t.Fatalf("pubPC.CreateAnswer: %v", err)
	}
	if err := browser.SetRemoteDescription(*answer.PionSessionDescription()); err != nil {
		t.Fatalf("browser.SetRemoteDescription: %v", err)
	}

	// Pion fires OnTrack only on first incoming RTP (not at SDP time), so
	// the browser must actually stream. Send minimal VP8 packets until the
	// test ends; Slive's handleTrack maps them onto trackID via the
	// published domain track (kind match).
	stopRTP := make(chan struct{})
	t.Cleanup(func() { close(stopRTP) })
	go func() {
		seq := uint16(1000)
		ts := uint32(90000)
		for {
			select {
			case <-stopRTP:
				return
			default:
			}
			pkt := &rtp.Packet{
				Header:  rtp.Header{Version: 2, PayloadType: 96, SSRC: 0x12345678, SequenceNumber: seq, Timestamp: ts, Marker: true},
				Payload: []byte{0x10, 0x20, 0x48, 0x00},
			}
			_ = wireTrack.WriteRTP(pkt)
			seq++
			ts += 3000
			time.Sleep(20 * time.Millisecond)
		}
	}()

	waitFor := func(desc string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("timeout waiting for %s", desc)
	}
	waitFor("browser ICE connected", func() bool {
		return browser.ICEConnectionState() == pionwebrtc.ICEConnectionStateConnected ||
			browser.ICEConnectionState() == pionwebrtc.ICEConnectionStateCompleted
	})
	waitFor("publisher forwarder with remote ingress", func() bool {
		fw := h.getForwarder(trackID)
		if fw == nil {
			return false
		}
		pt := fw.PublisherTrack()
		return pt != nil && pt.IsRemote()
	})

	fw := h.getForwarder(trackID)
	ingressSSRC, ok := fw.PublisherTrack().RemoteSSRC()
	if !ok || ingressSSRC == 0 {
		t.Fatalf("publisher ingress SSRC = (%d,%v), want nonzero", ingressSSRC, ok)
	}

	if err := fw.AddSubscriber(subPC); err != nil {
		t.Fatalf("AddSubscriber: %v", err)
	}
	var egressSSRC uint32
	for ssrc, tid := range subPC.EgressSSRCMap() {
		if tid == trackID {
			egressSSRC = ssrc
		}
	}
	if egressSSRC == 0 {
		t.Fatalf("no egress SSRC mapped for %q: %v", trackID, subPC.EgressSSRCMap())
	}

	return &keyframeRelayFixture{
		h: h, pubPC: pubPC, subPC: subPC,
		pubBrowser: browser, pubSender: pubSender, fw: fw,
		trackID: trackID, publisherID: pubID,
		ingressSSRC: ingressSSRC, egressSSRC: egressSSRC,
		rtcpCh: rtcpCh,
	}
}

// pollPublisherRTCP collects RTCP packets arriving at the publisher browser
// until match returns true or the timeout elapses. Non-matching packets
// (e.g. TWCC/SR from Slive) are skipped. It returns (nil, false) on timeout
// so negative tests can assert silence without failing.
func (f *keyframeRelayFixture) pollPublisherRTCP(timeout time.Duration, match func(pionrtcp.Packet) bool) (pionrtcp.Packet, bool) {
	return awaitRTCPPackets(f.rtcpCh, timeout, match)
}

// readPublisherRTCP is pollPublisherRTCP that fails the test on timeout.
func (f *keyframeRelayFixture) readPublisherRTCP(t *testing.T, timeout time.Duration, match func(pionrtcp.Packet) bool) pionrtcp.Packet {
	t.Helper()
	if p, ok := f.pollPublisherRTCP(timeout, match); ok {
		return p
	}
	t.Fatal("timeout waiting for RTCP at publisher browser")
	return nil
}

// startRTCPPump feeds every RTCP batch arriving at sender into a channel.
// A single pump per sender is required: concurrent ReadRTCP calls on one
// RTPSender split packets across readers and flake tests.
func startRTCPPump(t *testing.T, sender *pionwebrtc.RTPSender) chan pollResult {
	t.Helper()
	ch := make(chan pollResult, 64)
	go func() {
		for {
			pkts, _, err := sender.ReadRTCP()
			ch <- pollResult{pkts, err}
			if err != nil {
				return
			}
		}
	}()
	return ch
}

// awaitRTCPPackets returns the first packet satisfying match, or (nil, false)
// on timeout. Non-matching packets are skipped.
func awaitRTCPPackets(ch chan pollResult, timeout time.Duration, match func(pionrtcp.Packet) bool) (pionrtcp.Packet, bool) {
	deadline := time.After(timeout)
	for {
		select {
		case r := <-ch:
			if r.err != nil {
				return nil, false
			}
			for _, p := range r.pkts {
				if match(p) {
					return p, true
				}
			}
		case <-deadline:
			return nil, false
		}
	}
}

// TestKeyframeRelayPLI: subscriber PLI -> correct track -> correct publisher
// PC -> publisher WriteRTCP receives PLI with publisher-ingress media SSRC.
func TestKeyframeRelayPLI(t *testing.T) {
	f := newKeyframeRelayFixture(t, "kf-room-1", "kf-pub-1", "kf-sub-1", "kf-track-1")

	forwarded, publisherID := f.h.relayKeyframeRequest(f.subPC, f.trackID, "PLI", 0xAABBCCDD, f.egressSSRC)
	if !forwarded {
		t.Fatal("relayKeyframeRequest PLI returned forwarded=false")
	}
	if publisherID != f.publisherID {
		t.Errorf("publisher = %q, want %q", publisherID, f.publisherID)
	}

	got := f.readPublisherRTCP(t, 5*time.Second, func(p pionrtcp.Packet) bool {
		_, ok := p.(*pionrtcp.PictureLossIndication)
		return ok
	})
	pli, ok := got.(*pionrtcp.PictureLossIndication)
	if !ok {
		t.Fatalf("packet = %T, want PLI", got)
	}
	if pli.MediaSSRC != f.ingressSSRC {
		t.Errorf("PLI MediaSSRC = %d, want publisher-ingress %d (subscriber egress was %d)",
			pli.MediaSSRC, f.ingressSSRC, f.egressSSRC)
	}
	if f.egressSSRC == f.ingressSSRC {
		t.Logf("note: egress and ingress SSRCs coincide on this run; mapping still resolved via EgressSSRCMap")
	}
	if pli.SenderSSRC != 0xAABBCCDD {
		t.Errorf("PLI SenderSSRC = %d, want preserved subscriber sender 0xAABBCCDD", pli.SenderSSRC)
	}
}

// TestKeyframeRelayUnknownSSRC: PLI for an unmapped SSRC is safely ignored:
// no panic, no forward, nothing sent to the publisher.
func TestKeyframeRelayUnknownSSRC(t *testing.T) {
	f := newKeyframeRelayFixture(t, "kf-room-2", "kf-pub-2", "kf-sub-2", "kf-track-2")

	forwarded, _ := f.h.relayKeyframeRequest(f.subPC, f.trackID, "PLI", 0x11111111, 0xDEADBEEF)
	if forwarded {
		t.Error("unknown-SSRC PLI returned forwarded=true, want false")
	}
	// Unknown track is equally ignored.
	forwarded, _ = f.h.relayKeyframeRequest(f.subPC, "no-such-track", "PLI", 0x11111111, f.egressSSRC)
	if forwarded {
		t.Error("unknown-track PLI returned forwarded=true, want false")
	}
	// Nil subscriber PC must not panic.
	if fwd, _ := f.h.relayKeyframeRequest(nil, f.trackID, "PLI", 0x11111111, f.egressSSRC); fwd {
		t.Error("nil-PC PLI returned forwarded=true, want false")
	}

	// Nothing PLI/FIR-shaped reaches the publisher: a short quiet window
	// must yield no PLI/FIR (other traffic such as TWCC is ignored).
	isKeyframe := func(p pionrtcp.Packet) bool {
		switch p.(type) {
		case *pionrtcp.PictureLossIndication, *pionrtcp.FullIntraRequest:
			return true
		}
		return false
	}
	if got, found := f.pollPublisherRTCP(700*time.Millisecond, isKeyframe); found {
		t.Errorf("publisher received %T for unmapped feedback, want silence", got)
	}
}

// TestKeyframeRelayMultipleSubscribers: PLIs from two subscribers of the same
// track both route to the same publisher; a subscriber of another track
// routes to its own publisher only (no cross-participant routing).
func TestKeyframeRelayMultipleSubscribers(t *testing.T) {
	f := newKeyframeRelayFixture(t, "kf-room-3", "kf-pub-3a", "kf-sub-3a", "kf-track-3a")
	h := f.h

	// Second subscriber on the same track, same handler/room.
	_, subB := joinParticipant(t, h, "kf-room-3", "kf-sub-3b")
	subBPC, err := h.ensurePeerConnection(subB, nil)
	if err != nil {
		t.Fatalf("ensurePeerConnection subB: %v", err)
	}
	t.Cleanup(func() { _ = subBPC.Close() })
	if err := f.fw.AddSubscriber(subBPC); err != nil {
		t.Fatalf("AddSubscriber B: %v", err)
	}
	var egressB uint32
	for ssrc, tid := range subBPC.EgressSSRCMap() {
		if tid == f.trackID {
			egressB = ssrc
		}
	}
	if egressB == 0 {
		t.Fatalf("no egress SSRC for subscriber B: %v", subBPC.EgressSSRCMap())
	}

	if fwd, _ := h.relayKeyframeRequest(f.subPC, f.trackID, "PLI", 0xA1, f.egressSSRC); !fwd {
		t.Error("subscriber A PLI not forwarded")
	}
	if fwd, _ := h.relayKeyframeRequest(subBPC, f.trackID, "PLI", 0xB2, egressB); !fwd {
		t.Error("subscriber B PLI not forwarded")
	}
	for i, wantSender := range []uint32{0xA1, 0xB2} {
		got := f.readPublisherRTCP(t, 5*time.Second, func(p pionrtcp.Packet) bool {
			_, ok := p.(*pionrtcp.PictureLossIndication)
			return ok
		})
		pli := got.(*pionrtcp.PictureLossIndication)
		if pli.MediaSSRC != f.ingressSSRC || pli.SenderSSRC != wantSender {
			t.Errorf("PLI %d = sender %d media %d, want sender %d media %d",
				i, pli.SenderSSRC, pli.MediaSSRC, wantSender, f.ingressSSRC)
		}
	}

	// Cross-track isolation: subscriber A's egress SSRC presented against a
	// DIFFERENT track must not route anywhere.
	if fwd, _ := h.relayKeyframeRequest(f.subPC, "kf-track-3a-other", "PLI", 0xA1, f.egressSSRC); fwd {
		t.Error("cross-track PLI returned forwarded=true, want false")
	}
}

// TestKeyframeRelayFIR: FIR is routed upstream with the publisher-ingress
// media SSRC and a per-track FIR sequence number that increments.
func TestKeyframeRelayFIR(t *testing.T) {
	f := newKeyframeRelayFixture(t, "kf-room-4", "kf-pub-4", "kf-sub-4", "kf-track-4")

	for wantSeq := uint8(1); wantSeq <= 2; wantSeq++ {
		forwarded, publisherID := f.h.relayKeyframeRequest(f.subPC, f.trackID, "FIR", 0xC3, f.egressSSRC)
		if !forwarded {
			t.Fatalf("relayKeyframeRequest FIR #%d returned forwarded=false", wantSeq)
		}
		if publisherID != f.publisherID {
			t.Errorf("publisher = %q, want %q", publisherID, f.publisherID)
		}
		got := f.readPublisherRTCP(t, 5*time.Second, func(p pionrtcp.Packet) bool {
			_, ok := p.(*pionrtcp.FullIntraRequest)
			return ok
		})
		fir, ok := got.(*pionrtcp.FullIntraRequest)
		if !ok {
			t.Fatalf("packet = %T, want FIR", got)
		}
		if fir.MediaSSRC != f.ingressSSRC {
			t.Errorf("FIR MediaSSRC = %d, want publisher-ingress %d", fir.MediaSSRC, f.ingressSSRC)
		}
		if len(fir.FIR) != 1 {
			t.Fatalf("FIR entries = %d, want 1", len(fir.FIR))
		}
		if fir.FIR[0].SSRC != f.ingressSSRC {
			t.Errorf("FIR entry SSRC = %d, want %d", fir.FIR[0].SSRC, f.ingressSSRC)
		}
		if fir.FIR[0].SequenceNumber != wantSeq {
			t.Errorf("FIR seq = %d, want %d", fir.FIR[0].SequenceNumber, wantSeq)
		}
	}
}

// TestKeyframeRelayH264AfterReconcile is the end-to-end regression test for
// the egress-codec reconciliation fix: publish (placeholder, codec unknown),
// subscribe BEFORE the TrackRemote arrives (provisional VP8 egress), then
// connect an H264-only publisher. The swap must rebuild the subscriber
// egress to H264, and PLI relayed with the fresh egress SSRC must reach the
// publisher addressed to the H264 ingress SSRC.
func TestKeyframeRelayH264AfterReconcile(t *testing.T) {
	const (
		roomID  = "kf-room-h264"
		pubID   = "kf-pub-h264"
		subID   = "kf-sub-h264"
		trackID = "kf-track-h264"
	)
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	room, pub := joinParticipant(t, h, roomID, pubID)
	_, sub := joinParticipant(t, h, roomID, subID)
	pubPC, err := h.ensurePeerConnection(pub, channelSender(make(chan string, 8)))
	if err != nil {
		t.Fatalf("ensurePeerConnection publisher: %v", err)
	}
	subPC, err := h.ensurePeerConnection(sub, channelSender(make(chan string, 8)))
	if err != nil {
		t.Fatalf("ensurePeerConnection subscriber: %v", err)
	}

	// Publish first: eager placeholder forwarder, codec unknown.
	mustPublishTrackViaHandler(t, h, room, pub, trackID, "video", "camera")
	fw := waitForForwarder(t, h, trackID, true)
	if mime := fw.PublisherTrack().Codec().MimeType; mime != "" {
		t.Fatalf("placeholder publisher codec = %q, want unknown (empty)", mime)
	}

	// Subscribe before any media: provisional VP8 egress.
	mustSubscribeTrackViaHandler(t, h, room, sub, trackID)
	pre := subPC.GetLocalTrack(trackID)
	if pre == nil {
		t.Fatal("missing subscriber track pre-swap")
	}
	if mime := pre.Codec().MimeType; mime != pionwebrtc.MimeTypeVP8 {
		t.Fatalf("provisional egress codec = %q, want VP8", mime)
	}
	var oldEgress uint32
	for ssrc, tid := range subPC.EgressSSRCMap() {
		if tid == trackID {
			oldEgress = ssrc
		}
	}
	if oldEgress == 0 {
		t.Fatal("no provisional egress SSRC")
	}

	// Connect an H264-only publisher browser and stream.
	browser, err := pionwebrtc.NewPeerConnection(pionwebrtc.Configuration{})
	if err != nil {
		t.Fatalf("browser NewPeerConnection: %v", err)
	}
	t.Cleanup(func() { _ = browser.Close() })
	h264Cap := pionwebrtc.RTPCodecCapability{
		MimeType:    pionwebrtc.MimeTypeH264,
		ClockRate:   90000,
		SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1",
	}
	wireTrack, err := pionwebrtc.NewTrackLocalStaticRTP(h264Cap, "wire-"+trackID, "stream-"+trackID)
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}
	pubSender, err := browser.AddTrack(wireTrack)
	if err != nil {
		t.Fatalf("browser.AddTrack: %v", err)
	}
	for _, tr := range browser.GetTransceivers() {
		if err := tr.SetCodecPreferences([]pionwebrtc.RTPCodecParameters{{
			RTPCodecCapability: h264Cap,
			PayloadType:        102,
		}}); err != nil {
			t.Fatalf("SetCodecPreferences: %v", err)
		}
	}
	offer, err := browser.CreateOffer(nil)
	if err != nil {
		t.Fatalf("browser.CreateOffer: %v", err)
	}
	if err := browser.SetLocalDescription(offer); err != nil {
		t.Fatalf("browser.SetLocalDescription: %v", err)
	}
	<-pionwebrtc.GatheringCompletePromise(browser)
	answer, err := pubPC.CreateAnswer(webrtc.NewSessionDescription(browser.LocalDescription()))
	if err != nil {
		t.Fatalf("pubPC.CreateAnswer: %v", err)
	}
	if err := browser.SetRemoteDescription(*answer.PionSessionDescription()); err != nil {
		t.Fatalf("browser.SetRemoteDescription: %v", err)
	}
	rtcpCh := startRTCPPump(t, pubSender)
	stopRTP := make(chan struct{})
	t.Cleanup(func() { close(stopRTP) })
	go func() {
		seq := uint16(5000)
		ts := uint32(450000)
		for {
			select {
			case <-stopRTP:
				return
			default:
			}
			pkt := &rtp.Packet{
				Header:  rtp.Header{Version: 2, PayloadType: 102, SSRC: 0x33333333, SequenceNumber: seq, Timestamp: ts, Marker: true},
				Payload: []byte{0x65, 0x88, 0x84, 0x21},
			}
			_ = wireTrack.WriteRTP(pkt)
			seq++
			ts += 3000
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// The swap must reconcile the provisional subscriber to H264.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if got := subPC.GetLocalTrack(trackID); got != nil && got.Codec().MimeType == pionwebrtc.MimeTypeH264 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscriber egress never reconciled to H264")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if mime := fw.PublisherTrack().Codec().MimeType; mime != pionwebrtc.MimeTypeH264 {
		t.Fatalf("publisher codec = %q, want H264", mime)
	}
	ingressSSRC, ok := fw.PublisherTrack().RemoteSSRC()
	if !ok || ingressSSRC == 0 {
		t.Fatalf("publisher ingress SSRC = (%d,%v), want nonzero", ingressSSRC, ok)
	}

	// The swap itself requests an upstream keyframe (sender == ingress SSRC).
	if _, found := awaitRTCPPackets(rtcpCh, 5*time.Second, func(p pionrtcp.Packet) bool {
		pli, ok := p.(*pionrtcp.PictureLossIndication)
		return ok && pli.MediaSSRC == ingressSSRC && pli.SenderSSRC == ingressSSRC
	}); !found {
		t.Error("no swap-triggered upstream PLI arrived at publisher")
	}

	// PLI relayed with the FRESH egress SSRC reaches the publisher.
	var newEgress uint32
	for ssrc, tid := range subPC.EgressSSRCMap() {
		if tid == trackID {
			newEgress = ssrc
		}
	}
	if newEgress == 0 {
		t.Fatal("no reconciled egress SSRC")
	}
	if newEgress == oldEgress {
		t.Log("note: egress SSRC unchanged across rebuild (pion reused SSRC); mapping still re-resolved")
	}
	forwarded, pid := h.relayKeyframeRequest(subPC, trackID, "PLI", 0xAABBCCDD, newEgress)
	if !forwarded || pid != pubID {
		t.Fatalf("relay after reconcile = (%v,%q), want (true,%q)", forwarded, pid, pubID)
	}
	got, found := awaitRTCPPackets(rtcpCh, 5*time.Second, func(p pionrtcp.Packet) bool {
		pli, ok := p.(*pionrtcp.PictureLossIndication)
		return ok && pli.SenderSSRC == 0xAABBCCDD
	})
	if !found {
		t.Fatal("relayed PLI never arrived at publisher after reconcile")
	}
	if pli := got.(*pionrtcp.PictureLossIndication); pli.MediaSSRC != ingressSSRC {
		t.Errorf("relayed PLI MediaSSRC = %d, want ingress %d", pli.MediaSSRC, ingressSSRC)
	}

	// The STALE (pre-rebuild) egress SSRC no longer resolves: ignored.
	if oldEgress != newEgress {
		if fwd, _ := h.relayKeyframeRequest(subPC, trackID, "PLI", 0xAABBCCDD, oldEgress); fwd {
			t.Error("stale-SSRC PLI returned forwarded=true, want false")
		}
	}
}
