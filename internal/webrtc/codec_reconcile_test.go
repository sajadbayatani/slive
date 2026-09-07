package webrtc

// Regression tests for the subscriber-egress codec reconciliation fix.
//
// Root cause under test: a subscriber attaching before the real TrackRemote
// arrives snapshots the placeholder (unknown) codec and builds a provisional
// egress TrackLocal (VP8 by kind inference). When the real TrackRemote
// arrives with a different codec (e.g. H264), UpdatePublisher must rebuild
// the subscriber TrackLocal from the authoritative codec; otherwise H264
// bytes flow through a VP8-negotiated binding and can never decode.

import (
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// reconcileLoopback connects a raw pion publisher browser to a Slive
// publisher PC. The browser streams crafted RTP so OnTrack fires with a real
// TrackRemote carrying the negotiated codec.
type reconcileLoopback struct {
	pubPC     *PeerConnection
	browser   *pionBrowser
	wireTrack *webrtc.TrackLocalStaticRTP
	remoteCh  chan *WebRTCTrack
	stopRTP   chan struct{}
}

type pionBrowser struct {
	pc *webrtc.PeerConnection
}

func newReconcileLoopback(t *testing.T, trackID, mime string) *reconcileLoopback {
	t.Helper()

	pub := domain.NewParticipant("rc-pub", "RC Pub")
	room := domain.NewRoom("rc-room")
	if err := room.Join(pub); err != nil {
		t.Fatalf("room.Join: %v", err)
	}
	pub.SetRoom(room)
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

	pubPC, err := NewPeerConnection(PeerConnectionConfig{
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback,
	}, pub, nil)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	t.Cleanup(func() { _ = pubPC.Close() })
	remoteCh := make(chan *WebRTCTrack, 1)
	pubPC.OnTrack(func(wt *WebRTCTrack) {
		select {
		case remoteCh <- wt:
		default:
		}
	})

	browserPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("browser NewPeerConnection: %v", err)
	}
	t.Cleanup(func() { _ = browserPC.Close() })
	capability := webrtc.RTPCodecCapability{MimeType: mime, ClockRate: 90000}
	pt := webrtc.PayloadType(96)
	if mime == webrtc.MimeTypeH264 {
		capability.SDPFmtpLine = "level-asymmetry-allowed=1;packetization-mode=1"
		pt = webrtc.PayloadType(102)
	}
	wireTrack, err := webrtc.NewTrackLocalStaticRTP(capability, trackID, "stream-"+trackID)
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}
	if _, err := browserPC.AddTrack(wireTrack); err != nil {
		t.Fatalf("browser.AddTrack: %v", err)
	}
	// Pin the offer to a single codec so the TrackRemote codec is deterministic.
	for _, tr := range browserPC.GetTransceivers() {
		prefs := []webrtc.RTPCodecParameters{{
			RTPCodecCapability: capability,
			PayloadType:        pt,
		}}
		if err := tr.SetCodecPreferences(prefs); err != nil {
			t.Fatalf("SetCodecPreferences: %v", err)
		}
	}

	offer, err := browserPC.CreateOffer(nil)
	if err != nil {
		t.Fatalf("browser.CreateOffer: %v", err)
	}
	if err := browserPC.SetLocalDescription(offer); err != nil {
		t.Fatalf("browser.SetLocalDescription: %v", err)
	}
	<-webrtc.GatheringCompletePromise(browserPC)
	answer, err := pubPC.CreateAnswer(NewSessionDescription(browserPC.LocalDescription()))
	if err != nil {
		t.Fatalf("pubPC.CreateAnswer: %v", err)
	}
	if err := browserPC.SetRemoteDescription(*answer.PionSessionDescription()); err != nil {
		t.Fatalf("browser.SetRemoteDescription: %v", err)
	}

	// Stream until the test ends: pion fires OnTrack only on first RTP.
	stopRTP := make(chan struct{})
	t.Cleanup(func() { close(stopRTP) })
	var payload []byte
	if mime == webrtc.MimeTypeH264 {
		payload = []byte{0x65, 0x88, 0x84, 0x21, 0xa0, 0x0b, 0x76, 0x52}
	} else {
		payload = []byte{0x10, 0x20, 0x48, 0x00, 0x9d, 0x01, 0x2a}
	}
	go func() {
		seq := uint16(2000)
		ts := uint32(180000)
		for {
			select {
			case <-stopRTP:
				return
			default:
			}
			pkt := &rtp.Packet{
				Header:  rtp.Header{Version: 2, PayloadType: uint8(pt), SSRC: 0x22222222, SequenceNumber: seq, Timestamp: ts, Marker: true},
				Payload: append([]byte(nil), payload...),
			}
			_ = wireTrack.WriteRTP(pkt)
			seq++
			ts += 3000
			time.Sleep(20 * time.Millisecond)
		}
	}()

	lb := &reconcileLoopback{
		pubPC:     pubPC,
		browser:   &pionBrowser{pc: browserPC},
		wireTrack: wireTrack,
		remoteCh:  remoteCh,
		stopRTP:   stopRTP,
	}
	_ = lb
	return lb
}

func (lb *reconcileLoopback) waitRemote(t *testing.T) *WebRTCTrack {
	t.Helper()
	select {
	case wt := <-lb.remoteCh:
		return wt
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for OnTrack remote wrapper")
		return nil
	}
}

// unknownPlaceholder mimics handlePublishTrack's eager placeholder: codec
// unknown until the real TrackRemote arrives.
func unknownPlaceholder(t *testing.T, trackID string) *WebRTCTrack {
	t.Helper()
	dt, err := domain.NewTrack(trackID, domain.TrackKindVideo, domain.TrackSourceCamera)
	if err != nil {
		t.Fatalf("domain.NewTrack: %v", err)
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{}, trackID, trackID+"-stream")
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}
	return NewWebRTCTrack(dt, pionTrack, webrtc.RTPCodecParameters{})
}

func newReconcileSubscriber(t *testing.T, id string) *PeerConnection {
	t.Helper()
	sub := domain.NewParticipant(id, "Sub "+id)
	pc, err := NewPeerConnection(PeerConnectionConfig{
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback,
	}, sub, nil)
	if err != nil {
		t.Fatalf("NewPeerConnection subscriber: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

// TestPlaceholderCodecUnknown: the eager placeholder carries no codec authority.
func TestPlaceholderCodecUnknown(t *testing.T) {
	ph := unknownPlaceholder(t, "rc-unknown-1")
	if mime := ph.Codec().MimeType; mime != "" {
		t.Errorf("placeholder codec = %q, want unknown (empty)", mime)
	}
	if ph.IsRemote() {
		t.Error("placeholder IsRemote = true, want false")
	}
}

// TestReconcileH264: subscriber attaches pre-swap (provisional VP8), real
// H264 TrackRemote arrives, egress must be rebuilt to H264.
func TestReconcileH264(t *testing.T) {
	const trackID = "rc-h264-1"
	lb := newReconcileLoopback(t, trackID, webrtc.MimeTypeH264)

	fw, err := NewTrackForwarder(unknownPlaceholder(t, trackID))
	if err != nil {
		t.Fatalf("NewTrackForwarder: %v", err)
	}
	subPC := newReconcileSubscriber(t, "rc-sub-h264")
	if err := fw.AddSubscriber(subPC); err != nil {
		t.Fatalf("AddSubscriber pre-swap: %v", err)
	}
	pre := subPC.GetLocalTrack(trackID)
	if pre == nil {
		t.Fatal("missing subscriber track pre-swap")
	}
	if mime := pre.Codec().MimeType; mime != webrtc.MimeTypeVP8 {
		t.Fatalf("provisional egress codec = %q, want VP8 (kind inference)", mime)
	}

	remote := lb.waitRemote(t)
	if mime := remote.Codec().MimeType; mime != webrtc.MimeTypeH264 {
		t.Fatalf("ingress codec = %q, want H264", mime)
	}
	if err := fw.UpdatePublisher(remote); err != nil {
		t.Fatalf("UpdatePublisher: %v", err)
	}

	got := subPC.GetLocalTrack(trackID)
	if got == nil {
		t.Fatal("missing subscriber track post-swap")
	}
	if mime := got.Codec().MimeType; mime != webrtc.MimeTypeH264 {
		t.Errorf("reconciled egress codec = %q, want H264 (got H264-through-VP8 bug)", mime)
	}
	if got == pre {
		t.Error("subscriber track wrapper unchanged after MIME swap; expected rebuild")
	}
	if mime := fw.PublisherTrack().Codec().MimeType; mime != webrtc.MimeTypeH264 {
		t.Errorf("publisher codec = %q, want H264", mime)
	}
}

// TestReconcileVP8NoOp: matching codec needs no rebuild; the same wrapper
// (and no extra renegotiation churn) survives the swap.
func TestReconcileVP8NoOp(t *testing.T) {
	const trackID = "rc-vp8-1"
	lb := newReconcileLoopback(t, trackID, webrtc.MimeTypeVP8)

	fw, err := NewTrackForwarder(unknownPlaceholder(t, trackID))
	if err != nil {
		t.Fatalf("NewTrackForwarder: %v", err)
	}
	subPC := newReconcileSubscriber(t, "rc-sub-vp8")
	if err := fw.AddSubscriber(subPC); err != nil {
		t.Fatalf("AddSubscriber pre-swap: %v", err)
	}
	pre := subPC.GetLocalTrack(trackID)
	if pre == nil {
		t.Fatal("missing subscriber track pre-swap")
	}

	remote := lb.waitRemote(t)
	if mime := remote.Codec().MimeType; mime != webrtc.MimeTypeVP8 {
		t.Fatalf("ingress codec = %q, want VP8", mime)
	}
	if err := fw.UpdatePublisher(remote); err != nil {
		t.Fatalf("UpdatePublisher: %v", err)
	}
	got := subPC.GetLocalTrack(trackID)
	if got == nil {
		t.Fatal("missing subscriber track post-swap")
	}
	if got != pre {
		t.Error("subscriber track rebuilt despite matching VP8 codec; expected no-op")
	}
	if mime := got.Codec().MimeType; mime != webrtc.MimeTypeVP8 {
		t.Errorf("egress codec = %q, want VP8", mime)
	}
}

// TestReconcileMultipleSubscribers: every pre-swap subscriber is reconciled.
func TestReconcileMultipleSubscribers(t *testing.T) {
	const trackID = "rc-multi-1"
	lb := newReconcileLoopback(t, trackID, webrtc.MimeTypeH264)

	fw, err := NewTrackForwarder(unknownPlaceholder(t, trackID))
	if err != nil {
		t.Fatalf("NewTrackForwarder: %v", err)
	}
	subs := []*PeerConnection{
		newReconcileSubscriber(t, "rc-multi-a"),
		newReconcileSubscriber(t, "rc-multi-b"),
	}
	for _, subPC := range subs {
		if err := fw.AddSubscriber(subPC); err != nil {
			t.Fatalf("AddSubscriber pre-swap: %v", err)
		}
	}
	remote := lb.waitRemote(t)
	if err := fw.UpdatePublisher(remote); err != nil {
		t.Fatalf("UpdatePublisher: %v", err)
	}
	for i, subPC := range subs {
		got := subPC.GetLocalTrack(trackID)
		if got == nil {
			t.Fatalf("subscriber %d missing track post-swap", i)
		}
		if mime := got.Codec().MimeType; mime != webrtc.MimeTypeH264 {
			t.Errorf("subscriber %d egress codec = %q, want H264", i, mime)
		}
	}
}

// TestReconciledEgressCarriesPublisherBytes: end-to-end wire check that no
// H264 payload ever traverses a VP8-negotiated binding. The subscriber
// browser negotiates the reconciled track and must receive the exact bytes
// the publisher streamed, labeled with the subscriber-negotiated H264 PT.
func TestReconciledEgressCarriesPublisherBytes(t *testing.T) {
	const trackID = "rc-wire-1"
	lb := newReconcileLoopback(t, trackID, webrtc.MimeTypeH264)

	fw, err := NewTrackForwarder(unknownPlaceholder(t, trackID))
	if err != nil {
		t.Fatalf("NewTrackForwarder: %v", err)
	}
	subPC := newReconcileSubscriber(t, "rc-sub-wire")
	if err := fw.AddSubscriber(subPC); err != nil {
		t.Fatalf("AddSubscriber pre-swap: %v", err)
	}
	remote := lb.waitRemote(t)
	if err := fw.UpdatePublisher(remote); err != nil {
		t.Fatalf("UpdatePublisher: %v", err)
	}

	// Negotiate the subscriber leg AFTER reconciliation, like a live call.
	subBrowser, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("subBrowser NewPeerConnection: %v", err)
	}
	t.Cleanup(func() { _ = subBrowser.Close() })
	type trackSeen struct {
		track *webrtc.TrackRemote
	}
	seenCh := make(chan trackSeen, 1)
	subBrowser.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		select {
		case seenCh <- trackSeen{tr}:
		default:
		}
	})
	offer, err := subPC.CreateOffer()
	if err != nil {
		t.Fatalf("subPC.CreateOffer: %v", err)
	}
	if err := subBrowser.SetRemoteDescription(*offer.PionSessionDescription()); err != nil {
		t.Fatalf("subBrowser.SetRemoteDescription: %v", err)
	}
	subAnswer, err := subBrowser.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("subBrowser.CreateAnswer: %v", err)
	}
	if err := subBrowser.SetLocalDescription(subAnswer); err != nil {
		t.Fatalf("subBrowser.SetLocalDescription: %v", err)
	}
	<-webrtc.GatheringCompletePromise(subBrowser)
	if err := subPC.SetRemoteDescription(NewSessionDescription(subBrowser.LocalDescription())); err != nil {
		t.Fatalf("subPC.SetRemoteDescription: %v", err)
	}

	var subRemote *webrtc.TrackRemote
	select {
	case s := <-seenCh:
		subRemote = s.track
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for subscriber-browser OnTrack")
	}
	subCodec := subRemote.Codec()
	if subCodec.MimeType != webrtc.MimeTypeH264 {
		t.Fatalf("subscriber-negotiated codec = %q, want H264 (VP8 binding = bug)", subCodec.MimeType)
	}
	// Read one forwarded packet: PT must be the H264 PT, payload identical.
	buf := make([]byte, 1500)
	_ = buf
	deadline := time.Now().Add(10 * time.Second)
	for {
		n, _, err := subRemote.Read(buf)
		if err != nil {
			t.Fatalf("subRemote.Read: %v", err)
		}
		pkt := &rtp.Packet{}
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			continue
		}
		if pkt.Header.PayloadType != uint8(subCodec.PayloadType) {
			t.Fatalf("egress PT = %d, want subscriber H264 PT %d", pkt.Header.PayloadType, subCodec.PayloadType)
		}
		want := []byte{0x65, 0x88, 0x84, 0x21, 0xa0, 0x0b, 0x76, 0x52}
		if len(pkt.Payload) != len(want) {
			if time.Now().After(deadline) {
				t.Fatalf("payload len = %d, want %d", len(pkt.Payload), len(want))
			}
			continue
		}
		match := true
		for i := range want {
			if pkt.Payload[i] != want[i] {
				match = false
				break
			}
		}
		if !match {
			if time.Now().After(deadline) {
				t.Fatalf("payload bytes = %v, want %v", pkt.Payload, want)
			}
			continue
		}
		break
	}
}
