package webrtc

import (
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

func newTestPublisherTrack(t *testing.T, id string, mime string) *WebRTCTrack {
	t.Helper()
	kind := domain.TrackKindAudio
	source := domain.TrackSourceMicrophone
	if mime == webrtc.MimeTypeVP8 || mime == webrtc.MimeTypeVP9 || mime == webrtc.MimeTypeH264 {
		kind = domain.TrackKindVideo
		source = domain.TrackSourceCamera
	}
	dt, err := domain.NewTrack(id, kind, source)
	if err != nil {
		t.Fatalf("domain.NewTrack: %v", err)
	}
	cap := webrtc.RTPCodecCapability{MimeType: mime}
	if mime == webrtc.MimeTypeOpus {
		cap.ClockRate = 48000
		cap.Channels = 2
		cap.SDPFmtpLine = "minptime=10;useinbandfec=1"
	} else {
		cap.ClockRate = 90000
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(cap, id, id+"-stream")
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}
	codec := webrtc.RTPCodecParameters{
		RTPCodecCapability: cap,
		PayloadType:        111,
	}
	return NewWebRTCTrack(dt, pionTrack, codec)
}

func newTestPeerConnection(t *testing.T, id, name string) *PeerConnection {
	t.Helper()
	pc, err := NewPeerConnection(PeerConnectionConfig{
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback,
	}, domain.NewParticipant(id, name), nil)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	if _, err := pc.PionPeerConnection().AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("AddTransceiverFromKind: %v", err)
	}
	return pc
}

func TestNewTrackForwarderNilPublisher(t *testing.T) {
	if _, err := NewTrackForwarder(nil); err == nil {
		t.Fatal("expected error for nil publisher")
	}
}

func TestTrackForwarderLifecycle(t *testing.T) {
	pub := newTestPublisherTrack(t, "audio-1", webrtc.MimeTypeOpus)
	fw, err := NewTrackForwarder(pub)
	if err != nil {
		t.Fatalf("NewTrackForwarder: %v", err)
	}
	if fw.PublisherTrack() != pub {
		t.Error("PublisherTrack mismatch")
	}
	if fw.IsRunning() {
		t.Error("should not be running initially")
	}
	if fw.SubscriberCount() != 0 {
		t.Errorf("count = %d, want 0", fw.SubscriberCount())
	}

	if err := fw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !fw.IsRunning() {
		t.Error("should be running after Start")
	}
	// Idempotent second Start
	if err := fw.Start(); err != nil {
		t.Fatalf("second Start: %v", err)
	}

	if err := fw.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if fw.IsRunning() {
		t.Error("should not be running after Stop")
	}
	// Idempotent second Stop
	if err := fw.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

func TestTrackForwarderAddRemoveSubscriber(t *testing.T) {
	pub := newTestPublisherTrack(t, "audio-2", webrtc.MimeTypeOpus)
	fw, _ := NewTrackForwarder(pub)

	pc1 := newTestPeerConnection(t, "sub-1", "Bob")
	pc2 := newTestPeerConnection(t, "sub-2", "Carol")

	if err := fw.AddSubscriber(nil); err == nil {
		t.Error("expected error for nil pc")
	}
	if err := fw.AddSubscriber(pc1); err != nil {
		t.Fatalf("AddSubscriber pc1: %v", err)
	}
	if fw.SubscriberCount() != 1 {
		t.Errorf("count = %d, want 1", fw.SubscriberCount())
	}
	// Idempotent
	if err := fw.AddSubscriber(pc1); err != nil {
		t.Fatalf("second AddSubscriber pc1: %v", err)
	}
	if fw.SubscriberCount() != 1 {
		t.Errorf("count after duplicate add = %d, want 1", fw.SubscriberCount())
	}
	if err := fw.AddSubscriber(pc2); err != nil {
		t.Fatalf("AddSubscriber pc2: %v", err)
	}
	if fw.SubscriberCount() != 2 {
		t.Errorf("count = %d, want 2", fw.SubscriberCount())
	}

	// Verify subscriber tracks are added to PCs with Codec preservation
	if got := pc1.GetLocalTrack("audio-2"); got == nil {
		t.Error("pc1 should have forwarded track")
	} else if got.Codec().MimeType != webrtc.MimeTypeOpus {
		t.Errorf("codec mime = %q, want %q", got.Codec().MimeType, webrtc.MimeTypeOpus)
	}
	if got := pc2.GetLocalTrack("audio-2"); got == nil {
		t.Error("pc2 should have forwarded track")
	}

	if err := fw.RemoveSubscriber(pc1); err != nil {
		t.Fatalf("RemoveSubscriber pc1: %v", err)
	}
	if fw.SubscriberCount() != 1 {
		t.Errorf("count after remove = %d, want 1", fw.SubscriberCount())
	}
	if pc1.GetLocalTrack("audio-2") != nil {
		t.Error("pc1 track should be removed")
	}
	// Not found
	if err := fw.RemoveSubscriber(pc1); err == nil {
		t.Error("expected error removing non-subscriber")
	}
	if err := fw.RemoveSubscriber(nil); err == nil {
		t.Error("expected error for nil pc")
	}
}

func TestTrackForwarderAddSubscriberClosedPC(t *testing.T) {
	pub := newTestPublisherTrack(t, "audio-3", webrtc.MimeTypeOpus)
	fw, _ := NewTrackForwarder(pub)
	pc := newTestPeerConnection(t, "closed-sub", "Dave")
	if err := pc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := fw.AddSubscriber(pc); err == nil {
		t.Error("expected error adding closed PC")
	}
}

func TestTrackForwarderCodecPreservation(t *testing.T) {
	pub := newTestPublisherTrack(t, "video-1", webrtc.MimeTypeVP8)
	// Override codec to VP8/90000
	pub.SetCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeVP8,
			ClockRate: 90000,
		},
		PayloadType: 96,
	})
	fw, _ := NewTrackForwarder(pub)
	pc := newTestPeerConnection(t, "sub-v", "Eve")
	if err := fw.AddSubscriber(pc); err != nil {
		t.Fatalf("AddSubscriber: %v", err)
	}
	got := pc.GetLocalTrack("video-1")
	if got == nil {
		t.Fatal("missing forwarded track")
	}
	if got.Codec().MimeType != webrtc.MimeTypeVP8 {
		t.Errorf("mime = %q, want VP8", got.Codec().MimeType)
	}
	if got.Codec().ClockRate != 90000 {
		t.Errorf("clockrate = %d, want 90000", got.Codec().ClockRate)
	}
	if got.Codec().PayloadType != 96 {
		t.Errorf("payloadtype = %d, want 96", got.Codec().PayloadType)
	}
}

func TestTrackForwarderForwardPacket(t *testing.T) {
	pub := newTestPublisherTrack(t, "audio-fwd", webrtc.MimeTypeOpus)
	fw, _ := NewTrackForwarder(pub)
	pc1 := newTestPeerConnection(t, "fwd-1", "F1")
	pc2 := newTestPeerConnection(t, "fwd-2", "F2")
	if err := fw.AddSubscriber(pc1); err != nil {
		t.Fatalf("AddSubscriber pc1: %v", err)
	}
	if err := fw.AddSubscriber(pc2); err != nil {
		t.Fatalf("AddSubscriber pc2: %v", err)
	}
	// Forwarding before any DTLS binding is expected to succeed (no bindings yet => no error)
	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    111,
			SequenceNumber: 1,
			Timestamp:      90000,
			SSRC:           12345,
		},
		Payload: []byte{0x11, 0x22, 0x33},
	}
	if err := fw.WriteRTP(pkt); err != nil {
		t.Fatalf("WriteRTP: %v", err)
	}
	if err := fw.ForwardRTPPacket(pkt); err != nil {
		t.Fatalf("ForwardRTPPacket: %v", err)
	}
	// Raw Write path
	raw, err := pkt.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if n, err := fw.Write(raw); err != nil || n != len(raw) {
		t.Fatalf("Write raw: n=%d err=%v", n, err)
	}
	if err := fw.WriteRTP(nil); err == nil {
		t.Error("expected error for nil packet")
	}
	if _, err := fw.Write([]byte{0x00, 0x01}); err == nil {
		t.Error("expected error for invalid RTP")
	}
	// Slow subscriber shouldn't block others: WriteRTP is async per subscriber
	// So this is just a smoke test that it returns quickly.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			_ = fw.WriteRTP(pkt)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WriteRTP blocked")
	}
}

func TestTrackForwarderStartStopCleansUpTracks(t *testing.T) {
	pub := newTestPublisherTrack(t, "audio-clean", webrtc.MimeTypeOpus)
	fw, _ := NewTrackForwarder(pub)
	pc := newTestPeerConnection(t, "clean-1", "Clean")
	if err := fw.AddSubscriber(pc); err != nil {
		t.Fatalf("AddSubscriber: %v", err)
	}
	if err := fw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Stop should remove tracks from subscribers
	if err := fw.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if pc.GetLocalTrack("audio-clean") != nil {
		t.Error("track should be removed after Stop")
	}
}

func TestTrackForwarderConcurrentAddRemoveStartStop(t *testing.T) {
	pub := newTestPublisherTrack(t, "audio-conc", webrtc.MimeTypeOpus)
	fw, _ := NewTrackForwarder(pub)
	pcs := make([]*PeerConnection, 8)
	for i := range pcs {
		pcs[i] = newTestPeerConnection(t, "conc-"+string(rune('A'+i)), "User")
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pc := pcs[idx%len(pcs)]
			_ = fw.AddSubscriber(pc)
			_ = fw.RemoveSubscriber(pc)
			_ = fw.Start()
			_ = fw.Stop()
			_ = fw.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2}, Payload: []byte{0x01}})
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent ops deadlocked")
	}
}

func TestPeerConnectionOnLocalTrackAddedHook(t *testing.T) {
	pc := newTestPeerConnection(t, "hook-pc", "Hooky")
	ch := make(chan *WebRTCTrack, 1)
	pc.OnLocalTrackAdded(func(track *WebRTCTrack) {
		ch <- track
	})
	dt, _ := domain.NewTrack("hook-track", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	pionTrack, _ := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "hook-track", "stream")
	wrapped := NewWebRTCTrack(dt, pionTrack, webrtc.RTPCodecParameters{})
	if err := pc.AddTrack(wrapped); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}
	select {
	case got := <-ch:
		if got != wrapped {
			t.Errorf("hook got %v, want %v", got, wrapped)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnLocalTrackAdded not fired")
	}
	// Clearing hook should not fire
	pc.OnLocalTrackAdded(nil)
	dt2, _ := domain.NewTrack("hook-track-2", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	pionTrack2, _ := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "hook-track-2", "stream")
	wrapped2 := NewWebRTCTrack(dt2, pionTrack2, webrtc.RTPCodecParameters{})
	if err := pc.AddTrack(wrapped2); err != nil {
		t.Fatalf("AddTrack 2: %v", err)
	}
	select {
	case got := <-ch:
		t.Fatalf("unexpected hook fire after clear: %v", got)
	case <-time.After(100 * time.Millisecond):
	}
}
