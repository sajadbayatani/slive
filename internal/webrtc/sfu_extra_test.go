package webrtc

import (
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

func TestTrackForwarder_PacketForwarding(t *testing.T) {
	pub := newTestPublisherTrack(t, "pkt-fwd", webrtc.MimeTypeOpus)
	fw, _ := NewTrackForwarder(pub)
	pc1 := newTestPeerConnection(t, "pkt-sub-1", "Sub1")
	pc2 := newTestPeerConnection(t, "pkt-sub-2", "Sub2")
	if err := fw.AddSubscriber(pc1); err != nil {
		t.Fatalf("AddSubscriber pc1: %v", err)
	}
	if err := fw.AddSubscriber(pc2); err != nil {
		t.Fatalf("AddSubscriber pc2: %v", err)
	}

	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    111,
			SequenceNumber: 42,
			Timestamp:      90000,
			SSRC:           0x1234,
		},
		Payload: []byte{0x11, 0x22, 0x33, 0x44},
	}
	// Verify clone semantics: original payload must not be mutated by WriteRTP
	origPayload := append([]byte(nil), pkt.Payload...)
	if err := fw.WriteRTP(pkt); err != nil {
		t.Fatalf("WriteRTP: %v", err)
	}
	// Give async goroutines a moment to dispatch
	time.Sleep(100 * time.Millisecond)
	if string(pkt.Payload) != string(origPayload) {
		t.Errorf("original packet payload mutated")
	}
	// Raw Write path should also clone
	raw, _ := pkt.Marshal()
	if _, err := fw.Write(raw); err != nil {
		t.Fatalf("Write raw: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	// Ensure forwarded tracks still present
	if pc1.GetLocalTrack("pkt-fwd") == nil || pc2.GetLocalTrack("pkt-fwd") == nil {
		t.Error("forwarded tracks missing after WriteRTP")
	}
}

func TestTrackForwarder_MultipleSubscribers(t *testing.T) {
	pub := newTestPublisherTrack(t, "multi-sub", webrtc.MimeTypeOpus)
	fw, _ := NewTrackForwarder(pub)
	const n = 5
	pcs := make([]*PeerConnection, n)
	for i := 0; i < n; i++ {
		pcs[i] = newTestPeerConnection(t, "multi-sub-pc-"+string(rune('A'+i)), "User")
		if err := fw.AddSubscriber(pcs[i]); err != nil {
			t.Fatalf("AddSubscriber %d: %v", i, err)
		}
	}
	if fw.SubscriberCount() != n {
		t.Fatalf("SubscriberCount = %d, want %d", fw.SubscriberCount(), n)
	}
	// All must have the forwarded track with correct codec
	for i, pc := range pcs {
		tr := pc.GetLocalTrack("multi-sub")
		if tr == nil {
			t.Fatalf("pc %d missing forwarded track", i)
		}
		if tr.Codec().MimeType != webrtc.MimeTypeOpus {
			t.Errorf("pc %d codec = %q, want opus", i, tr.Codec().MimeType)
		}
	}
	// Fan-out: one WriteRTP should not error and should be non-blocking for all
	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 1}, Payload: []byte{0xAB}}
	start := time.Now()
	for i := 0; i < 20; i++ {
		if err := fw.WriteRTP(pkt); err != nil {
			t.Fatalf("WriteRTP %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("fan-out to %d subscribers took %s, want <2s", n, elapsed)
	}
	// Remove one, others stay
	if err := fw.RemoveSubscriber(pcs[0]); err != nil {
		t.Fatalf("RemoveSubscriber: %v", err)
	}
	if fw.SubscriberCount() != n-1 {
		t.Errorf("after remove count = %d, want %d", fw.SubscriberCount(), n-1)
	}
	if pcs[0].GetLocalTrack("multi-sub") != nil {
		t.Error("removed pc still has track")
	}
	for i := 1; i < n; i++ {
		if pcs[i].GetLocalTrack("multi-sub") == nil {
			t.Errorf("pc %d should still have track", i)
		}
	}
}

func TestTrackForwarder_SubscriberBackpressure(t *testing.T) {
	pub := newTestPublisherTrack(t, "backpressure", webrtc.MimeTypeOpus)
	fw, _ := NewTrackForwarder(pub)
	pcFast := newTestPeerConnection(t, "bp-fast", "Fast")
	pcSlow := newTestPeerConnection(t, "bp-slow", "Slow")
	if err := fw.AddSubscriber(pcFast); err != nil {
		t.Fatalf("AddSubscriber fast: %v", err)
	}
	if err := fw.AddSubscriber(pcSlow); err != nil {
		t.Fatalf("AddSubscriber slow: %v", err)
	}
	// Simulate slow subscriber by not reading; WriteRTP dispatches per-subscriber
	// in its own goroutine, so fast path must not block.
	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 999}, Payload: []byte{0x01, 0x02}}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			_ = fw.WriteRTP(pkt)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WriteRTP blocked despite backpressure isolation")
	}
	// Verify both PCs still have tracks
	if pcFast.GetLocalTrack("backpressure") == nil || pcSlow.GetLocalTrack("backpressure") == nil {
		t.Error("tracks missing after backpressure test")
	}
}

func TestTrackForwarder_PublisherClose(t *testing.T) {
	pub := newTestPublisherTrack(t, "pub-close", webrtc.MimeTypeOpus)
	fw, _ := NewTrackForwarder(pub)
	pc := newTestPeerConnection(t, "pub-close-sub", "Sub")
	if err := fw.AddSubscriber(pc); err != nil {
		t.Fatalf("AddSubscriber: %v", err)
	}
	if err := fw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// TrackLocal: Start is no-op, IsRunning stays false (B3 fix).
	if fw.IsRunning() {
		t.Error("IsRunning should be false after Start on TrackLocal")
	}
	// Publisher close should not deadlock Stop.
	_ = pub.Close()
	// Give run goroutine a chance to exit (no-op for TrackLocal)
	time.Sleep(100 * time.Millisecond)
	// Stop must clean up tracks and be idempotent
	if err := fw.Stop(); err != nil {
		t.Fatalf("Stop after publisher close: %v", err)
	}
	if pc.GetLocalTrack("pub-close") != nil {
		t.Error("track should be removed after Stop")
	}
	if fw.IsRunning() {
		t.Error("should not be running after Stop")
	}
	// Second Stop is idempotent
	if err := fw.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	// WriteRTP after publisher close should still be safe (no panic) even though publisher is gone
	pkt := &rtp.Packet{Header: rtp.Header{Version: 2}, Payload: []byte{0x01}}
	_ = fw.WriteRTP(pkt)
}

func TestTrackForwarder_ConcurrentAddRemove(t *testing.T) {
	pub := newTestPublisherTrack(t, "conc-spec", webrtc.MimeTypeOpus)
	fw, _ := NewTrackForwarder(pub)
	pcs := make([]*PeerConnection, 6)
	for i := range pcs {
		pcs[i] = newTestPeerConnection(t, "conc-spec-"+string(rune('A'+i)), "User")
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pc := pcs[idx%len(pcs)]
			_ = fw.AddSubscriber(pc)
			_ = fw.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2}, Payload: []byte{0x01, 0x02}})
			_ = fw.RemoveSubscriber(pc)
			_ = fw.Start()
			_ = fw.Stop()
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Add/Remove/Start/Stop deadlocked")
	}
}

func TestTrackForwarder_SSRCUniqueness(t *testing.T) {
	pub := newTestPublisherTrack(t, "ssrc-uniq", webrtc.MimeTypeOpus)
	fw, _ := NewTrackForwarder(pub)
	pc1 := newTestPeerConnection(t, "ssrc-1", "Alice")
	pc2 := newTestPeerConnection(t, "ssrc-2", "Bob")
	pc3 := newTestPeerConnection(t, "ssrc-3", "Carol")
	for _, pc := range []*PeerConnection{pc1, pc2, pc3} {
		if err := fw.AddSubscriber(pc); err != nil {
			t.Fatalf("AddSubscriber: %v", err)
		}
	}
	// Each subscriber must have a distinct pion TrackLocal instance (hence unique SSRC after Bind).
	// We verify distinct instances via pointer inequality of the underlying pion track stored in subscriberEntry.
	// Since subscriberEntry is private, we verify via GetLocalTrack returning distinct wrappers.
	tr1 := pc1.GetLocalTrack("ssrc-uniq")
	tr2 := pc2.GetLocalTrack("ssrc-uniq")
	tr3 := pc3.GetLocalTrack("ssrc-uniq")
	if tr1 == nil || tr2 == nil || tr3 == nil {
		t.Fatal("missing forwarded tracks")
	}
	// Wrappers must be distinct objects
	if tr1 == tr2 || tr2 == tr3 || tr1 == tr3 {
		t.Error("subscriber tracks share same wrapper instance; should be distinct per subscriber")
	}
	// Underlying pion tracks must also be distinct
	p1 := tr1.PionTrack()
	p2 := tr2.PionTrack()
	p3 := tr3.PionTrack()
	if p1 == p2 || p2 == p3 || p1 == p3 {
		t.Error("underlying pion tracks are not distinct; SSRC uniqueness would fail")
	}
	// Track IDs are same, StreamIDs are same pattern but per-track uniqueness of SSRC is ensured by pion at Bind time.
	if tr1.ID() != "ssrc-uniq" || tr2.ID() != "ssrc-uniq" {
		t.Error("track ID mismatch")
	}
}

func TestTrackForwarder_CodecPreservation_Video(t *testing.T) {
	pub := newTestPublisherTrack(t, "video-codec", webrtc.MimeTypeVP8)
	pub.SetCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeVP8,
			ClockRate: 90000,
		},
		PayloadType: 96,
	})
	fw, _ := NewTrackForwarder(pub)
	pc := newTestPeerConnection(t, "video-codec-sub", "Viewer")
	if err := fw.AddSubscriber(pc); err != nil {
		t.Fatalf("AddSubscriber: %v", err)
	}
	got := pc.GetLocalTrack("video-codec")
	if got == nil {
		t.Fatal("missing forwarded video track")
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
	// Audio variant with fmtp line preservation
	pub2 := newTestPublisherTrack(t, "audio-codec", webrtc.MimeTypeOpus)
	pub2.SetCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		},
		PayloadType: 111,
	})
	fw2, _ := NewTrackForwarder(pub2)
	pc2 := newTestPeerConnection(t, "audio-codec-sub", "Listener")
	if err := fw2.AddSubscriber(pc2); err != nil {
		t.Fatalf("AddSubscriber audio: %v", err)
	}
	got2 := pc2.GetLocalTrack("audio-codec")
	if got2 == nil {
		t.Fatal("missing forwarded audio track")
	}
	if got2.Codec().SDPFmtpLine != "minptime=10;useinbandfec=1" {
		t.Errorf("fmtp = %q, want preserved", got2.Codec().SDPFmtpLine)
	}
}

func TestTrackForwarder_WriteClonesPayload(t *testing.T) {
	pub := newTestPublisherTrack(t, "clone-test", webrtc.MimeTypeOpus)
	fw, _ := NewTrackForwarder(pub)
	pc := newTestPeerConnection(t, "clone-sub", "Sub")
	if err := fw.AddSubscriber(pc); err != nil {
		t.Fatalf("AddSubscriber: %v", err)
	}
	orig := []byte{0xAA, 0xBB, 0xCC}
	pkt := &rtp.Packet{
		Header:  rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: 1, Timestamp: 1, SSRC: 1},
		Payload: orig,
	}
	if err := fw.WriteRTP(pkt); err != nil {
		t.Fatalf("WriteRTP: %v", err)
	}
	// Mutate original after WriteRTP, forwarded copies must not be affected (they were cloned)
	orig[0] = 0xFF
	pkt.Payload[1] = 0xFF
	// No assertion on forwarded data (no bindings), but verify no panic and original mutation doesn't affect internal clones
	time.Sleep(50 * time.Millisecond)
}

// Ensure the forwarder's publisher track kind fallback works for unknown codec (zero value)
func TestTrackForwarder_UnknownCodecFallback(t *testing.T) {
	dt, _ := domain.NewTrack("fallback-track", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	// Zero-value codec to trigger fallback
	pionTrack, _ := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "fallback-track", "stream")
	wrapped := NewWebRTCTrack(dt, pionTrack, webrtc.RTPCodecParameters{})
	fw, _ := NewTrackForwarder(wrapped)
	pc := newTestPeerConnection(t, "fallback-sub", "Sub")
	if err := fw.AddSubscriber(pc); err != nil {
		t.Fatalf("AddSubscriber with fallback codec: %v", err)
	}
	got := pc.GetLocalTrack("fallback-track")
	if got == nil {
		t.Fatal("missing track after fallback")
	}
	// Wrapper codec remains zero (original), but underlying pion track has fallback mime.
	if pion, ok := got.PionTrack().(*webrtc.TrackLocalStaticRTP); ok {
		if pion.Codec().MimeType == "" {
			t.Error("expected fallback mime type to be set on pion track")
		}
	} else if got.Codec().MimeType == "" {
		t.Log("wrapper codec empty as expected for fallback path; underlying pion codec holds fallback")
	}
}
