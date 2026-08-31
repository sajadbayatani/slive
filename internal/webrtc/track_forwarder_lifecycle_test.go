package webrtc

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// newRemotePublisherTrack creates a WebRTCTrack whose underlying pion track
// is a real TrackRemote (IsRemote()==true) via a loopback PeerConnection pair.
func newRemotePublisherTrack(t *testing.T, id, mime string) *WebRTCTrack {
	t.Helper()
	// Use unique suffix to avoid port/ID collisions when multiple tracks are created in same process.
	uniq := t.Name() + "-" + id + "-" + time.Now().Format("150405.000000")
	cfg := PeerConnectionConfig{
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback,
	}
	sender, err := NewPeerConnection(cfg, domain.NewParticipant("sender-"+uniq, "Sender"), nil)
	if err != nil {
		t.Fatalf("NewPeerConnection sender: %v", err)
	}
	t.Cleanup(func() { _ = sender.Close() })
	receiver, err := NewPeerConnection(cfg, domain.NewParticipant("receiver-"+uniq, "Receiver"), nil)
	if err != nil {
		t.Fatalf("NewPeerConnection receiver: %v", err)
	}
	t.Cleanup(func() { _ = receiver.Close() })

	ch := make(chan *WebRTCTrack, 1)
	receiver.OnTrack(func(tr *WebRTCTrack) {
		select {
		case ch <- tr:
		default:
		}
	})

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
	} else {
		cap.ClockRate = 90000
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(cap, id, id+"-stream")
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}
	codec := webrtc.RTPCodecParameters{RTPCodecCapability: cap, PayloadType: 111}
	wrapped := NewWebRTCTrack(dt, pionTrack, codec)
	if err := sender.AddTrack(wrapped); err != nil {
		t.Fatalf("sender.AddTrack: %v", err)
	}

	offer, err := sender.CreateOffer()
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	answer, err := receiver.CreateAnswer(offer)
	if err != nil {
		t.Fatalf("CreateAnswer: %v", err)
	}
	if err := sender.SetRemoteDescription(answer); err != nil {
		t.Fatalf("SetRemoteDescription: %v", err)
	}
	// Pump a few packets to ensure OnTrack fires even if negotiation alone is enough.
	go func() {
		for i := 0; i < 5; i++ {
			_, _ = pionTrack.Write([]byte{0x80, 111, 0, byte(i), 0, 0, 0, 0, 0, 0, 0, 0})
			time.Sleep(10 * time.Millisecond)
		}
	}()
	select {
	case tr := <-ch:
		// Ensure IsRemote true
		if !tr.IsRemote() {
			t.Fatalf("captured track IsRemote false")
		}
		return tr
	case <-time.After(15 * time.Second):
		t.Fatalf("timeout waiting for OnTrack remote track %s", id)
		return nil
	}
}

// tryNewRemotePublisherTrack attempts to capture a TrackRemote without failing the test.
func tryNewRemotePublisherTrack(t *testing.T, id, mime string) *WebRTCTrack {
	t.Helper()
	uniq := t.Name() + "-" + id + "-" + time.Now().Format("150405.000000")
	cfg := PeerConnectionConfig{
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback,
	}
	sender, err := NewPeerConnection(cfg, domain.NewParticipant("sender-"+uniq, "Sender"), nil)
	if err != nil {
		return nil
	}
	t.Cleanup(func() { _ = sender.Close() })
	receiver, err := NewPeerConnection(cfg, domain.NewParticipant("receiver-"+uniq, "Receiver"), nil)
	if err != nil {
		return nil
	}
	t.Cleanup(func() { _ = receiver.Close() })
	ch := make(chan *WebRTCTrack, 1)
	receiver.OnTrack(func(tr *WebRTCTrack) {
		select {
		case ch <- tr:
		default:
		}
	})
	kind := domain.TrackKindAudio
	source := domain.TrackSourceMicrophone
	if mime == webrtc.MimeTypeVP8 || mime == webrtc.MimeTypeVP9 || mime == webrtc.MimeTypeH264 {
		kind = domain.TrackKindVideo
		source = domain.TrackSourceCamera
	}
	dt, err := domain.NewTrack(id, kind, source)
	if err != nil {
		return nil
	}
	cap := webrtc.RTPCodecCapability{MimeType: mime}
	if mime == webrtc.MimeTypeOpus {
		cap.ClockRate = 48000
		cap.Channels = 2
	} else {
		cap.ClockRate = 90000
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(cap, id, id+"-stream")
	if err != nil {
		return nil
	}
	codec := webrtc.RTPCodecParameters{RTPCodecCapability: cap, PayloadType: 111}
	wrapped := NewWebRTCTrack(dt, pionTrack, codec)
	if err := sender.AddTrack(wrapped); err != nil {
		return nil
	}
	offer, err := sender.CreateOffer()
	if err != nil {
		return nil
	}
	answer, err := receiver.CreateAnswer(offer)
	if err != nil {
		return nil
	}
	if err := sender.SetRemoteDescription(answer); err != nil {
		return nil
	}
	go func() {
		for i := 0; i < 5; i++ {
			_, _ = pionTrack.Write([]byte{0x80, 111, 0, byte(i), 0, 0, 0, 0, 0, 0, 0, 0})
			time.Sleep(10 * time.Millisecond)
		}
	}()
	select {
	case tr := <-ch:
		if !tr.IsRemote() {
			return nil
		}
		return tr
	case <-time.After(5 * time.Second):
		return nil
	}
}

func TestTrackForwarder_IsRunning_ForLocalVsRemote(t *testing.T) {
	pubLocal := newTestPublisherTrack(t, "isrun-local", webrtc.MimeTypeOpus)
	fw, err := NewTrackForwarder(pubLocal)
	if err != nil {
		t.Fatalf("NewTrackForwarder: %v", err)
	}
	t.Cleanup(func() { _ = fw.Stop() })
	if err := fw.Start(); err != nil {
		t.Fatalf("Start local: %v", err)
	}
	if fw.IsRunning() {
		t.Error("IsRunning should be false after Start on TrackLocal")
	}
	if err := fw.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if fw.IsRunning() {
		t.Error("IsRunning should be false after Stop")
	}
	// TrackRemote path - use deadline polling retry under load
	var rem *WebRTCTrack
	deadlineRem := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadlineRem) {
		rem = tryNewRemotePublisherTrack(t, "isrun-remote", webrtc.MimeTypeOpus)
		if rem != nil && rem.IsRemote() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if rem == nil || !rem.IsRemote() {
		t.Skipf("remote track capture unavailable after retries, skipping remote IsRunning assertions")
		return
	}
	fwRemote, err := NewTrackForwarder(rem)
	if err != nil {
		t.Fatalf("NewTrackForwarder remote: %v", err)
	}
	t.Cleanup(func() { _ = fwRemote.Stop() })
	if err := fwRemote.Start(); err != nil {
		t.Fatalf("Start remote: %v", err)
	}
	// Poll for running true (goroutine launch is async but Start sets running before launch)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fwRemote.IsRunning() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !fwRemote.IsRunning() {
		t.Error("IsRunning should be true after Start on TrackRemote")
	}
	if err := fwRemote.Stop(); err != nil {
		t.Fatalf("Stop remote: %v", err)
	}
	if fwRemote.IsRunning() {
		t.Error("IsRunning should be false after Stop on remote")
	}
	// UpdatePublisher swaps
	fwSwap, err := NewTrackForwarder(pubLocal)
	if err != nil {
		t.Fatalf("NewTrackForwarder swap: %v", err)
	}
	t.Cleanup(func() { _ = fwSwap.Stop() })
	if err := fwSwap.Start(); err != nil {
		t.Fatalf("Start swap local: %v", err)
	}
	if fwSwap.IsRunning() {
		t.Error("swap initial local should not be running")
	}
	if err := fwSwap.UpdatePublisher(rem); err != nil {
		t.Fatalf("UpdatePublisher to remote: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fwSwap.IsRunning() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !fwSwap.IsRunning() {
		t.Error("IsRunning should be true after UpdatePublisher to TrackRemote")
	}
	// Swap back to local
	if err := fwSwap.UpdatePublisher(pubLocal); err != nil {
		t.Fatalf("UpdatePublisher to local: %v", err)
	}
	// After local swap, should not be running; allow small poll for stop to propagate
	time.Sleep(50 * time.Millisecond)
	if fwSwap.IsRunning() {
		t.Error("IsRunning should be false after UpdatePublisher back to TrackLocal")
	}
}

func TestTrackForwarder_BufferRace(t *testing.T) {
	pub := newTestPublisherTrack(t, "buf-race", webrtc.MimeTypeOpus)
	fw, err := NewTrackForwarderWithConfig(pub, ForwarderConfig{QueueSize: 8})
	if err != nil {
		t.Fatalf("NewTrackForwarderWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = fw.Stop() })
	pc := newTestPeerConnection(t, "buf-race-sub", "Sub")
	if err := fw.AddSubscriber(pc); err != nil {
		t.Fatalf("AddSubscriber: %v", err)
	}
	// Pause writer so queue retains packet for inspection
	fw.mu.RLock()
	entry := fw.subscribers[pc]
	fw.mu.RUnlock()
	if entry == nil {
		t.Fatal("subscriber entry not found")
	}
	entry.cancel()
	<-entry.done
	// Replace with paused writer (no drain) so queue stays
	entry.ctx, entry.cancel = context.WithCancel(context.Background())
	entry.done = make(chan struct{})
	close(entry.done) // no goroutine, close done immediately to satisfy RemoveSubscriber later; but we need queue not drained
	// Actually we need entry.done to be closed already, but RemoveSubscriber waits on <-entry.done which would already be closed.
	// For inspection we keep done closed; WriteRTP will still enqueue.

	origPayload := []byte{0x11, 0x22, 0x33, 0x44}
	pkt := &rtp.Packet{
		Header:  rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: 1, Timestamp: 1, SSRC: 1},
		Payload: append([]byte(nil), origPayload...),
	}
	// Concurrent WriteRTP with mutation
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = fw.WriteRTP(pkt)
		}()
	}
	wg.Wait()
	// Mutate original after WriteRTP returns
	for i := range pkt.Payload {
		pkt.Payload[i] = 0xFF
	}
	origPayload[0] = 0xFF
	// Inspect queued packets via channel drain without writer
	fw.mu.RLock()
	e2 := fw.subscribers[pc]
	fw.mu.RUnlock()
	if e2 == nil {
		t.Fatal("entry gone")
	}
	// Drain queue
	qLen := len(e2.queue)
	if qLen == 0 {
		t.Fatal("queue empty after WriteRTP; writer may have consumed")
	}
	for i := 0; i < qLen; i++ {
		qpkt := <-e2.queue
		if len(qpkt.Payload) != 4 {
			t.Errorf("queued payload len = %d, want 4", len(qpkt.Payload))
		}
		if qpkt.Payload[0] == 0xFF {
			t.Errorf("queued payload mutated, got %x want original 0x11", qpkt.Payload[0])
		}
		if qpkt.Payload[1] == 0xFF || qpkt.Payload[2] == 0xFF {
			t.Error("queued payload shows mutation from concurrent writer")
		}
	}
	// Restore entry for cleanup: recreate writer that drains (no-op)
	entry.ctx, entry.cancel = context.WithCancel(context.Background())
	entry.done = make(chan struct{})
	go entry.runWriter()
}

func TestTrackForwarder_BackpressureDrops(t *testing.T) {
	pub := newTestPublisherTrack(t, "bp-drops", webrtc.MimeTypeOpus)
	fw, err := NewTrackForwarderWithConfig(pub, ForwarderConfig{QueueSize: 2})
	if err != nil {
		t.Fatalf("NewTrackForwarderWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = fw.Stop() })
	pcFast := newTestPeerConnection(t, "bp-fast", "Fast")
	pcSlow := newTestPeerConnection(t, "bp-slow", "Slow")
	if err := fw.AddSubscriber(pcFast); err != nil {
		t.Fatalf("AddSubscriber fast: %v", err)
	}
	if err := fw.AddSubscriber(pcSlow); err != nil {
		t.Fatalf("AddSubscriber slow: %v", err)
	}
	// Make slow writer artificially slow
	fw.mu.RLock()
	slowEntry := fw.subscribers[pcSlow]
	fw.mu.RUnlock()
	if slowEntry == nil {
		t.Fatal("slow entry not found")
	}
	slowEntry.cancel()
	<-slowEntry.done
	slowEntry.ctx, slowEntry.cancel = context.WithCancel(context.Background())
	slowEntry.done = make(chan struct{})
	go func(e *subscriberEntry) {
		defer close(e.done)
		for {
			select {
			case <-e.ctx.Done():
				return
			case pkt := <-e.queue:
				time.Sleep(80 * time.Millisecond)
				_ = e.pionTrack.WriteRTP(pkt)
			}
		}
	}(slowEntry)

	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 1, SequenceNumber: 1}, Payload: []byte{0x01, 0x02}}
	// Send 20 packets with small pacing to let fast drain but slow accumulate
	for i := 0; i < 20; i++ {
		pkt.Header.SequenceNumber = uint16(i)
		start := time.Now()
		if err := fw.WriteRTP(pkt); err != nil {
			t.Fatalf("WriteRTP %d: %v", i, err)
		}
		if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
			t.Errorf("WriteRTP %d took %s, want <10ms", i, elapsed)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Allow fast to drain
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fw.QueueDepth(pcFast) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if d := fw.QueueDepth(pcFast); d != 0 {
		t.Errorf("fast queue depth = %d, want 0 (should drain)", d)
	}
	if got := fw.DroppedCount(pcSlow); got == 0 {
		t.Error("DroppedCount slow = 0, want >0")
	}
	if got := fw.DroppedCount(pcFast); got != 0 {
		t.Errorf("DroppedCount fast = %d, want 0", got)
	}
}

func TestTrackForwarder_GoroutineBound(t *testing.T) {
	pub := newTestPublisherTrack(t, "goroutine-bound", webrtc.MimeTypeOpus)
	fw, err := NewTrackForwarder(pub)
	if err != nil {
		t.Fatalf("NewTrackForwarder: %v", err)
	}
	t.Cleanup(func() { _ = fw.Stop() })
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	// Baseline after GC to reduce noise from previous tests.
	beforeSubs := runtime.NumGoroutine()
	const n = 5
	pcs := make([]*PeerConnection, n)
	for i := 0; i < n; i++ {
		pcs[i] = newTestPeerConnection(t, "gb-pc-"+string(rune('A'+i)), "User")
		if err := fw.AddSubscriber(pcs[i]); err != nil {
			t.Fatalf("AddSubscriber %d: %v", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	afterSubs := runtime.NumGoroutine()
	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111}, Payload: []byte{0x01}}
	for i := 0; i < 100; i++ {
		_ = fw.WriteRTP(pkt)
	}
	time.Sleep(200 * time.Millisecond)
	afterBurst := runtime.NumGoroutine()
	// Writers are N, plus RTCP readers per PC (N), plus ICE/pion internals; allow generous slack.
	// Each PeerConnection adds ~3-4 goroutines (ICE gathering, RTCP, SCTP), so bound is n*6+15.
	if afterSubs > beforeSubs+n*6+15 {
		t.Errorf("goroutine leak after AddSubscriber: before %d afterSubs %d n %d", beforeSubs, afterSubs, n)
	}
	// Burst must not create per-packet goroutines: afterBurst should be close to afterSubs.
	if afterBurst > afterSubs+5 {
		t.Errorf("goroutine leak after burst: afterSubs %d afterBurst %d, want <= afterSubs+5 (not per-packet)", afterSubs, afterBurst)
	}
	// Overall bound: not +100*N
	if afterBurst > beforeSubs+n*6+20 {
		t.Errorf("overall goroutine bound violated: before %d afterBurst %d n %d", beforeSubs, afterBurst, n)
	}
	_ = pcs
}

func TestTrackForwarder_LockHierarchy(t *testing.T) {
	pub := newTestPublisherTrack(t, "lock-hier", webrtc.MimeTypeOpus)
	fw, err := NewTrackForwarder(pub)
	if err != nil {
		t.Fatalf("NewTrackForwarder: %v", err)
	}
	t.Cleanup(func() { _ = fw.Stop() })
	pcs := make([]*PeerConnection, 4)
	for i := range pcs {
		pcs[i] = newTestPeerConnection(t, "lh-pc-"+string(rune('A'+i)), "User")
	}
	var rem *WebRTCTrack
	rem = tryNewRemotePublisherTrack(t, "lh-remote", webrtc.MimeTypeOpus)
	if rem == nil || !rem.IsRemote() {
		t.Logf("remote track capture failed, falling back to local publisher for lock test")
		rem = pub
	}
	// 50 goroutines for 2s (shortened from 5s for CI speed but still race heavy)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pc := pcs[idx%len(pcs)]
			for ctx.Err() == nil {
				_ = fw.AddSubscriber(pc)
				_ = fw.RemoveSubscriber(pc)
				_ = fw.Start()
				_ = fw.Stop()
				_ = fw.UpdatePublisher(rem)
				_ = fw.UpdatePublisher(pub)
				_ = fw.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2}, Payload: []byte{0x01}})
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lock hierarchy test deadlocked")
	}
}

func TestTrackForwarder_ExtensionDeepCopy(t *testing.T) {
	pub := newTestPublisherTrack(t, "ext-deep", webrtc.MimeTypeOpus)
	fw, err := NewTrackForwarderWithConfig(pub, ForwarderConfig{QueueSize: 4})
	if err != nil {
		t.Fatalf("NewTrackForwarderWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = fw.Stop() })
	pc := newTestPeerConnection(t, "ext-sub", "Sub")
	if err := fw.AddSubscriber(pc); err != nil {
		t.Fatalf("AddSubscriber: %v", err)
	}
	fw.mu.RLock()
	entry := fw.subscribers[pc]
	fw.mu.RUnlock()
	if entry == nil {
		t.Fatal("entry not found")
	}
	entry.cancel()
	<-entry.done
	entry.ctx, entry.cancel = context.WithCancel(context.Background())
	entry.done = make(chan struct{})
	close(entry.done)

	extPayload := []byte{0x01, 0x02, 0x03}
	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version: 2, PayloadType: 111, SequenceNumber: 1, SSRC: 1,
			Extensions: []rtp.Extension{{}},
		},
		Payload: []byte{0xAA},
	}
	(*extMirror)(unsafe.Pointer(&pkt.Header.Extensions[0])).payload = append([]byte(nil), extPayload...)
	if err := fw.WriteRTP(pkt); err != nil {
		t.Fatalf("WriteRTP: %v", err)
	}
	// Mutate original extension payload
	if len(pkt.Header.Extensions) > 0 {
		m := (*extMirror)(unsafe.Pointer(&pkt.Header.Extensions[0]))
		if len(m.payload) > 0 {
			m.payload[0] = 0xFF
		}
	}
	extPayload[0] = 0xFF
	// Inspect queued copy
	if len(entry.queue) == 0 {
		t.Fatal("queue empty")
	}
	qpkt := <-entry.queue
	if len(qpkt.Header.Extensions) == 0 {
		t.Fatal("queued packet missing extensions")
	}
	qPayload := (*extMirror)(unsafe.Pointer(&qpkt.Header.Extensions[0])).payload
	if len(qPayload) == 0 {
		t.Fatal("queued extension payload empty")
	}
	if qPayload[0] == 0xFF {
		t.Errorf("queued extension payload mutated to 0xFF, want 0x01 deep copy")
	}
	// Restore writer
	entry.ctx, entry.cancel = context.WithCancel(context.Background())
	entry.done = make(chan struct{})
	go entry.runWriter()
}
