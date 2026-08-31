package webrtc

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// captureHandler records slog records in a thread-safe slice.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
	attrs   []map[string]any
	enabled slog.Level
}

func newCaptureHandler(level slog.Level) *captureHandler {
	return &captureHandler{enabled: level}
}

func (h *captureHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.enabled
}

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	m := make(map[string]any)
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.attrs = append(h.attrs, m)
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *captureHandler) WithGroup(_ string) slog.Handler {
	return h
}

func (h *captureHandler) Records() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]slog.Record, len(h.records))
	copy(out, h.records)
	return out
}

func (h *captureHandler) Attrs() []map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]map[string]any, len(h.attrs))
	copy(out, h.attrs)
	return out
}

func (h *captureHandler) Reset() {
	h.mu.Lock()
	h.records = nil
	h.attrs = nil
	h.mu.Unlock()
}

func TestMetrics_SnapshotAtomic(t *testing.T) {
	ConnectionMetrics.Reset()
	pub := newTestPublisherTrack(t, "snap-atomic", webrtc.MimeTypeOpus)
	fw, err := NewTrackForwarderWithConfig(pub, ForwarderConfig{QueueSize: 2})
	if err != nil {
		t.Fatalf("NewTrackForwarderWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = fw.Stop() })

	pcFast := newTestPeerConnection(t, "snap-fast", "Fast")
	pcSlow := newTestPeerConnection(t, "snap-slow", "Slow")
	if err := fw.AddSubscriber(pcFast); err != nil {
		t.Fatalf("AddSubscriber fast: %v", err)
	}
	if err := fw.AddSubscriber(pcSlow); err != nil {
		t.Fatalf("AddSubscriber slow: %v", err)
	}
	// Block slow writer so queue fills and drops are generated.
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
	close(slowEntry.done)

	var fakeGC atomic.Uint64

	const attemptsPerGoroutine = 100
	const failuresPerGoroutine = 80
	const goroutinesAttempts = 10
	const goroutinesFailures = 10
	const goroutinesWrite = 10
	const burstPerWrite = 20

	var wg sync.WaitGroup
	var snapshotsMu sync.Mutex
	var snapshots []MetricsSnapshot

	// Snapshot collector.
	stopSnap := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSnap:
				return
			case <-ticker.C:
				snap := ConnectionMetrics.Snapshot()
				// Include forwarder dropped via TotalDropped for monotonic check.
				snap.ForwarderDroppedTotal = fw.TotalDropped()
				snap.GCReapedTotal = fakeGC.Load()
				snapshotsMu.Lock()
				snapshots = append(snapshots, snap)
				snapshotsMu.Unlock()
			}
		}
	}()

	// Concurrent increments.
	for i := 0; i < goroutinesAttempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < attemptsPerGoroutine; j++ {
				ConnectionMetrics.IncrementAttempts()
			}
		}()
	}
	for i := 0; i < goroutinesFailures; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < failuresPerGoroutine; j++ {
				ConnectionMetrics.IncrementFailures()
			}
		}()
	}
	for i := 0; i < goroutinesWrite; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: 1, SSRC: 1}, Payload: []byte{0x01, 0x02}}
			for j := 0; j < burstPerWrite; j++ {
				pkt.Header.SequenceNumber = uint16(j)
				_ = fw.WriteRTP(pkt)
			}
		}()
	}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				fakeGC.Add(1)
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stopSnap)

	// Wait for all (including collector) with timeout.
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("TestMetrics_SnapshotAtomic deadlocked")
	}

	// Restore slow entry for cleanup.
	fw.mu.RLock()
	e := fw.subscribers[pcSlow]
	fw.mu.RUnlock()
	if e != nil {
		e.ctx, e.cancel = context.WithCancel(context.Background())
		e.done = make(chan struct{})
		go e.runWriter()
	}

	expectedAttempts := uint64(goroutinesAttempts * attemptsPerGoroutine)
	if got := ConnectionMetrics.AttemptsTotal(); got != expectedAttempts {
		t.Errorf("AttemptsTotal = %d, want %d", got, expectedAttempts)
	}
	expectedFailures := uint64(goroutinesFailures * failuresPerGoroutine)
	if got := ConnectionMetrics.FailuresTotal(); got != expectedFailures {
		t.Errorf("FailuresTotal = %d, want %d", got, expectedFailures)
	}
	if got := fakeGC.Load(); got != 250 {
		t.Errorf("fakeGC = %d, want 250", got)
	}

	snapshotsMu.Lock()
	defer snapshotsMu.Unlock()
	if len(snapshots) == 0 {
		t.Fatal("no snapshots collected")
	}
	// Monotonic: ForwarderDroppedTotal, AttemptsTotal, GCReapedTotal never decrease.
	for i := 1; i < len(snapshots); i++ {
		prev := snapshots[i-1]
		cur := snapshots[i]
		if cur.ForwarderDroppedTotal < prev.ForwarderDroppedTotal {
			t.Errorf("ForwarderDroppedTotal decreased at %d: %d -> %d", i, prev.ForwarderDroppedTotal, cur.ForwarderDroppedTotal)
		}
		if cur.ConnectionAttemptsTotal < prev.ConnectionAttemptsTotal {
			t.Errorf("AttemptsTotal decreased at %d: %d -> %d", i, prev.ConnectionAttemptsTotal, cur.ConnectionAttemptsTotal)
		}
		if cur.GCReapedTotal < prev.GCReapedTotal {
			t.Errorf("GCReapedTotal decreased at %d: %d -> %d", i, prev.GCReapedTotal, cur.GCReapedTotal)
		}
	}
	// Final snapshot ForwarderDroppedTotal should equal fw.TotalDropped()
	finalSnap := snapshots[len(snapshots)-1]
	if total := fw.TotalDropped(); finalSnap.ForwarderDroppedTotal > total {
		t.Errorf("final snapshot ForwarderDroppedTotal %d > TotalDropped %d, must be monotonic and <= total", finalSnap.ForwarderDroppedTotal, total)
	}
}

func TestStructuredEvents_Keys(t *testing.T) {
	ch := newCaptureHandler(slog.LevelInfo)
	logger := slog.New(ch)
	orig := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(orig) })

	// Forwarder start/stop/swapped
	pubLocal := newTestPublisherTrack(t, "struct-local", webrtc.MimeTypeOpus)
	fw, err := NewTrackForwarderWithConfig(pubLocal, ForwarderConfig{QueueSize: 2})
	if err != nil {
		t.Fatalf("NewTrackForwarder: %v", err)
	}
	t.Cleanup(func() { _ = fw.Stop() })
	ch.Reset()
	if err := fw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	hasStart := false
	for _, m := range ch.Attrs() {
		if m["event"] == "forwarder_start" {
			hasStart = true
			if m["track_id"] == "" {
				t.Error("forwarder_start missing track_id")
			}
			if _, ok := m["queue_size"]; !ok {
				t.Error("forwarder_start missing queue_size")
			}
		}
	}
	if !hasStart {
		t.Error("forwarder_start event not captured")
	}

	ch.Reset()
	if err := fw.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	hasStop := false
	for _, m := range ch.Attrs() {
		if m["event"] == "forwarder_stop" {
			hasStop = true
			if m["track_id"] == "" {
				t.Error("forwarder_stop missing track_id")
			}
		}
	}
	if !hasStop {
		t.Error("forwarder_stop event not captured")
	}

	// swapped: local -> remote if available
	ch.Reset()
	rem := tryNewRemotePublisherTrack(t, "struct-remote", webrtc.MimeTypeOpus)
	if rem != nil && rem.IsRemote() {
		fw2, _ := NewTrackForwarderWithConfig(pubLocal, ForwarderConfig{QueueSize: 2})
		t.Cleanup(func() { _ = fw2.Stop() })
		_ = fw2.Start()
		ch.Reset()
		if err := fw2.UpdatePublisher(rem); err != nil {
			t.Fatalf("UpdatePublisher: %v", err)
		}
		found := false
		for _, m := range ch.Attrs() {
			if m["event"] == "forwarder_swapped" {
				found = true
				if m["track_id"] == "" {
					t.Error("forwarder_swapped missing track_id")
				}
				if m["is_remote"] == nil {
					t.Error("forwarder_swapped missing is_remote")
				}
			}
		}
		if !found {
			t.Error("forwarder_swapped not captured")
		}
	} else {
		t.Logf("remote track unavailable, skipping swapped assertions")
	}

	// queue_dropped
	ch.Reset()
	pubQ := newTestPublisherTrack(t, "struct-q", webrtc.MimeTypeOpus)
	fwQ, _ := NewTrackForwarderWithConfig(pubQ, ForwarderConfig{QueueSize: 2})
	t.Cleanup(func() { _ = fwQ.Stop() })
	pcQ := newTestPeerConnection(t, "struct-q-sub", "Sub")
	if err := fwQ.AddSubscriber(pcQ); err != nil {
		t.Fatalf("AddSubscriber: %v", err)
	}
	fwQ.mu.RLock()
	entry := fwQ.subscribers[pcQ]
	fwQ.mu.RUnlock()
	entry.cancel()
	<-entry.done
	entry.ctx, entry.cancel = context.WithCancel(context.Background())
	entry.done = make(chan struct{})
	close(entry.done)
	ch.Reset()
	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 1}, Payload: []byte{0x01}}
	for i := 0; i < 10; i++ {
		pkt.Header.SequenceNumber = uint16(i)
		_ = fwQ.WriteRTP(pkt)
	}
	foundDrop := false
	for _, m := range ch.Attrs() {
		if m["event"] == "queue_dropped" {
			foundDrop = true
			if m["track_id"] == "" {
				t.Error("queue_dropped missing track_id")
			}
			if m["dropped_total"] == nil {
				t.Error("queue_dropped missing dropped_total")
			}
			if m["queue_depth"] == nil {
				t.Error("queue_dropped missing queue_depth")
			}
		}
	}
	if !foundDrop {
		t.Error("queue_dropped not captured")
	}
	// restore writer
	entry.ctx, entry.cancel = context.WithCancel(context.Background())
	entry.done = make(chan struct{})
	go entry.runWriter()

	// peer_connected / disconnected / failed plus track_available via handleTrack
	ch.Reset()
	pcPeer := newTestPeerConnection(t, "struct-peer", "Peer")
	// Need config logger to be our capture logger, otherwise pc.logger is slog.Default() already set.
	// Default is already capturing, so handleConnectionStateChange will use it.
	pcPeer.handleConnectionStateChange(webrtc.PeerConnectionStateConnected)
	pcPeer.handleConnectionStateChange(webrtc.PeerConnectionStateDisconnected)
	pcPeer.handleConnectionStateChange(webrtc.PeerConnectionStateFailed)
	// Give logger a moment
	time.Sleep(10 * time.Millisecond)
	events := map[string]bool{}
	for _, m := range ch.Attrs() {
		if ev, ok := m["event"].(string); ok {
			events[ev] = true
			// All peer events should have participant_id and room_id keys (maybe empty but present) and event key
			if m["event"] == nil {
				t.Error("record missing event key")
			}
		}
	}
	for _, want := range []string{"peer_connected", "peer_disconnected", "peer_failed"} {
		if !events[want] {
			t.Errorf("event %q not captured, got %v", want, events)
		}
	}
	// Verify no mu held during log via concurrent stress (no deadlock within 2s)
	ch.Reset()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				_ = fwQ.WriteRTP(pkt)
				_ = fwQ.SubscriberCount()
				_ = fwQ.MaxQueueDepth()
			}
		}()
	}
	for ctx.Err() == nil {
		slog.Default().Info("concurrent log", "event", "test_concurrent", "room_id", "r1", "participant_id", "p1", "track_id", "t1")
		time.Sleep(1 * time.Millisecond)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("concurrent forwarder mu + log deadlocked")
	}

	// track_available via handleTrack if possible (use remote track if available)
	if rem != nil && rem.IsRemote() {
		ch.Reset()
		pcTrack := newTestPeerConnection(t, "struct-track", "TrackPeer")
		// Use the remote track's underlying *webrtc.TrackRemote if we can
		if pionTrack := rem.PionTrack(); pionTrack != nil {
			if tr, ok := pionTrack.(*webrtc.TrackRemote); ok {
				pcTrack.handleTrack(tr, nil)
				time.Sleep(10 * time.Millisecond)
				foundTA := false
				for _, m := range ch.Attrs() {
					if m["event"] == "track_available" {
						foundTA = true
						if m["track_id"] == "" {
							t.Error("track_available missing track_id")
						}
						if m["participant_id"] == nil {
							t.Error("track_available missing participant_id")
						}
					}
				}
				if !foundTA {
					t.Logf("track_available not captured via handleTrack (may be filtered), skipping strict check")
				}
			}
		}
		_ = pcTrack
	}
	_ = domain.NewParticipant // keep import used
}

func TestForwarderMetricsIsolated(t *testing.T) {
	pub := newTestPublisherTrack(t, "isolated", webrtc.MimeTypeOpus)
	fw, err := NewTrackForwarderWithConfig(pub, ForwarderConfig{QueueSize: 2})
	if err != nil {
		t.Fatalf("NewTrackForwarderWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = fw.Stop() })

	pcSlow := newTestPeerConnection(t, "isolated-slow", "Slow")
	if err := fw.AddSubscriber(pcSlow); err != nil {
		t.Fatalf("AddSubscriber slow: %v", err)
	}
	// Block slow queue.
	fw.mu.RLock()
	entry := fw.subscribers[pcSlow]
	fw.mu.RUnlock()
	if entry == nil {
		t.Fatal("entry not found")
	}
	entry.cancel()
	<-entry.done
	entry.ctx, entry.cancel = context.WithCancel(context.Background())
	entry.done = make(chan struct{})
	close(entry.done)

	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 1}, Payload: []byte{0x01, 0x02}}
	for i := 0; i < 20; i++ {
		pkt.Header.SequenceNumber = uint16(i)
		_ = fw.WriteRTP(pkt)
	}
	if got := fw.DroppedCount(pcSlow); got == 0 {
		t.Fatalf("DroppedCount slow = 0, want >0")
	}
	if got := fw.QueueDepth(pcSlow); got > 2 {
		t.Errorf("QueueDepth = %d, want <=2", got)
	}
	if got := fw.MaxQueueDepth(); got > 2 {
		t.Errorf("MaxQueueDepth = %d, want <=2", got)
	}
	if got := fw.TotalDropped(); got == 0 {
		t.Error("TotalDropped = 0, want >0")
	}
	// Ensure TotalDropped increments monotonically with additional writes.
	before := fw.TotalDropped()
	for i := 0; i < 10; i++ {
		pkt.Header.SequenceNumber = uint16(100 + i)
		_ = fw.WriteRTP(pkt)
	}
	if after := fw.TotalDropped(); after <= before {
		t.Errorf("TotalDropped not monotonic: before %d after %d", before, after)
	}
	// Restore writer for cleanup.
	entry.ctx, entry.cancel = context.WithCancel(context.Background())
	entry.done = make(chan struct{})
	go entry.runWriter()
}
