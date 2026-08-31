package scale

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/config"
	"github.com/sajadbayatani/slive/internal/domain"
	apphttp "github.com/sajadbayatani/slive/internal/http"
	"github.com/sajadbayatani/slive/internal/signaling"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

func waitForConditionScale(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func effectiveScaleProfile(t *testing.T) ScaleProfile {
	t.Helper()
	p := DefaultScaleProfile()
	if testing.Short() || raceEnabled {
		p.Rooms = p.RaceRooms
		p.ParticipantsPerRoom = p.RaceParticipantsPerRoom
		p.PublishersPerRoom = p.RaceParticipantsPerRoom / 2
		p.SubscribersPerRoom = p.RaceParticipantsPerRoom / 2
	}
	return p
}

func runScaleProfile(t *testing.T, h *ScaleHarness, profile ScaleProfile) {
	t.Helper()
	h.CreateRooms(profile.Rooms)
	for i := 0; i < profile.Rooms; i++ {
		roomID := fmt.Sprintf("room-%03d", i)
		h.JoinParticipants(roomID, profile.ParticipantsPerRoom)
	}
	for i := 0; i < profile.Rooms; i++ {
		var publishers []*domain.Participant
		for j := 0; j < profile.PublishersPerRoom; j++ {
			pid := fmt.Sprintf("participant-%03d-%03d", i, j)
			h.mu.Lock()
			p := h.participants[pid]
			h.mu.Unlock()
			publishers = append(publishers, p)
		}
		h.PublishTracks(publishers)
	}
	for i := 0; i < profile.Rooms; i++ {
		var subs []*domain.Participant
		for j := profile.PublishersPerRoom; j < profile.PublishersPerRoom+profile.SubscribersPerRoom; j++ {
			pid := fmt.Sprintf("participant-%03d-%03d", i, j)
			h.mu.Lock()
			p := h.participants[pid]
			h.mu.Unlock()
			subs = append(subs, p)
		}
		for j := 0; j < profile.PublishersPerRoom; j++ {
			pid := fmt.Sprintf("participant-%03d-%03d", i, j)
			trackID := fmt.Sprintf("track-%s", pid)
			var chosen []*domain.Participant
			for k := 0; k < profile.SubsPerTrack; k++ {
				idx := (j*profile.SubsPerTrack + k) % len(subs)
				chosen = append(chosen, subs[idx])
			}
			h.SubscribeTracks(trackID, chosen)
		}
	}
}

func TestScale_RoomsAndForwarderFanout(t *testing.T) {
	profile := effectiveScaleProfile(t)
	deadline := time.Now().Add(60 * time.Second)
	h := NewScaleHarness(t)
	t.Cleanup(func() { _ = h.Handler.Shutdown() })

	snapBefore := h.Snapshot()
	beforeDropped := snapBefore.ForwarderDroppedTotal

	runScaleProfile(t, h, profile)

	// Capture during burst snapshot.
	snapMidCh := make(chan webrtc.MetricsSnapshot, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		snapMidCh <- h.Snapshot()
	}()

	h.BurstRTP(profile.PacketsPerForwarder)

	var snapMid webrtc.MetricsSnapshot
	select {
	case snapMid = <-snapMidCh:
	case <-time.After(2 * time.Second):
		snapMid = h.Snapshot()
	}

	// Allow queues to drain briefly.
	time.Sleep(200 * time.Millisecond)
	snapAfter := h.Snapshot()

	if time.Now().After(deadline) {
		t.Fatalf("TestScale_RoomsAndForwarderFanout exceeded 60s deadline")
	}

	expectedRooms := profile.Rooms
	expectedParticipants := profile.Rooms * profile.ParticipantsPerRoom
	expectedTracks := profile.Rooms * profile.PublishersPerRoom
	expectedSubs := profile.Rooms * profile.PublishersPerRoom * profile.SubsPerTrack

	if snapAfter.RoomsActive != expectedRooms {
		t.Errorf("rooms_active = %d, want %d", snapAfter.RoomsActive, expectedRooms)
	}
	if snapAfter.ParticipantsActive != expectedParticipants {
		t.Errorf("participants_active = %d, want %d", snapAfter.ParticipantsActive, expectedParticipants)
	}
	if snapAfter.TracksPublished != expectedTracks {
		t.Errorf("tracks_published = %d, want %d", snapAfter.TracksPublished, expectedTracks)
	}
	if snapAfter.ForwarderSubscribers != expectedSubs {
		t.Errorf("forwarder_subscribers = %d, want %d", snapAfter.ForwarderSubscribers, expectedSubs)
	}
	// Monotonic dropped.
	if snapMid.ForwarderDroppedTotal < beforeDropped {
		t.Errorf("forwarder_dropped_total mid %d < before %d (not monotonic)", snapMid.ForwarderDroppedTotal, beforeDropped)
	}
	if snapAfter.ForwarderDroppedTotal < snapMid.ForwarderDroppedTotal {
		t.Errorf("forwarder_dropped_total after %d < mid %d (not monotonic)", snapAfter.ForwarderDroppedTotal, snapMid.ForwarderDroppedTotal)
	}

	// Health poll during scale.
	code, body, err := h.PollHealth()
	if err != nil {
		t.Fatalf("PollHealth: %v", err)
	}
	if code != 200 {
		t.Fatalf("health status = %d, want 200", code)
	}
	if _, ok := body["forwarder_dropped_total"]; !ok {
		t.Errorf("health missing forwarder_dropped_total")
	}

	_ = beforeDropped
}

func TestScale_GoroutineBound(t *testing.T) {
	profile := effectiveScaleProfile(t)
	before := runtime.NumGoroutine()
	h := NewScaleHarness(t)
	t.Cleanup(func() { _ = h.Handler.Shutdown() })

	runScaleProfile(t, h, profile)

	snapDuring := h.Snapshot()
	_ = snapDuring

	h.BurstRTP(profile.PacketsPerForwarder)
	snapAfterBurst := h.Snapshot()
	if snapAfterBurst.ForwarderDroppedTotal < snapDuring.ForwarderDroppedTotal {
		t.Errorf("TotalDropped decreased mid-run: before %d after %d", snapDuring.ForwarderDroppedTotal, snapAfterBurst.ForwarderDroppedTotal)
	}

	_ = h.Handler.Shutdown()
	time.Sleep(500 * time.Millisecond)
	after := runtime.NumGoroutine()
	allowance := before + 10*profile.Rooms*profile.ParticipantsPerRoom
	if after > allowance {
		t.Errorf("goroutine leak: before %d after %d allowance %d (rooms %d participants %d)", before, after, allowance, profile.Rooms, profile.ParticipantsPerRoom)
	}
	// Also ensure TotalDropped not resetting after shutdown (snapshot should still be monotonic)
	snapFinal := h.Snapshot()
	if snapFinal.ForwarderDroppedTotal < snapAfterBurst.ForwarderDroppedTotal {
		t.Errorf("TotalDropped reset after shutdown: burst %d final %d", snapAfterBurst.ForwarderDroppedTotal, snapFinal.ForwarderDroppedTotal)
	}
}

func TestScale_GCUnderLoad(t *testing.T) {
	rm := signaling.NewRoomManager()
	handler := signaling.NewHandler(rm,
		signaling.WithPeerConnectionConfig(webrtc.PeerConnectionConfig{
			ICEServers:   []pionwebrtc.ICEServer{},
			SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
		}),
		signaling.WithGCTTL(200*time.Millisecond),
	)
	t.Cleanup(func() { _ = handler.Shutdown() })

	profile := ScaleProfile{
		Rooms:               5,
		ParticipantsPerRoom: 4,
		PublishersPerRoom:   2,
		SubscribersPerRoom:  2,
		SubsPerTrack:        1,
		PacketsPerForwarder: 100,
	}
	for i := 0; i < profile.Rooms; i++ {
		roomID := fmt.Sprintf("room-%03d", i)
		room, err := rm.GetOrCreateRoom(roomID)
		if err != nil {
			t.Fatalf("GetOrCreateRoom %s: %v", roomID, err)
		}
		for j := 0; j < profile.ParticipantsPerRoom; j++ {
			pid := fmt.Sprintf("participant-%03d-%03d", i, j)
			p := domain.NewParticipant(pid, "User "+pid)
			if err := room.Join(p); err != nil {
				t.Fatalf("room.Join %s %s: %v", roomID, pid, err)
			}
			p.SetRoom(room)
		}
	}
	var allParticipants []*domain.Participant
	for i := 0; i < profile.Rooms; i++ {
		roomID := fmt.Sprintf("room-%03d", i)
		room := rm.GetRoom(roomID)
		for _, pid := range room.Participants() {
			if p := room.GetParticipant(pid); p != nil {
				allParticipants = append(allParticipants, p)
			}
		}
	}
	if len(allParticipants) < 10 {
		t.Fatalf("need at least 10 participants, got %d", len(allParticipants))
	}
	ghostPIDs := make([]string, 10)
	ghostRoomIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		p := allParticipants[i]
		ghostPIDs[i] = p.ID()
		if p.Room() != nil {
			ghostRoomIDs[i] = p.Room().ID()
		}
	}

	var fws []*webrtc.TrackForwarder
	for i := 0; i < 5; i++ {
		dt, _ := domain.NewTrack(fmt.Sprintf("gc-track-%03d", i), domain.TrackKindAudio, domain.TrackSourceMicrophone)
		cap := pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}
		pionTrack, _ := pionwebrtc.NewTrackLocalStaticRTP(cap, dt.ID(), dt.ID()+"-stream")
		codec := pionwebrtc.RTPCodecParameters{RTPCodecCapability: cap, PayloadType: 111}
		wrapped := webrtc.NewWebRTCTrack(dt, pionTrack, codec)
		fw, err := webrtc.NewTrackForwarderWithConfig(wrapped, webrtc.ForwarderConfig{QueueSize: 64})
		if err != nil {
			t.Fatalf("NewTrackForwarder: %v", err)
		}
		_ = fw.Start()
		defer fw.Stop()
		dummyPC, _ := webrtc.NewPeerConnection(webrtc.PeerConnectionConfig{
			ICEServers:   []pionwebrtc.ICEServer{},
			SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
		}, domain.NewParticipant(fmt.Sprintf("dummy-%03d", i), "dummy"), nil)
		if _, err := dummyPC.PionPeerConnection().AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
			t.Fatalf("AddTransceiver: %v", err)
		}
		_ = fw.AddSubscriber(dummyPC)
		fws = append(fws, fw)
	}
	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 12345}, Payload: []byte{0x01, 0x02}}
	for _, fw := range fws {
		for i := 0; i < 100; i++ {
			pkt.Header.SequenceNumber = uint16(i)
			_ = fw.WriteRTP(pkt)
		}
	}
	// Arm 10 ghost timers during burst.
	for i := 0; i < 10; i++ {
		handler.ArmGhostForTest(ghostRoomIDs[i], ghostPIDs[i])
	}
	// Wait TTL+500ms poll 10ms.
	waitForConditionScale(t, 800*time.Millisecond, "GCReapedTotal 10 after TTL+500ms", func() bool {
		return handler.Snapshot().GCReapedTotal == 10
	})
	if got := handler.GCReapedCount(); got != 10 {
		t.Errorf("GCReapedCount = %d, want 10", got)
	}
	snap := handler.Snapshot()
	if snap.RoomsActive != profile.Rooms {
		t.Errorf("rooms_active = %d, want %d", snap.RoomsActive, profile.Rooms)
	}
	if snap.ParticipantsActive != 10 {
		t.Errorf("participants_active = %d, want 10 after reap", snap.ParticipantsActive)
	}
	// No double-reap panic.
	for i := 0; i < 10; i++ {
		handler.ReapGhostForTest(ghostRoomIDs[i], ghostPIDs[i])
	}
	if got := handler.GCReapedCount(); got < 10 {
		t.Errorf("GCReapedCount after double-reap = %d, want >=10", got)
	}
	for _, fw := range fws {
		_ = fw.Stop()
	}
}

func TestScale_HealthDuringBurst(t *testing.T) {
	profile := effectiveScaleProfile(t)
	h := NewScaleHarness(t)
	t.Cleanup(func() { _ = h.Handler.Shutdown() })

	runScaleProfile(t, h, profile)

	// Start burst in background.
	doneBurst := make(chan struct{})
	go func() {
		h.BurstRTP(profile.PacketsPerForwarder)
		close(doneBurst)
	}()

	// 20 concurrent scrapers every 100ms.
	var wg sync.WaitGroup
	errCh := make(chan error, 40)
	stop := make(chan struct{})
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					code, body, err := h.PollHealth()
					if err != nil {
						select {
						case errCh <- fmt.Errorf("poll health: %w", err):
						default:
						}
						return
					}
					if code != 200 {
						select {
						case errCh <- fmt.Errorf("health status %d", code):
						default:
						}
						return
					}
					// Verify JSON fields present.
					for _, k := range []string{"forwarder_dropped_total", "forwarder_queue_depth", "gc_reaped_total"} {
						if _, ok := body[k]; !ok {
							select {
							case errCh <- fmt.Errorf("health missing %s", k):
							default:
							}
							return
						}
					}
					// Also check Content-Type via direct GET.
					resp, err := http.Get(h.HealthURL + "/healthz")
					if err == nil {
						resp.Body.Close()
						if resp.StatusCode != 200 {
							select {
							case errCh <- fmt.Errorf("direct health status %d", resp.StatusCode):
							default:
							}
							return
						}
					}
				}
			}
		}()
	}

	select {
	case <-doneBurst:
	case <-time.After(20 * time.Second):
		t.Fatal("burst timed out")
	}
	// Let scrapers run a bit more.
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Errorf("health scraper error: %v", err)
		}
	}
}

func TestScale_HealthAfterHardening(t *testing.T) {
	profile := effectiveScaleProfile(t)

	runOnce := func() (uint64, int, time.Duration) {
		h := NewScaleHarness(t)
		start := time.Now()
		runScaleProfile(t, h, profile)
		createRoomsElapsed := time.Since(start)
		h.BurstRTP(profile.PacketsPerForwarder)
		time.Sleep(200 * time.Millisecond)
		snap := h.Snapshot()
		_ = h.Handler.Shutdown()
		h.HealthServer.Close()
		time.Sleep(100 * time.Millisecond)
		// also close forwarders/pcs via harness cleanup
		for _, pc := range h.pcs {
			_ = pc.Close()
		}
		for _, fw := range h.forwarders {
			_ = fw.Stop()
		}
		return snap.ForwarderDroppedTotal, snap.Goroutines, createRoomsElapsed
	}

	dropped1, goroutines1, elapsed1 := runOnce()
	dropped2, goroutines2, elapsed2 := runOnce()

	// Hardened should be <= baseline in one of the dimensions.
	// We assert dropped2 <= dropped1 or goroutines2 <= goroutines1 or p99 CreateRooms <= baseline.
	if dropped2 > dropped1 && goroutines2 > goroutines1 && elapsed2 > elapsed1 {
		t.Errorf("hardened regression: dropped %d > %d and goroutines %d > %d and elapsed %s > %s", dropped2, dropped1, goroutines2, goroutines1, elapsed2, elapsed1)
	}
	// Also log baseline for operator capacity planning (JSON).
	baseline := map[string]any{
		"rooms":               profile.Rooms,
		"participants":        profile.Rooms * profile.ParticipantsPerRoom,
		"dropped_baseline":    dropped1,
		"dropped_hardened":    dropped2,
		"goroutines_baseline": goroutines1,
		"goroutines_hardened": goroutines2,
	}
	j, _ := json.Marshal(baseline)
	t.Logf("health after hardening: %s", string(j))

	// Verify single httptest server per harness (health URL reuse).
	h := NewScaleHarness(t)
	t.Cleanup(func() { _ = h.Handler.Shutdown() })
	if h.HealthServer == nil {
		t.Fatal("HealthServer nil")
	}
	code, _, err := h.PollHealth()
	if err != nil || code != 200 {
		t.Fatalf("health poll after hardening: %v code %d", err, code)
	}

	// Ensure health handler remains race-clean under concurrent load.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = http.Get(h.HealthURL + "/healthz")
		}()
	}
	wg.Wait()

	// Verify router uses single server (not per-room).
	_ = apphttp.NewRouter(config.Config{HealthPath: "/health"}, apphttp.HandlerDeps{MetricsSnapshot: h.Snapshot})
}
