package signaling

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/sajadbayatani/slive/internal/config"
	"github.com/sajadbayatani/slive/internal/domain"
	apphttp "github.com/sajadbayatani/slive/internal/http"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

// obsCaptureHandler records slog records.
type obsCaptureHandler struct {
	mu      sync.Mutex
	records []slog.Record
	attrs   []map[string]any
}

func newObsCaptureHandler() *obsCaptureHandler { return &obsCaptureHandler{} }

func (h *obsCaptureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *obsCaptureHandler) Handle(_ context.Context, r slog.Record) error {
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
func (h *obsCaptureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *obsCaptureHandler) WithGroup(_ string) slog.Handler      { return h }
func (h *obsCaptureHandler) Attrs() []map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]map[string]any, len(h.attrs))
	copy(out, h.attrs)
	return out
}

func TestMetrics_ForwarderDroppedReflected(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })

	trackID := "obs-drop-track"
	room, pub := joinParticipant(t, h, "obs-drop-room", "pub-drop")

	payload, _ := json.Marshal(PublishTrackRequest{RoomID: room.ID(), ParticipantID: pub.ID(), Track: TrackInfo{ID: trackID, Kind: "audio", Source: "microphone"}})
	conn := newHeadlessConn(pub.ID(), room.ID())
	if err := h.handleMessage(conn, room, pub, &Message{Type: MessageTypePublishTrack, Data: payload}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	fwOld := h.getForwarder(trackID)
	if fwOld == nil {
		t.Fatalf("forwarder not found after publish")
	}
	pubTrack := fwOld.PublisherTrack()
	h.removeForwarder(trackID)
	fw, err := webrtc.NewTrackForwarderWithConfig(pubTrack, webrtc.ForwarderConfig{QueueSize: 2})
	if err != nil {
		t.Fatalf("NewTrackForwarderWithConfig: %v", err)
	}
	if err := fw.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	h.trackForwardersMutex.Lock()
	h.trackForwarders[trackID] = fw
	h.trackForwardersMutex.Unlock()
	t.Cleanup(func() { h.removeForwarder(trackID) })

	subs := make([]*webrtc.PeerConnection, 3)
	for i, id := range []string{"sub-drop-1", "sub-drop-2", "sub-drop-3"} {
		_, p := joinParticipant(t, h, "obs-drop-room", id)
		pc, _ := h.ensurePeerConnection(p, channelSender(make(chan string, 8)))
		subs[i] = pc
		mustSubscribeTrackViaHandler(t, h, room, p, trackID)
	}

	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 1}, Payload: []byte{0x01, 0x02}}
	snapBefore := h.Snapshot()
	beforeDropped := snapBefore.ForwarderDroppedTotal

	// Burst 100 packets; if no drops yet, retry bursts to ensure backpressure triggers.
	for attempt := 0; attempt < 5; attempt++ {
		for i := 0; i < 100; i++ {
			pkt.Header.SequenceNumber = uint16(i + attempt*100)
			_ = fw.WriteRTP(pkt)
		}
		if fw.TotalDropped() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	waitForCondition(t, 2*time.Second, "ForwarderDroppedTotal increment", func() bool {
		s := h.Snapshot()
		return s.ForwarderDroppedTotal > beforeDropped
	})
	snap := h.Snapshot()
	if snap.ForwarderDroppedTotal == 0 {
		t.Fatalf("ForwarderDroppedTotal = 0, want >0")
	}
	var sum uint64
	for _, pc := range subs {
		sum += fw.DroppedCount(pc)
	}
	if snap.ForwarderDroppedTotal != sum {
		t.Errorf("Snapshot ForwarderDroppedTotal %d != sum DroppedCount %d", snap.ForwarderDroppedTotal, sum)
	}
	if got := fw.TotalDropped(); got != sum {
		t.Errorf("TotalDropped %d != sum %d", got, sum)
	}
}

func TestMetrics_GCReapedReflected(t *testing.T) {
	h := newTestHandlerWithGCTTL(100 * time.Millisecond)
	t.Cleanup(func() { _ = h.Shutdown() })

	roomID := "obs-gc-room"
	var pIDs []string
	for i := 0; i < 3; i++ {
		pid := "gc-p" + string(rune('a'+i))
		pIDs = append(pIDs, pid)
		_, p := joinParticipant(t, h, roomID, pid)
		_, _ = h.ensurePeerConnection(p, channelSender(make(chan string, 8)))
	}
	room := h.roomManager.GetRoom(roomID)
	if room == nil {
		t.Fatal("room missing")
	}
	for _, pid := range pIDs {
		if p := room.GetParticipant(pid); p != nil {
			h.handleConnectionClosed(room, p)
		}
	}

	waitForCondition(t, 800*time.Millisecond, "GCReapedTotal 3", func() bool {
		s := h.Snapshot()
		return s.GCReapedTotal == 3
	})
	if got := h.GCReapedCount(); got != 3 {
		t.Errorf("GCReapedCount = %d, want 3", got)
	}
	snap := h.Snapshot()
	if snap.GCReapedTotal != 3 {
		t.Errorf("Snapshot GCReapedTotal = %d, want 3", snap.GCReapedTotal)
	}
	if snap.RoomsActive != 1 {
		t.Errorf("RoomsActive = %d, want 1", snap.RoomsActive)
	}
	if snap.ParticipantsActive != 0 {
		t.Errorf("ParticipantsActive = %d, want 0", snap.ParticipantsActive)
	}
	if len(room.Participants()) != 0 {
		t.Errorf("room participants = %d, want 0 after reap", len(room.Participants()))
	}
}

func TestHealthSnapshot_NoLockDuringWrite(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })

	room, pub := joinParticipant(t, h, "health-lock-room", "pub-lock")
	_, _ = h.ensurePeerConnection(pub, channelSender(make(chan string, 8)))
	mustPublishTrackViaHandler(t, h, room, pub, "audio-lock", "audio", "microphone")
	fw := waitForForwarder(t, h, "audio-lock", true)
	_ = fw

	router := apphttp.NewRouter(config.Config{HealthPath: "/health"}, apphttp.HandlerDeps{MetricsSnapshot: h.Snapshot})
	ts := httptest.NewServer(router.ServeMux())
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for ctx.Err() == nil {
				pid := "fuzz-" + string(rune('a'+idx%10))
				room := h.roomManager.GetRoom("health-lock-room")
				if room == nil {
					continue
				}
				p := room.GetParticipant(pid)
				if p == nil {
					np := domain.NewParticipant(pid, "User "+pid)
					_ = room.Join(np)
					np.SetRoom(room)
					_, _ = h.ensurePeerConnection(np, channelSender(make(chan string, 8)))
					p = np
				}
				switch idx % 4 {
				case 0:
					trackID := "t-" + pid
					payload, _ := json.Marshal(PublishTrackRequest{RoomID: room.ID(), ParticipantID: pid, Track: TrackInfo{ID: trackID, Kind: "audio", Source: "microphone"}})
					conn := newHeadlessConn(pid, room.ID())
					_ = h.handleMessage(conn, room, p, &Message{Type: MessageTypePublishTrack, Data: payload})
				case 1:
					payload, _ := json.Marshal(SubscribeTrackRequest{RoomID: room.ID(), ParticipantID: pid, TrackID: "audio-lock"})
					conn := newHeadlessConn(pid, room.ID())
					_ = h.handleMessage(conn, room, p, &Message{Type: MessageTypeSubscribeTrack, Data: payload})
				case 2:
					_ = h.Snapshot()
				case 3:
					h.reapGhost(room.ID(), pid)
				}
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}
	errCh := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				resp, err := http.Get(ts.URL + "/healthz")
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				if resp.StatusCode != http.StatusOK {
					select {
					case errCh <- err:
					default:
					}
				}
				resp.Body.Close()
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	var healthWg sync.WaitGroup
	healthErr := make(chan error, 20)
	for i := 0; i < 20; i++ {
		healthWg.Add(1)
		go func() {
			defer healthWg.Done()
			resp, err := http.Get(ts.URL + "/healthz")
			if err != nil {
				healthErr <- err
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				healthErr <- err
			}
		}()
	}
	healthWg.Wait()
	close(healthErr)
	for err := range healthErr {
		if err != nil {
			t.Errorf("health fetch error: %v", err)
		}
	}
	select {
	case err := <-errCh:
		t.Errorf("fuzz health error: %v", err)
	default:
	}
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("TestHealthSnapshot_NoLockDuringWrite deadlocked")
	}
}
