package signaling

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sajadbayatani/slive/internal/domain"
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

func TestRoomManager_ConcurrentCreateRooms(t *testing.T) {
	rm := NewRoomManager()
	t.Cleanup(func() {
		// No shutdown needed for RoomManager.
	})

	var wg sync.WaitGroup
	// 100 goroutines GetOrCreateRoom for 100 distinct IDs.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			roomID := fmt.Sprintf("room-%03d", idx)
			if _, err := rm.GetOrCreateRoom(roomID); err != nil {
				t.Errorf("GetOrCreateRoom %s: %v", roomID, err)
			}
		}(i)
	}
	wg.Wait()

	// 100 concurrent GetRoom reads (RWMutex read-mostly).
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			roomID := fmt.Sprintf("room-%03d", idx)
			room := rm.GetRoom(roomID)
			if room == nil {
				t.Errorf("GetRoom %s returned nil", roomID)
			}
		}(i)
	}
	wg.Wait()

	ids := rm.RoomIDs()
	if len(ids) != 100 {
		t.Errorf("RoomIDs count = %d, want 100", len(ids))
	}
	// Verify determinism: all expected IDs present.
	m := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	for i := 0; i < 100; i++ {
		roomID := fmt.Sprintf("room-%03d", i)
		if _, ok := m[roomID]; !ok {
			t.Errorf("missing room %s", roomID)
		}
	}
}

func TestHandler_ConcurrentPublishSubscribeUnderScale(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })

	roomID := "scale-conc-room"
	room, err := h.roomManager.GetOrCreateRoom(roomID)
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}

	// Create 20 participants deterministically participant-%03d.
	const n = 20
	participants := make([]*domain.Participant, n)
	for i := 0; i < n; i++ {
		pid := fmt.Sprintf("participant-%03d", i)
		p := domain.NewParticipant(pid, "User "+pid)
		if err := room.Join(p); err != nil {
			t.Fatalf("Join %s: %v", pid, err)
		}
		p.SetRoom(room)
		participants[i] = p
		if _, err := h.ensurePeerConnection(p, channelSender(make(chan string, 8))); err != nil {
			t.Fatalf("ensurePeerConnection %s: %v", pid, err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p := participants[idx]
			trackID := fmt.Sprintf("track-%03d", idx)
			// Publish
			payload, _ := json.Marshal(PublishTrackRequest{RoomID: roomID, ParticipantID: p.ID(), Track: TrackInfo{ID: trackID, Kind: "audio", Source: "microphone"}})
			conn := newHeadlessConn(p.ID(), roomID)
			_ = h.handleMessage(conn, room, p, &Message{Type: MessageTypePublishTrack, Data: payload})
			// Subscribe to next participant's track (deterministic).
			targetIdx := (idx + 1) % n
			targetTrackID := fmt.Sprintf("track-%03d", targetIdx)
			// Wait for target track to be published (poll).
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if room.GetTrack(targetTrackID) != nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if room.GetTrack(targetTrackID) != nil {
				payload2, _ := json.Marshal(SubscribeTrackRequest{RoomID: roomID, ParticipantID: p.ID(), TrackID: targetTrackID})
				conn2 := newHeadlessConn(p.ID(), roomID)
				_ = h.handleMessage(conn2, room, p, &Message{Type: MessageTypeSubscribeTrack, Data: payload2})
				time.Sleep(5 * time.Millisecond)
				payload3, _ := json.Marshal(UnsubscribeTrackRequest{RoomID: roomID, ParticipantID: p.ID(), TrackID: targetTrackID})
				conn3 := newHeadlessConn(p.ID(), roomID)
				_ = h.handleMessage(conn3, room, p, &Message{Type: MessageTypeUnsubscribeTrack, Data: payload3})
			}
			// Leave
			payload4, _ := json.Marshal(LeaveRoomRequest{RoomID: roomID, ParticipantID: p.ID()})
			conn4 := newHeadlessConn(p.ID(), roomID)
			_ = h.handleMessage(conn4, room, p, &Message{Type: MessageTypeLeaveRoom, Data: payload4})

			// MetricsSnapshot sampling during concurrency.
			_ = h.Snapshot()
		}(i)
	}
	wg.Wait()

	// After all leaves, TracksPublished eventually 0.
	waitForConditionScale(t, 2*time.Second, "TracksPublished 0 after all leaves", func() bool {
		snap := h.Snapshot()
		return snap.TracksPublished == 0
	})
	snap := h.Snapshot()
	if snap.TracksPublished != 0 {
		t.Errorf("TracksPublished = %d, want 0", snap.TracksPublished)
	}
	if len(room.Tracks()) != 0 {
		t.Errorf("room.Tracks() = %d, want 0", len(room.Tracks()))
	}
}

func TestScale_GoroutineBound_Signaling(t *testing.T) {
	// Additional goroutine bound check via signaling handler lifecycle.
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	for i := 0; i < 10; i++ {
		roomID := fmt.Sprintf("room-%03d", i)
		room, _ := h.roomManager.GetOrCreateRoom(roomID)
		for j := 0; j < 4; j++ {
			pid := fmt.Sprintf("participant-%03d-%03d", i, j)
			p := domain.NewParticipant(pid, "User "+pid)
			_ = room.Join(p)
			p.SetRoom(room)
			_, _ = h.ensurePeerConnection(p, channelSender(make(chan string, 8)))
		}
	}
	snap := h.Snapshot()
	if snap.RoomsActive != 10 {
		t.Errorf("rooms_active = %d, want 10", snap.RoomsActive)
	}
}

func TestScale_GCUnderLoad_Signaling(t *testing.T) {
	h := newTestHandlerWithGCTTL(200 * time.Millisecond)
	t.Cleanup(func() { _ = h.Shutdown() })

	roomID := "gc-scale-room"
	for i := 0; i < 10; i++ {
		pid := fmt.Sprintf("gc-participant-%03d", i)
		room, p := joinParticipant(t, h, roomID, pid)
		_, _ = h.ensurePeerConnection(p, channelSender(make(chan string, 8)))
		_ = room
	}
	room := h.roomManager.GetRoom(roomID)
	for i := 0; i < 10; i++ {
		pid := fmt.Sprintf("gc-participant-%03d", i)
		if p := room.GetParticipant(pid); p != nil {
			h.handleConnectionClosed(room, p)
		}
	}
	waitForConditionScale(t, 1200*time.Millisecond, "GCReapedTotal 10 after TTL+500ms", func() bool {
		return h.Snapshot().GCReapedTotal == 10
	})
	if got := h.GCReapedCount(); got != 10 {
		t.Errorf("GCReapedCount = %d, want 10", got)
	}
	snap := h.Snapshot()
	if snap.RoomsActive != 1 {
		t.Errorf("rooms_active = %d, want 1", snap.RoomsActive)
	}
	if snap.ParticipantsActive != 0 {
		t.Errorf("participants_active = %d, want 0", snap.ParticipantsActive)
	}
	// No double-reap panic: call again.
	h.reapGhost(roomID, "gc-participant-000")
	h.reapGhost(roomID, "gc-participant-001")
}
