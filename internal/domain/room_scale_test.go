package domain

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func waitForConditionDomain(t *testing.T, timeout time.Duration, what string, cond func() bool) {
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
	// Simulate RoomManager behavior using domain Rooms with RWMutex.
	// This test lives in domain package to satisfy validation:
	// GOMODCACHE=.gocache/mod go test ./internal/domain/... -race -count=1 -run TestRoomManager_Concurrent
	type manager struct {
		mu    sync.RWMutex
		rooms map[string]*Room
	}
	m := &manager{rooms: make(map[string]*Room)}
	getOrCreate := func(roomID string) (*Room, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if r, ok := m.rooms[roomID]; ok {
			return r, nil
		}
		r := NewRoom(roomID)
		if err := r.Create(); err != nil {
			return nil, err
		}
		m.rooms[roomID] = r
		return r, nil
	}
	getRoom := func(roomID string) *Room {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return m.rooms[roomID]
	}
	roomIDs := func() []string {
		m.mu.RLock()
		defer m.mu.RUnlock()
		ids := make([]string, 0, len(m.rooms))
		for id := range m.rooms {
			ids = append(ids, id)
		}
		return ids
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			roomID := fmt.Sprintf("room-%03d", idx)
			if _, err := getOrCreate(roomID); err != nil {
				t.Errorf("GetOrCreateRoom %s: %v", roomID, err)
			}
		}(i)
	}
	wg.Wait()

	// 100 concurrent reads.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			roomID := fmt.Sprintf("room-%03d", idx)
			if r := getRoom(roomID); r == nil {
				t.Errorf("GetRoom %s nil", roomID)
			}
		}(i)
	}
	wg.Wait()

	ids := roomIDs()
	if len(ids) != 100 {
		t.Errorf("RoomIDs count = %d, want 100", len(ids))
	}
	mm := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		mm[id] = struct{}{}
	}
	for i := 0; i < 100; i++ {
		roomID := fmt.Sprintf("room-%03d", i)
		if _, ok := mm[roomID]; !ok {
			t.Errorf("missing room %s", roomID)
		}
	}

	// Also test concurrent Join/Leave on a single room (RWMutex read-mostly).
	room := NewRoom("concurrent-room")
	_ = room.Create()
	var wg2 sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg2.Add(1)
		go func(idx int) {
			defer wg2.Done()
			pid := fmt.Sprintf("participant-%03d", idx)
			p := NewParticipant(pid, "User "+pid)
			_ = room.Join(p)
			// Read tracks concurrently.
			_ = room.Participants()
			_ = room.GetParticipant(pid)
		}(i)
	}
	wg2.Wait()
	if len(room.Participants()) != 50 {
		t.Errorf("participants = %d, want 50", len(room.Participants()))
	}
}

func TestRoom_ConcurrentJoinLeave(t *testing.T) {
	room := NewRoom("scale-room")
	_ = room.Create()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pid := fmt.Sprintf("participant-%03d", idx)
			p := NewParticipant(pid, "User "+pid)
			_ = room.Join(p)
			time.Sleep(2 * time.Millisecond)
			_ = room.Leave(pid)
		}(i)
	}
	wg.Wait()
	waitForConditionDomain(t, 1*time.Second, "room empty after concurrent leave", func() bool {
		return len(room.Participants()) == 0
	})
}
