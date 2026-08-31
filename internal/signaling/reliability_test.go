package signaling

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/sajadbayatani/slive/internal/domain"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

func waitForCondition(t *testing.T, timeout time.Duration, what string, cond func() bool) {
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

func newTestHandlerWithGCTTL(ttl time.Duration) *Handler {
	return NewHandler(NewRoomManager(), WithPeerConnectionConfig(newTestPeerConnectionConfig()), WithGCTTL(ttl))
}

func TestGhostGC_ReapsAfterTTL(t *testing.T) {
	h := newTestHandlerWithGCTTL(100 * time.Millisecond)
	t.Cleanup(func() { _ = h.Shutdown() })

	room, alice := joinParticipant(t, h, "ghost-reap-room", "alice-reap")
	_, _ = h.ensurePeerConnection(alice, channelSender(make(chan string, 8)))
	mustPublishTrackViaHandler(t, h, room, alice, "audio-ghost", "audio", "microphone")

	_, bob := joinParticipant(t, h, "ghost-reap-room", "bob-reap")
	_, _ = h.ensurePeerConnection(bob, channelSender(make(chan string, 8)))

	h.handleConnectionClosed(room, bob)

	waitForCondition(t, 800*time.Millisecond, "ghost reap bob", func() bool {
		return h.roomManager.GetRoom("ghost-reap-room").GetParticipant("bob-reap") == nil
	})
	if got := h.getPeerConnection("bob-reap"); got != nil {
		t.Error("peer connection still present after ghost reap")
	}
	// Publisher track should remain in room registry; forwarder may be pruned when SubscriberCount==0 (idle forwarder GC)
	if tr := h.roomManager.GetRoom("ghost-reap-room").GetTrack("audio-ghost"); tr == nil {
		t.Error("publisher track should remain after bob reap")
	}
	if got := h.GCReapedCount(); got != 1 {
		t.Errorf("GCReapedCount = %d, want 1", got)
	}
	// Idempotent second reap should be safe
	h.reapGhost("ghost-reap-room", "bob-reap")
	if got := h.GCReapedCount(); got < 1 {
		t.Errorf("GCReapedCount = %d, want >=1", got)
	}
}

func TestGhostGC_CancelOnReconnect(t *testing.T) {
	h := newTestHandlerWithGCTTL(100 * time.Millisecond)
	t.Cleanup(func() { _ = h.Shutdown() })

	room, _ := joinParticipant(t, h, "ghost-cancel-room", "alice-cancel")
	// alice not needed for this test, but keep room alive
	_ = room

	_, bob := joinParticipant(t, h, "ghost-cancel-room", "bob-cancel")
	sink1 := make(chan string, 8)
	_, _ = h.ensurePeerConnection(bob, channelSender(sink1))

	h.handleConnectionClosed(room, bob)

	time.Sleep(30 * time.Millisecond)
	sink2 := make(chan string, 8)
	if _, err := h.ensurePeerConnection(bob, channelSender(sink2)); err != nil {
		t.Fatalf("reconnect ensurePeerConnection: %v", err)
	}
	h.cancelGhostTimer("bob-cancel")

	time.Sleep(150 * time.Millisecond)
	waitForCondition(t, 500*time.Millisecond, "bob still present after cancel", func() bool {
		return h.roomManager.GetRoom("ghost-cancel-room").GetParticipant("bob-cancel") != nil
	})
	if got := h.getPeerConnection("bob-cancel"); got == nil {
		t.Error("peer connection should still be present after cancel")
	}
	if got := h.GCReapedCount(); got != 0 {
		t.Errorf("GCReapedCount = %d, want 0 after cancel", got)
	}
}

func TestGhostGC_ExplicitLeave(t *testing.T) {
	h := newTestHandlerWithGCTTL(100 * time.Millisecond)
	t.Cleanup(func() { _ = h.Shutdown() })

	room, alice := joinParticipant(t, h, "ghost-leave-room", "alice-leave")
	_, _ = h.ensurePeerConnection(alice, channelSender(make(chan string, 8)))
	mustPublishTrackViaHandler(t, h, room, alice, "audio-leave", "audio", "microphone")

	_, bob := joinParticipant(t, h, "ghost-leave-room", "bob-leave")
	_, _ = h.ensurePeerConnection(bob, channelSender(make(chan string, 8)))

	h.handleConnectionClosed(room, bob)
	time.Sleep(20 * time.Millisecond)

	payload, _ := json.Marshal(LeaveRoomRequest{RoomID: room.ID(), ParticipantID: bob.ID()})
	conn := newHeadlessConn(bob.ID(), room.ID())
	if err := h.handleMessage(conn, room, bob, &Message{Type: MessageTypeLeaveRoom, Data: payload}); err != nil {
		t.Fatalf("handleMessage leave: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	waitForCondition(t, 300*time.Millisecond, "bob removed via leave", func() bool {
		return h.roomManager.GetRoom("ghost-leave-room").GetParticipant("bob-leave") == nil
	})
	if got := h.GCReapedCount(); got != 0 {
		t.Errorf("GCReapedCount = %d, want 0 explicit leave should not reap", got)
	}
	if tr := h.roomManager.GetRoom("ghost-leave-room").GetTrack("audio-leave"); tr == nil {
		t.Error("publisher track should remain after bob explicit leave")
	}
}

func TestGhostGC_ConcurrentReaps(t *testing.T) {
	h := newTestHandlerWithGCTTL(100 * time.Millisecond)
	t.Cleanup(func() { _ = h.Shutdown() })

	roomID := "ghost-conc-room"
	var pIDs []string
	for i := 0; i < 10; i++ {
		pid := "conc-" + string(rune('a'+i))
		pIDs = append(pIDs, pid)
		_, p := joinParticipant(t, h, roomID, pid)
		_, _ = h.ensurePeerConnection(p, channelSender(make(chan string, 8)))
		if i%2 == 0 {
			mustPublishTrackViaHandler(t, h, h.roomManager.GetRoom(roomID), p, "track-"+pid, "audio", "microphone")
		}
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
	waitForCondition(t, 2*time.Second, "all ghosts reaped", func() bool {
		return h.GCReapedCount() >= 10
	})
	for _, pid := range pIDs {
		if got := room.GetParticipant(pid); got != nil {
			t.Errorf("participant %s still present after concurrent reap", pid)
		}
		if got := h.getPeerConnection(pid); got != nil {
			t.Errorf("peer connection %s still present", pid)
		}
	}
}

func TestBackpressure_Integration(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })

	room, publisher := joinParticipant(t, h, "bp-int-room", "pub-bp")
	_, _ = h.ensurePeerConnection(publisher, channelSender(make(chan string, 8)))
	mustPublishTrackViaHandler(t, h, room, publisher, "audio-bp", "audio", "microphone")
	fw := waitForForwarder(t, h, "audio-bp", true)

	subs := make([]*webrtc.PeerConnection, 3)
	ids := []string{"sub-bp-1", "sub-bp-2", "sub-bp-3"}
	for i, id := range ids {
		_, p := joinParticipant(t, h, "bp-int-room", id)
		pc, _ := h.ensurePeerConnection(p, channelSender(make(chan string, 8)))
		subs[i] = pc
		mustSubscribeTrackViaHandler(t, h, room, p, "audio-bp")
	}

	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 1, SequenceNumber: 1}, Payload: []byte{0x01, 0x02}}
	start := time.Now()
	for i := 0; i < 100; i++ {
		pkt.Header.SequenceNumber = uint16(i)
		if err := fw.WriteRTP(pkt); err != nil {
			t.Fatalf("WriteRTP %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("burst 100 WriteRTP took %s, should be non-blocking", elapsed)
	}
	if fw.SubscriberCount() != 3 {
		t.Errorf("SubscriberCount = %d, want 3", fw.SubscriberCount())
	}
	for i, pc := range subs {
		if pc.GetLocalTrack("audio-bp") == nil {
			t.Errorf("sub %d track missing after burst", i)
		}
	}
	_ = subs

	room2, pub2 := joinParticipant(t, h, "bp-q2-room", "pub-q2")
	_, _ = h.ensurePeerConnection(pub2, channelSender(make(chan string, 8)))
	mustPublishTrackViaHandler(t, h, room2, pub2, "audio-q2", "audio", "microphone")
	fw2 := waitForForwarder(t, h, "audio-q2", true)
	if got := fw2.TotalDropped(); got != 0 {
		t.Logf("TotalDropped = %d after burst (may be 0 with large queue)", got)
	}
}

func TestLockHierarchy_ConcurrentPublishSubscribe(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })

	roomID := "lock-hier-room"
	_, _ = joinParticipant(t, h, roomID, "base-lock")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pid := "lh-" + string(rune('a'+idx%10))
			for ctx.Err() == nil {
				room := h.roomManager.GetRoom(roomID)
				if room == nil {
					room, _ = h.roomManager.GetOrCreateRoom(roomID)
				}
				p := room.GetParticipant(pid)
				if p == nil {
					np := domain.NewParticipant(pid, "User "+pid)
					_ = room.Join(np)
					np.SetRoom(room)
					p = np
					_, _ = h.ensurePeerConnection(p, channelSender(make(chan string, 8)))
				}
				switch idx % 5 {
				case 0:
					trackID := "t-" + pid + "-a"
					payload, _ := json.Marshal(PublishTrackRequest{RoomID: roomID, ParticipantID: pid, Track: TrackInfo{ID: trackID, Kind: "audio", Source: "microphone"}})
					conn := newHeadlessConn(pid, roomID)
					_ = h.handleMessage(conn, room, p, &Message{Type: MessageTypePublishTrack, Data: payload})
				case 1:
					trackID := "t-base-lock-a"
					if room.GetTrack(trackID) != nil {
						payload, _ := json.Marshal(SubscribeTrackRequest{RoomID: roomID, ParticipantID: pid, TrackID: trackID})
						conn := newHeadlessConn(pid, roomID)
						_ = h.handleMessage(conn, room, p, &Message{Type: MessageTypeSubscribeTrack, Data: payload})
					}
				case 2:
					trackID := "t-" + pid + "-a"
					payload, _ := json.Marshal(UnsubscribeTrackRequest{RoomID: roomID, ParticipantID: pid, TrackID: trackID})
					conn := newHeadlessConn(pid, roomID)
					_ = h.handleMessage(conn, room, p, &Message{Type: MessageTypeUnsubscribeTrack, Data: payload})
				case 3:
					payload, _ := json.Marshal(LeaveRoomRequest{RoomID: roomID, ParticipantID: pid})
					conn := newHeadlessConn(pid, roomID)
					_ = h.handleMessage(conn, room, p, &Message{Type: MessageTypeLeaveRoom, Data: payload})
				case 4:
					h.reapGhost(roomID, pid)
				}
				_, _ = h.ensurePeerConnection(p, channelSender(make(chan string, 8)))
				time.Sleep(2 * time.Millisecond)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lock hierarchy concurrent test deadlocked")
	}
	room := h.roomManager.GetRoom(roomID)
	if room != nil {
		for _, tid := range room.Tracks() {
			tr := room.GetTrack(tid)
			if tr == nil {
				t.Errorf("track %s listed but nil", tid)
			} else if tr.Publisher() == nil && tr.HasSubscribers() {
				t.Logf("track %s has subscribers but no publisher (orphan)", tid)
			}
		}
	}
}
