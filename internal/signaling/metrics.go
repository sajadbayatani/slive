package signaling

import (
	"runtime"
	"sync/atomic"
	"time"

	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

// Snapshot collects a point-in-time copy of all metrics without holding
// handler locks during response write.
//
// Lock hierarchy (must be respected to avoid deadlocks):
//
//	Handler.trackForwardersMutex (RLock) -> TrackForwarder.mu (RLock) -> Room.mu (RLock via Participants()/Tracks())
//
// RoomManager.mu is acquired first to snapshot the room list, then released
// before acquiring Handler.trackForwardersMutex. Metrics collection never
// acquires Handler while holding TrackForwarder.mu (the reverse order), and
// all per-forwarder/per-room reads are done via RLock helpers that copy data
// before releasing. The returned MetricsSnapshot is cheap to copy and safe to
// encode without additional synchronization.
func (h *Handler) Snapshot() webrtc.MetricsSnapshot {
	snap := webrtc.MetricsSnapshot{
		ConnectionAttemptsTotal: webrtc.ConnectionMetrics.AttemptsTotal(),
		ConnectionFailuresTotal: webrtc.ConnectionMetrics.FailuresTotal(),
		GCReapedTotal:           atomic.LoadUint64(&h.gcReapedCount),
		UptimeSeconds:           int64(time.Since(webrtc.StartTime()).Seconds()),
		Goroutines:              runtime.NumGoroutine(),
	}

	// Forwarder aggregates: snapshot forwarder pointers under RLock then release.
	h.trackForwardersMutex.RLock()
	forwarders := make([]*webrtc.TrackForwarder, 0, len(h.trackForwarders))
	for _, fw := range h.trackForwarders {
		forwarders = append(forwarders, fw)
	}
	h.trackForwardersMutex.RUnlock()

	var totalSubs int
	var totalDropped uint64
	var maxDepth int
	for _, fw := range forwarders {
		totalSubs += fw.SubscriberCount()
		totalDropped += fw.TotalDropped()
		if d := fw.MaxQueueDepth(); d > maxDepth {
			maxDepth = d
		}
	}
	snap.ForwarderSubscribers = totalSubs
	snap.ForwarderDroppedTotal = totalDropped
	snap.ForwarderQueueDepth = maxDepth

	// Rooms / participants / tracks: acquire RoomManager RLock, copy room pointers, release,
	// then sample each room via its exported RLock-protected helpers.
	if h.roomManager != nil {
		// Collect room snapshots without holding Handler locks.
		ids := h.roomManager.RoomIDs()
		snap.RoomsActive = len(ids)
		var participants int
		var tracks int
		for _, id := range ids {
			if room := h.roomManager.GetRoom(id); room != nil {
				participants += len(room.Participants())
				tracks += len(room.Tracks())
			}
		}
		snap.ParticipantsActive = participants
		snap.TracksPublished = tracks
	}

	return snap
}

// Reset resets test-visible counters: connection metrics and GC reaped count,
// and per-forwarder dropped totals.
func (h *Handler) ResetMetrics() {
	webrtc.ConnectionMetrics.Reset()
	atomic.StoreUint64(&h.gcReapedCount, 0)
	h.trackForwardersMutex.RLock()
	for _, fw := range h.trackForwarders {
		fw.ResetDropped()
	}
	h.trackForwardersMutex.RUnlock()
}

// ResetGCReapedCount clears the ghost-reap counter. Intended for tests.
func (h *Handler) ResetGCReapedCount() {
	atomic.StoreUint64(&h.gcReapedCount, 0)
}
