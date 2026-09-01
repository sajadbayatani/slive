package scale

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sajadbayatani/slive/internal/domain"
)

var updateBaseline = flag.Bool("update-baseline", false, "write reports/scale-baseline.md (default writes to TempDir and asserts keys only)")

func TestScaleCapacity(t *testing.T) {
	profile := DefaultScaleProfile()
	if testing.Short() {
		profile.Rooms = profile.RaceRooms
		profile.ParticipantsPerRoom = profile.RaceParticipantsPerRoom
		profile.PublishersPerRoom = profile.RaceParticipantsPerRoom / 2
		profile.SubscribersPerRoom = profile.RaceParticipantsPerRoom / 2
	}
	start := time.Now()
	h := NewScaleHarness(t)

	snapBefore := h.Snapshot()
	goroutinesBefore := runtime.NumGoroutine()

	healthDone := make(chan struct{})
	healthErrCh := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-healthDone:
				return
			case <-ticker.C:
				code, _, err := h.PollHealth()
				if err != nil {
					select {
					case healthErrCh <- fmt.Errorf("health poll error: %w", err):
					default:
					}
					return
				}
				if code != 200 {
					select {
					case healthErrCh <- fmt.Errorf("health status %d", code):
					default:
					}
					return
				}
			}
		}
	}()

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

	h.BurstRTP(profile.PacketsPerForwarder)

	time.Sleep(2 * time.Second)
	code, healthBody, err := h.PollHealth()
	if err != nil {
		t.Fatalf("PollHealth: %v", err)
	}
	if code != 200 {
		t.Fatalf("health status = %d, want 200", code)
	}

	close(healthDone)
	select {
	case he := <-healthErrCh:
		t.Fatalf("health poller failed: %v", he)
	default:
	}

	snapAfter := h.Snapshot()
	durationMs := time.Since(start).Milliseconds()
	goroutinesAfter := snapAfter.Goroutines
	if goroutinesAfter == 0 {
		goroutinesAfter = runtime.NumGoroutine()
	}

	allowance := goroutinesBefore + 10*profile.Rooms*profile.ParticipantsPerRoom
	if goroutinesAfter > allowance {
		t.Errorf("goroutines %d exceeds allowance %d (before %d, rooms %d participants %d)", goroutinesAfter, allowance, goroutinesBefore, profile.Rooms, profile.ParticipantsPerRoom)
	}

	if healthBody != nil {
		if v, ok := healthBody["forwarder_dropped_total"].(float64); ok {
			healthDropped := uint64(v)
			diff := int64(healthDropped) - int64(snapAfter.ForwarderDroppedTotal)
			if diff < 0 {
				diff = -diff
			}
			allowed := int64(float64(snapAfter.ForwarderDroppedTotal)*0.05) + 1
			if snapAfter.ForwarderDroppedTotal > 0 && diff > allowed {
				t.Errorf("health forwarder_dropped_total %d vs snapshot %d diff %d > allowed %d", healthDropped, snapAfter.ForwarderDroppedTotal, diff, allowed)
			}
		}
	}

	roomsCreated := profile.Rooms
	participantsJoined := profile.Rooms * profile.ParticipantsPerRoom
	tracksPublished := profile.Rooms * profile.PublishersPerRoom
	forwarderSubscribers := snapAfter.ForwarderSubscribers

	baseline := map[string]any{
		"rooms_created":           roomsCreated,
		"participants_joined":     participantsJoined,
		"tracks_published":        tracksPublished,
		"forwarder_subscribers":   forwarderSubscribers,
		"forwarder_dropped_total": snapAfter.ForwarderDroppedTotal,
		"goroutines":              goroutinesAfter,
		"gc_reaped_total":         snapAfter.GCReapedTotal,
		"rooms_active":            snapAfter.RoomsActive,
		"duration_ms":             durationMs,
		"uptime_seconds":          snapAfter.UptimeSeconds,
	}
	j, _ := json.Marshal(baseline)
	t.Logf("scale baseline: %s", string(j))

	for _, k := range []string{"rooms_created", "participants_joined", "tracks_published", "forwarder_subscribers", "forwarder_dropped_total", "goroutines", "gc_reaped_total", "rooms_active"} {
		if _, ok := baseline[k]; !ok {
			t.Errorf("baseline missing %s", k)
		}
	}

	if time.Since(start) > 60*time.Second {
		t.Errorf("TestScaleCapacity took %s, want <60s", time.Since(start))
	}

	_ = snapBefore
	// Baseline flag gate: default writes to TempDir and asserts keys only;
	// with -update-baseline writes to reports/scale-baseline.md.
	content := fmt.Sprintf("# Scale Baseline\n\nDate: %s\n\nProfile: %d rooms x %d participants (%d publishers, %d subs), %d subs/track, %d pkt/forwarder\n\n```json\n%s\n```\n\n- goroutines before: %d after: %d allowance: %d\n- forwarder_dropped_total: %d\n- gc_reaped_total: %d\n- rooms_active: %d\n- duration_ms: %d uptime_seconds: %d\n",
		time.Now().Format(time.RFC3339), profile.Rooms, profile.ParticipantsPerRoom, profile.PublishersPerRoom, profile.SubscribersPerRoom, profile.SubsPerTrack, profile.PacketsPerForwarder, string(j), goroutinesBefore, goroutinesAfter, allowance, snapAfter.ForwarderDroppedTotal, snapAfter.GCReapedTotal, snapAfter.RoomsActive, durationMs, snapAfter.UptimeSeconds)
	if *updateBaseline {
		reportsPath := filepath.Join("..", "..", "reports", "scale-baseline.md")
		if _, err := os.Stat(filepath.Dir(reportsPath)); err == nil {
			_ = os.WriteFile(reportsPath, []byte(content), 0644)
			t.Logf("wrote %s (update-baseline)", reportsPath)
		}
	} else {
		tmpPath := filepath.Join(t.TempDir(), "scale-baseline.md")
		_ = os.WriteFile(tmpPath, []byte(content), 0644)
		t.Logf("baseline written to temp %s (use -update-baseline to write reports/)", tmpPath)
	}
}
