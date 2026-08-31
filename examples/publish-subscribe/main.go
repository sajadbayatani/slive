// Command publish-subscribe exercises the SDK SFU path in-process: a
// publisher publishes an audio track and a subscriber subscribes through
// real WebSocket signaling sessions (slive.Client.Connect), so the
// signaling Handler registers the subscriber on the track's
// TrackForwarder and Client.Snapshot reports forwarder_subscribers >= 1.
//
// The sessions use a STUN-free ICE config, so no external network is
// touched and the run is deterministic. Media note: the SDK surface from
// TASK-031 does not expose the Handler's TrackForwarder, so a synthetic
// WriteRTP burst cannot run from outside internal/*; the forwarder here is
// backed by a placeholder local track and its dropped counter stays at 0,
// which the example verifies is monotonic across snapshots.
package main

import (
	"context"
	"log"
	"time"

	"github.com/sajadbayatani/slive/pkg/slive"
)

func main() {
	cfg := slive.DefaultSDKConfig()
	cfg.STUNServers = []string{} // STUN-free: offline, deterministic ICE
	client, err := slive.NewClient(cfg)
	if err != nil {
		log.Fatal("new client:", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	const (
		roomID     = "room-002"
		trackID    = "track-001"
		publisher  = "publisher"
		subscriber = "subscriber"
	)

	// Publisher: connect a signaling session (auto-joins room-002) and
	// publish the audio track. The Handler creates the TrackForwarder.
	pub, err := client.Connect(ctx, roomID, publisher)
	if err != nil {
		log.Fatal("publisher connect:", err)
	}
	defer pub.Close()
	if err := pub.PublishTrack(ctx, trackID, slive.TrackKindAudio, slive.TrackSourceMicrophone); err != nil {
		log.Fatal("publish track:", err)
	}
	log.Printf("room joined: %s", roomID)
	log.Println("track published")

	// Subscriber: connect its own session and subscribe through signaling.
	// On success the subscriber's peer connection is registered on the
	// forwarder — no sleep needed, the response follows AddSubscriber.
	sub, err := client.Connect(ctx, roomID, subscriber)
	if err != nil {
		log.Fatal("subscriber connect:", err)
	}
	defer sub.Close()
	droppedBefore := client.Snapshot().ForwarderDroppedTotal
	if err := sub.SubscribeTrack(ctx, trackID); err != nil {
		log.Fatal("subscribe track:", err)
	}
	log.Println("subscriber joined room")
	log.Println("track subscribed")

	snapshot := waitForSubscribers(ctx, client, 1)
	log.Printf("forwarder_subscribers: %d", snapshot.ForwarderSubscribers)
	log.Printf("forwarder_dropped_total: %d", snapshot.ForwarderDroppedTotal)

	if snapshot.ForwarderSubscribers < 1 {
		log.Fatalf("acceptance failed: forwarder_subscribers=%d, want >=1", snapshot.ForwarderSubscribers)
	}
	if snapshot.ForwarderDroppedTotal < droppedBefore {
		log.Fatalf("acceptance failed: forwarder_dropped_total decreased %d -> %d",
			droppedBefore, snapshot.ForwarderDroppedTotal)
	}
	log.Printf("forwarder_dropped_total monotonic: %d -> %d", droppedBefore, snapshot.ForwarderDroppedTotal)
	log.Printf("rooms_active: %d participants_active: %d tracks_published: %d",
		snapshot.RoomsActive, snapshot.ParticipantsActive, snapshot.TracksPublished)
	log.Println("publish-subscribe: exit 0")
}

// waitForSubscribers polls Snapshot until at least want forwarder
// subscribers are registered or the deadline passes; the last snapshot is
// returned either way (the caller asserts on it).
func waitForSubscribers(ctx context.Context, client *slive.Client, want int) slive.MetricsSnapshot {
	deadline := time.Now().Add(2 * time.Second)
	var snapshot slive.MetricsSnapshot
	for {
		snapshot = client.Snapshot()
		if snapshot.ForwarderSubscribers >= want || time.Now().After(deadline) {
			return snapshot
		}
		select {
		case <-ctx.Done():
			return snapshot
		case <-time.After(20 * time.Millisecond):
		}
	}
}
