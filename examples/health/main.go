// Command health polls the real /healthz diagnostics endpoint in-process:
// it serves slive.Client.HTTPHandler (the same router production uses:
// health + /healthz from internal/http plus the signaling WebSocket mount)
// on an httptest server and logs status, uptime_seconds and goroutines for
// 3 polls.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/sajadbayatani/slive/pkg/slive"
)

// healthBody mirrors the /healthz JSON response: a status plus the
// flattened MetricsSnapshot fields used by this example.
type healthBody struct {
	Status        string `json:"status"`
	UptimeSeconds int64  `json:"uptime_seconds"`
	Goroutines    int    `json:"goroutines"`
	RoomsActive   int    `json:"rooms_active"`
}

func main() {
	cfg := slive.DefaultSDKConfig()
	cfg.STUNServers = []string{} // STUN-free: offline, deterministic fixtures
	client, err := slive.NewClient(cfg)
	if err != nil {
		log.Fatal("new client:", err)
	}
	defer client.Close()

	room, err := client.JoinRoom(context.Background(), "room-003", "charlie")
	if err != nil {
		log.Fatal("join room:", err)
	}
	log.Printf("room joined: %s", room.ID())

	_, err = client.PublishTrack(context.Background(), room.ID(), "charlie", "track-003",
		slive.TrackKindAudio, slive.TrackSourceMicrophone)
	if err != nil {
		log.Fatal("publish track:", err)
	}
	log.Println("track published")

	// Serve the SDK-composed router: /health, /healthz and /ws all come from
	// the real internal/http handler stack wired to Client.Snapshot.
	server := httptest.NewServer(client.HTTPHandler())
	defer server.Close()

	for i := 1; i <= 3; i++ {
		time.Sleep(300 * time.Millisecond)
		body, err := httpGetHealthz(server.URL + "/healthz")
		if err != nil {
			log.Fatalf("health check %d failed: %v", i, err)
		}
		log.Printf("health check %d: status=%s uptime_seconds=%d goroutines=%d rooms_active=%d",
			i, body.Status, body.UptimeSeconds, body.Goroutines, body.RoomsActive)
		if body.Status != "ok" {
			log.Fatalf("health check %d: status=%q, want ok", i, body.Status)
		}
	}

	log.Println("health: exit 0")
}

func httpGetHealthz(url string) (healthBody, error) {
	var out healthBody
	resp, err := http.Get(url)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return out, err
	}
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("healthz returned %d", resp.StatusCode)
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, err
	}
	return out, nil
}
