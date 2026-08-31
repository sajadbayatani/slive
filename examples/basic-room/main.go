package main

import (
	"context"
	"log"
	"time"

	"github.com/sajadbayatani/slive/pkg/slive"
)

func main() {
	cfg := slive.DefaultSDKConfig()
	cfg.STUNServers = []string{} // STUN-free: offline, deterministic fixtures
	client, err := slive.NewClient(cfg)
	if err != nil {
		log.Fatal("new client:", err)
	}
	defer client.Close()

	room, err := client.JoinRoom(context.Background(), "room-001", "alice")
	if err != nil {
		log.Fatal("join room:", err)
	}
	log.Printf("room joined: %s", room.ID())

	if _, err := client.JoinRoom(context.Background(), "room-001", "bob"); err != nil {
		log.Fatal("join room:", err)
	}
	log.Println("participant bob joined")

	snapshot := client.Snapshot()
	log.Printf("rooms_active: %d", snapshot.RoomsActive)
	log.Printf("participants_active: %d", snapshot.ParticipantsActive)

	time.Sleep(100 * time.Millisecond)
	log.Println("basic-room: exit 0")
}
