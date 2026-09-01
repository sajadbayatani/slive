//go:build slive_internal

package scale

import (
	"fmt"
	"testing"
	"time"

	"github.com/pion/rtp"
	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
	"github.com/sajadbayatani/slive/internal/signaling"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

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
	for i := 0; i < 10; i++ {
		handler.ArmGhostForTest(ghostRoomIDs[i], ghostPIDs[i])
	}
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
