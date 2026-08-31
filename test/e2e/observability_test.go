package e2e

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pion/rtp"
	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/config"
	"github.com/sajadbayatani/slive/internal/domain"
	apphttp "github.com/sajadbayatani/slive/internal/http"
	"github.com/sajadbayatani/slive/internal/signaling"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

func waitForConditionObs(t *testing.T, timeout time.Duration, what string, cond func() bool) {
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

func fetchHealth(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	return body
}

func TestE2E_Observability(t *testing.T) {
	handler := signaling.NewHandler(signaling.NewRoomManager(),
		signaling.WithPeerConnectionConfig(webrtc.PeerConnectionConfig{SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback}),
		signaling.WithGCTTL(200*time.Millisecond),
	)
	// External forwarder for drop test with QueueSize 2.
	dt, err := domain.NewTrack("ext-drop-track", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("domain.NewTrack: %v", err)
	}
	cap := pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}
	pionTrack, err := pionwebrtc.NewTrackLocalStaticRTP(cap, "ext-drop-track", "ext-drop-track-stream")
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}
	codec := pionwebrtc.RTPCodecParameters{RTPCodecCapability: cap, PayloadType: 111}
	wrapped := webrtc.NewWebRTCTrack(dt, pionTrack, codec)
	extFw, err := webrtc.NewTrackForwarderWithConfig(wrapped, webrtc.ForwarderConfig{QueueSize: 2})
	if err != nil {
		t.Fatalf("NewTrackForwarderWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = extFw.Stop() })
	// Add a dummy subscriber so queue exists.
	dummyPC, err := webrtc.NewPeerConnection(webrtc.PeerConnectionConfig{SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback}, domain.NewParticipant("dummy-sub", "Dummy"), nil)
	if err != nil {
		t.Fatalf("NewPeerConnection dummy: %v", err)
	}
	t.Cleanup(func() { _ = dummyPC.Close() })
	if _, err := dummyPC.PionPeerConnection().AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("AddTransceiverFromKind dummy: %v", err)
	}
	if err := extFw.AddSubscriber(dummyPC); err != nil {
		t.Fatalf("AddSubscriber dummy: %v", err)
	}

	snapshot := func() webrtc.MetricsSnapshot {
		s := handler.Snapshot()
		s.ForwarderDroppedTotal += extFw.TotalDropped()
		if d := extFw.MaxQueueDepth(); d > s.ForwarderQueueDepth {
			s.ForwarderQueueDepth = d
		}
		s.ForwarderSubscribers += extFw.SubscriberCount()
		return s
	}

	router := apphttp.NewRouter(config.Config{HealthPath: "/health", WebSocketPath: "/ws"}, apphttp.HandlerDeps{
		SignalingHandler: handler,
		MetricsSnapshot:  snapshot,
	})
	ts := httptest.NewServer(router.ServeMux())
	t.Cleanup(ts.Close)
	t.Cleanup(func() { _ = handler.Shutdown() })

	alice := dialWS(t, ts, "e2e-obs-room", "p-alice")
	alice.receiveOfType(signaling.MessageTypeRoomJoined, "alice room_joined")
	bob := dialWS(t, ts, "e2e-obs-room", "p-bob")
	bob.receiveOfType(signaling.MessageTypeRoomJoined, "bob room_joined")
	// Wait for alice to see bob.
	alice.receiveOfType(signaling.MessageTypeParticipantJoined, "alice participant_joined")

	alice.send(signaling.MessageTypePublishTrack, signaling.PublishTrackRequest{
		RoomID:        "e2e-obs-room",
		ParticipantID: "p-alice",
		Track:         signaling.TrackInfo{ID: "e2e-track-1", Kind: "audio", Source: "microphone"},
	})
	alice.receiveOfType(signaling.MessageTypeTrackPublished, "alice track_published")
	bob.receiveOfType(signaling.MessageTypeTrackAvailable, "bob track_available")

	bob.send(signaling.MessageTypeSubscribeTrack, signaling.SubscribeTrackRequest{
		RoomID:        "e2e-obs-room",
		ParticipantID: "p-alice",
		TrackID:       "e2e-track-1",
	})
	bob.receiveOfType(signaling.MessageTypeTrackSubscribed, "bob track_subscribed")

	// Initial health.
	initial := fetchHealth(t, ts.URL)
	initGC := int(initial["gc_reaped_total"].(float64))
	initDropped := int(initial["forwarder_dropped_total"].(float64))

	// Burst WriteRTP on external forwarder to generate drops.
	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 1}, Payload: []byte{0x01, 0x02}}
	for attempt := 0; attempt < 5; attempt++ {
		for i := 0; i < 100; i++ {
			pkt.Header.SequenceNumber = uint16(i + attempt*100)
			_ = extFw.WriteRTP(pkt)
		}
		if extFw.TotalDropped() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Poll health for forwarder_dropped_total increment.
	waitForConditionObs(t, 800*time.Millisecond, "forwarder_dropped_total increment", func() bool {
		body := fetchHealth(t, ts.URL)
		v, ok := body["forwarder_dropped_total"].(float64)
		if !ok {
			return false
		}
		return int(v) > initDropped
	})
	afterDrop := fetchHealth(t, ts.URL)
	if got := int(afterDrop["forwarder_dropped_total"].(float64)); got <= initDropped {
		t.Errorf("forwarder_dropped_total = %d, want > %d", got, initDropped)
	}

	// Ghost reap: close bob's transport without explicit leave.
	_ = bob.conn.Close()
	// Poll health for gc_reaped_total increment with TTL+500ms = 700ms deadline.
	waitForConditionObs(t, 800*time.Millisecond, "gc_reaped_total increment", func() bool {
		body := fetchHealth(t, ts.URL)
		v, ok := body["gc_reaped_total"].(float64)
		if !ok {
			return false
		}
		return int(v) > initGC
	})
	afterGC := fetchHealth(t, ts.URL)
	if got := int(afterGC["gc_reaped_total"].(float64)); got <= initGC {
		t.Errorf("gc_reaped_total = %d, want > %d", got, initGC)
	}
	// Also verify health still 200 JSON with status ok.
	if afterGC["status"] != "ok" {
		t.Errorf("health status = %v, want ok", afterGC["status"])
	}
}
