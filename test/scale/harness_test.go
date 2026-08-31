package scale

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
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

// ScaleProfile defines deterministic load parameters.
type ScaleProfile struct {
	Rooms                   int
	ParticipantsPerRoom     int
	PublishersPerRoom       int
	SubscribersPerRoom      int
	SubsPerTrack            int
	PacketsPerForwarder     int
	Duration                time.Duration
	RaceRooms               int
	RaceParticipantsPerRoom int
}

// DefaultScaleProfile returns the 100x16 profile from TASK-028.
func DefaultScaleProfile() ScaleProfile {
	return ScaleProfile{
		Rooms:                   100,
		ParticipantsPerRoom:     16,
		PublishersPerRoom:       8,
		SubscribersPerRoom:      8,
		SubsPerTrack:            3,
		PacketsPerForwarder:     1800, // 60 pkt/s * 30s
		Duration:                30 * time.Second,
		RaceRooms:               50,
		RaceParticipantsPerRoom: 8,
	}
}

// EffectiveProfile returns RaceRooms profile when running under race; by default
// it returns the full profile. Callers may override manually.
func (p ScaleProfile) Effective() ScaleProfile { return p }

// ScaleHarness drives RoomManager, Handler and TrackForwarder with STUN-free fixtures.
type ScaleHarness struct {
	t              *testing.T
	RoomManager    *signaling.RoomManager
	Handler        *signaling.Handler
	HealthServer   *httptest.Server
	HealthURL      string
	startTime      time.Time
	mu             sync.Mutex
	rooms          map[string]*domain.Room
	participants   map[string]*domain.Participant
	pcs            map[string]*webrtc.PeerConnection
	forwarders     map[string]*webrtc.TrackForwarder
	forwarderOrder []string
	trackIDs       []string
}

// NewScaleHarness creates RoomManager + Handler STUN-free with WithGCTTL(60s)
// and ForwarderConfig{QueueSize:64} plus MetricsSnapshot accessor and health server.
func NewScaleHarness(t *testing.T) *ScaleHarness {
	t.Helper()
	rm := signaling.NewRoomManager()
	handler := signaling.NewHandler(rm,
		signaling.WithPeerConnectionConfig(webrtc.PeerConnectionConfig{
			ICEServers:   []pionwebrtc.ICEServer{},
			SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
		}),
		signaling.WithGCTTL(60*time.Second),
	)
	h := &ScaleHarness{
		t:            t,
		RoomManager:  rm,
		Handler:      handler,
		rooms:        make(map[string]*domain.Room),
		participants: make(map[string]*domain.Participant),
		pcs:          make(map[string]*webrtc.PeerConnection),
		forwarders:   make(map[string]*webrtc.TrackForwarder),
		startTime:    time.Now(),
	}
	// Snapshot accessor merges handler snapshot with external forwarder aggregates.
	snapshotFn := func() webrtc.MetricsSnapshot {
		snap := handler.Snapshot()
		h.mu.Lock()
		var totalSubs int
		var totalDropped uint64
		var maxDepth int
		for _, fw := range h.forwarders {
			totalSubs += fw.SubscriberCount()
			totalDropped += fw.TotalDropped()
			if d := fw.MaxQueueDepth(); d > maxDepth {
				maxDepth = d
			}
		}
		h.mu.Unlock()
		// Add external forwarder contributions (handler's own forwarders are zero in this harness).
		snap.ForwarderSubscribers += totalSubs
		snap.ForwarderDroppedTotal += totalDropped
		if maxDepth > snap.ForwarderQueueDepth {
			snap.ForwarderQueueDepth = maxDepth
		}
		return snap
	}
	router := apphttp.NewRouter(config.Config{
		HealthPath:    "/health",
		WebSocketPath: "/ws",
	}, apphttp.HandlerDeps{
		SignalingHandler: handler,
		MetricsSnapshot:  snapshotFn,
	})
	ts := httptest.NewServer(router.ServeMux())
	h.HealthServer = ts
	h.HealthURL = ts.URL
	t.Cleanup(func() {
		ts.Close()
		_ = handler.Shutdown()
		for _, pc := range h.pcs {
			_ = pc.Close()
		}
		for _, fw := range h.forwarders {
			_ = fw.Stop()
		}
	})
	return h
}

// Snapshot returns the merged MetricsSnapshot.
func (h *ScaleHarness) Snapshot() webrtc.MetricsSnapshot {
	snap := h.Handler.Snapshot()
	h.mu.Lock()
	defer h.mu.Unlock()
	var totalSubs int
	var totalDropped uint64
	var maxDepth int
	for _, fw := range h.forwarders {
		totalSubs += fw.SubscriberCount()
		totalDropped += fw.TotalDropped()
		if d := fw.MaxQueueDepth(); d > maxDepth {
			maxDepth = d
		}
	}
	snap.ForwarderSubscribers += totalSubs
	snap.ForwarderDroppedTotal += totalDropped
	if maxDepth > snap.ForwarderQueueDepth {
		snap.ForwarderQueueDepth = maxDepth
	}
	return snap
}

// PollHealth performs GET /healthz via httptest.NewServer.
func (h *ScaleHarness) PollHealth() (int, map[string]any, error) {
	resp, err := http.Get(h.HealthURL + "/healthz")
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, m, nil
}

// CreateRooms creates n rooms with IDs room-%03d.
func (h *ScaleHarness) CreateRooms(n int) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		roomID := fmt.Sprintf("room-%03d", i)
		room, err := h.RoomManager.GetOrCreateRoom(roomID)
		if err != nil {
			h.t.Fatalf("CreateRooms GetOrCreateRoom %s: %v", roomID, err)
		}
		h.mu.Lock()
		h.rooms[roomID] = room
		h.mu.Unlock()
	}
}

// JoinParticipants joins m participants into roomID with IDs participant-%03d-%03d and creates STUN-free peer connections.
func (h *ScaleHarness) JoinParticipants(roomID string, m int) {
	h.t.Helper()
	h.mu.Lock()
	room := h.rooms[roomID]
	h.mu.Unlock()
	if room == nil {
		h.t.Fatalf("JoinParticipants room %s not found", roomID)
	}
	// Derive room index from ID for deterministic naming.
	roomIdx := 0
	_, _ = fmt.Sscanf(roomID, "room-%03d", &roomIdx)
	for j := 0; j < m; j++ {
		pid := fmt.Sprintf("participant-%03d-%03d", roomIdx, j)
		p := domain.NewParticipant(pid, "User "+pid)
		if err := room.Join(p); err != nil {
			h.t.Fatalf("room.Join %s %s: %v", roomID, pid, err)
		}
		p.SetRoom(room)
		pc, err := webrtc.NewPeerConnection(webrtc.PeerConnectionConfig{
			ICEServers:   []pionwebrtc.ICEServer{},
			SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
		}, p, nil)
		if err != nil {
			h.t.Fatalf("NewPeerConnection %s: %v", pid, err)
		}
		// Add a transceiver so codec negotiation is valid for track forwarder.
		if _, err := pc.PionPeerConnection().AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
			h.t.Fatalf("AddTransceiver %s: %v", pid, err)
		}
		h.mu.Lock()
		h.participants[pid] = p
		h.pcs[pid] = pc
		h.mu.Unlock()
	}
}

// PublishTracks creates 1 audio track per publisher and a forwarder with QueueSize 64.
func (h *ScaleHarness) PublishTracks(publishers []*domain.Participant) {
	h.t.Helper()
	for _, pub := range publishers {
		pid := pub.ID()
		trackID := fmt.Sprintf("track-%s", pid)
		tr, err := domain.NewTrack(trackID, domain.TrackKindAudio, domain.TrackSourceMicrophone)
		if err != nil {
			h.t.Fatalf("NewTrack %s: %v", trackID, err)
		}
		if err := pub.PublishTrack(tr); err != nil {
			h.t.Fatalf("PublishTrack %s: %v", trackID, err)
		}
		// Register in room registry.
		if room := pub.Room(); room != nil {
			_ = room.PublishTrack(tr)
		}
		// Create standalone WebRTC track for forwarder.
		cap := pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}
		pionTrack, err := pionwebrtc.NewTrackLocalStaticRTP(cap, trackID, trackID+"-stream")
		if err != nil {
			h.t.Fatalf("NewTrackLocalStaticRTP %s: %v", trackID, err)
		}
		codec := pionwebrtc.RTPCodecParameters{RTPCodecCapability: cap, PayloadType: 111}
		wrapped := webrtc.NewWebRTCTrack(tr, pionTrack, codec)
		fw, err := webrtc.NewTrackForwarderWithConfig(wrapped, webrtc.ForwarderConfig{QueueSize: 64})
		if err != nil {
			h.t.Fatalf("NewTrackForwarder %s: %v", trackID, err)
		}
		if err := fw.Start(); err != nil {
			h.t.Fatalf("Forwarder Start %s: %v", trackID, err)
		}
		h.mu.Lock()
		h.forwarders[trackID] = fw
		h.forwarderOrder = append(h.forwarderOrder, trackID)
		h.trackIDs = append(h.trackIDs, trackID)
		h.mu.Unlock()
	}
}

// SubscribeTracks subscribes each subscriber to the given trackID (3 subs per track in profile).
func (h *ScaleHarness) SubscribeTracks(trackID string, subscribers []*domain.Participant) {
	h.t.Helper()
	h.mu.Lock()
	fw := h.forwarders[trackID]
	h.mu.Unlock()
	if fw == nil {
		h.t.Fatalf("SubscribeTracks forwarder %s not found", trackID)
	}
	// Domain subscription first.
	var domTrack *domain.Track
	for _, sub := range subscribers {
		if len(sub.ID()) > 0 {
			if t := sub.Room(); t != nil {
				if tr := t.GetTrack(trackID); tr != nil {
					domTrack = tr
					break
				}
			}
		}
	}
	// Fallback: find via any participant's published tracks.
	if domTrack == nil {
		h.mu.Lock()
		for _, p := range h.participants {
			if tr := p.GetPublishedTrack(trackID); tr != nil {
				domTrack = tr
				break
			}
		}
		h.mu.Unlock()
	}
	for _, sub := range subscribers {
		if domTrack != nil {
			_ = sub.SubscribeTrack(domTrack)
		}
		pc := h.pcs[sub.ID()]
		if pc == nil {
			h.t.Fatalf("pc for %s not found", sub.ID())
		}
		if err := fw.AddSubscriber(pc); err != nil {
			h.t.Fatalf("AddSubscriber %s -> %s: %v", sub.ID(), trackID, err)
		}
	}
}

// BurstRTP sends numPackets cloned RTP packets via TrackForwarder.WriteRTP for each forwarder.
func (h *ScaleHarness) BurstRTP(numPackets int) {
	h.mu.Lock()
	fws := make([]*webrtc.TrackForwarder, 0, len(h.forwarders))
	for _, id := range h.forwarderOrder {
		fws = append(fws, h.forwarders[id])
	}
	h.mu.Unlock()
	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 12345}, Payload: []byte{0x01, 0x02, 0x03, 0x04}}
	for _, fw := range fws {
		for i := 0; i < numPackets; i++ {
			pkt.Header.SequenceNumber = uint16(i)
			pkt.Header.Timestamp = uint32(i * 960)
			_ = fw.WriteRTP(pkt)
		}
	}
}

// Uptime returns seconds since harness creation.
func (h *ScaleHarness) Uptime() int64 {
	return int64(time.Since(h.startTime).Seconds())
}

// Goroutines returns current runtime goroutine count.
func (h *ScaleHarness) Goroutines() int { return runtime.NumGoroutine() }
