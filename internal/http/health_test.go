package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/config"
	"github.com/sajadbayatani/slive/internal/domain"
	webrtcslive "github.com/sajadbayatani/slive/internal/webrtc"
)

func TestHealthEndpoint_JSON(t *testing.T) {
	canned := webrtcslive.MetricsSnapshot{
		ForwarderDroppedTotal:   5,
		GCReapedTotal:           1,
		RoomsActive:             2,
		ParticipantsActive:      4,
		TracksPublished:         1,
		ForwarderSubscribers:    3,
		ForwarderQueueDepth:     2,
		ConnectionAttemptsTotal: 10,
		ConnectionFailuresTotal: 1,
		Goroutines:              20,
		UptimeSeconds:           123,
	}
	deps := HandlerDeps{
		MetricsSnapshot: func() webrtcslive.MetricsSnapshot { return canned },
	}
	router := NewRouter(config.Config{HealthPath: "/health"}, deps)
	ts := httptest.NewServer(router.ServeMux())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status = %v, want ok", body["status"])
	}
	if got, ok := body["forwarder_dropped_total"]; !ok || int(got.(float64)) != 5 {
		t.Errorf("forwarder_dropped_total = %v, want 5", body["forwarder_dropped_total"])
	}
	if got, ok := body["gc_reaped_total"]; !ok || int(got.(float64)) != 1 {
		t.Errorf("gc_reaped_total = %v, want 1", body["gc_reaped_total"])
	}
	if got, ok := body["rooms_active"]; !ok || int(got.(float64)) != 2 {
		t.Errorf("rooms_active = %v, want 2", got)
	}
	if got, ok := body["participants_active"]; !ok || int(got.(float64)) != 4 {
		t.Errorf("participants_active = %v, want 4", got)
	}
	if got, ok := body["tracks_published"]; !ok || int(got.(float64)) != 1 {
		t.Errorf("tracks_published = %v, want 1", got)
	}
	resp2, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("/health status = %d, want 200", resp2.StatusCode)
	}
}

func TestHealthEndpoint_ConcurrentScrape(t *testing.T) {
	pub := newTestPublisherTrackHTTP(t, "http-conc", webrtc.MimeTypeOpus)
	fw, err := webrtcslive.NewTrackForwarderWithConfig(pub, webrtcslive.ForwarderConfig{QueueSize: 2})
	if err != nil {
		t.Fatalf("NewTrackForwarderWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = fw.Stop() })

	pc := newTestPeerConnectionHTTP(t, "http-sub", "Sub")
	if err := fw.AddSubscriber(pc); err != nil {
		t.Fatalf("AddSubscriber: %v", err)
	}

	var mu sync.Mutex
	var snapCount int
	deps := HandlerDeps{
		MetricsSnapshot: func() webrtcslive.MetricsSnapshot {
			mu.Lock()
			snapCount++
			mu.Unlock()
			return webrtcslive.MetricsSnapshot{
				ForwarderDroppedTotal: fw.TotalDropped(),
				ForwarderQueueDepth:   fw.MaxQueueDepth(),
				ForwarderSubscribers:  fw.SubscriberCount(),
				Goroutines:            10,
			}
		},
	}
	router := NewRouter(config.Config{HealthPath: "/health"}, deps)
	ts := httptest.NewServer(router.ServeMux())
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 1}, Payload: []byte{0x01}}
			for ctx.Err() == nil {
				for j := 0; j < 5; j++ {
					pkt.Header.SequenceNumber = uint16(j)
					_ = fw.WriteRTP(pkt)
				}
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	errCh := make(chan error, 20)
	var scrapeWg sync.WaitGroup
	for i := 0; i < 20; i++ {
		scrapeWg.Add(1)
		go func() {
			defer scrapeWg.Done()
			start := time.Now()
			resp, err := http.Get(ts.URL + "/healthz")
			if err != nil {
				errCh <- err
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errCh <- err
			}
			if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
				t.Errorf("scrape took %s, want <100ms", elapsed)
			}
		}()
	}
	scrapeWg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Errorf("scrape error: %v", err)
		}
	}
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("concurrent scrape + forwarder activity deadlocked")
	}
	_ = snapCount
}

func newTestPublisherTrackHTTP(t *testing.T, id, mime string) *webrtcslive.WebRTCTrack {
	t.Helper()
	kind := domain.TrackKindAudio
	source := domain.TrackSourceMicrophone
	if mime == webrtc.MimeTypeVP8 || mime == webrtc.MimeTypeVP9 || mime == webrtc.MimeTypeH264 {
		kind = domain.TrackKindVideo
		source = domain.TrackSourceCamera
	}
	dt, err := domain.NewTrack(id, kind, source)
	if err != nil {
		t.Fatalf("domain.NewTrack: %v", err)
	}
	cap := webrtc.RTPCodecCapability{MimeType: mime}
	if mime == webrtc.MimeTypeOpus {
		cap.ClockRate = 48000
		cap.Channels = 2
		cap.SDPFmtpLine = "minptime=10;useinbandfec=1"
	} else {
		cap.ClockRate = 90000
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(cap, id, id+"-stream")
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}
	codec := webrtc.RTPCodecParameters{RTPCodecCapability: cap, PayloadType: 111}
	return webrtcslive.NewWebRTCTrack(dt, pionTrack, codec)
}

func newTestPeerConnectionHTTP(t *testing.T, id, name string) *webrtcslive.PeerConnection {
	t.Helper()
	pc, err := webrtcslive.NewPeerConnection(webrtcslive.PeerConnectionConfig{SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback}, domain.NewParticipant(id, name), nil)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	if _, err := pc.PionPeerConnection().AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("AddTransceiverFromKind: %v", err)
	}
	return pc
}

func TestHealthEndpoint_TextPlain(t *testing.T) {
	canned := webrtcslive.MetricsSnapshot{ForwarderDroppedTotal: 7, GCReapedTotal: 2}
	deps := HandlerDeps{MetricsSnapshot: func() webrtcslive.MetricsSnapshot { return canned }}
	h := NewHealthHandler(deps)
	req := httptest.NewRequest("GET", "/healthz", nil)
	req.Header.Set("Accept", "text/plain")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}
	body := w.Body.String()
	if body == "" {
		t.Error("text plain body empty")
	}
}

func TestHealthEndpoint_Options(t *testing.T) {
	h := NewHealthHandler(HandlerDeps{})
	req := httptest.NewRequest("OPTIONS", "/healthz", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("OPTIONS status = %d, want 204", w.Code)
	}
}

func TestHealthEndpoint_Fallback(t *testing.T) {
	h := NewHealthHandler(HandlerDeps{})
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("fallback status = %d, want 200", w.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode fallback: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("fallback status field = %v, want ok", body["status"])
	}
}

type stubSnapshoter struct{ snap webrtcslive.MetricsSnapshot }

func (s stubSnapshoter) Snapshot() webrtcslive.MetricsSnapshot { return s.snap }

func TestHealthEndpoint_DiagnosticsSnapshoter(t *testing.T) {
	canned := webrtcslive.MetricsSnapshot{ForwarderDroppedTotal: 9}
	deps := HandlerDeps{DiagnosticsSnapshoter: stubSnapshoter{snap: canned}}
	h := NewHealthHandler(deps)
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if int(body["forwarder_dropped_total"].(float64)) != 9 {
		t.Errorf("forwarder_dropped_total = %v, want 9", body["forwarder_dropped_total"])
	}
}
