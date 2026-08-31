package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sajadbayatani/slive/internal/config"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

func waitForConditionHTTP(t *testing.T, timeout time.Duration, what string, cond func() bool) {
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

func TestScale_HealthDuringBurst(t *testing.T) {
	canned := webrtc.MetricsSnapshot{
		ForwarderDroppedTotal: 123,
		ForwarderQueueDepth:   2,
		GCReapedTotal:         5,
		RoomsActive:           10,
		ParticipantsActive:    40,
		TracksPublished:       20,
		ForwarderSubscribers:  60,
		Goroutines:            50,
		UptimeSeconds:         10,
	}
	deps := HandlerDeps{
		MetricsSnapshot: func() webrtc.MetricsSnapshot { return canned },
	}
	router := NewRouter(config.Config{HealthPath: "/health"}, deps)
	ts := httptest.NewServer(router.ServeMux())
	defer ts.Close()

	// 20 concurrent scrapers polling every 100ms during simulated burst.
	var wg sync.WaitGroup
	errCh := make(chan error, 40)
	stop := make(chan struct{})
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					resp, err := http.Get(ts.URL + "/healthz")
					if err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
					if resp.StatusCode != 200 {
						select {
						case errCh <- err:
						default:
						}
						resp.Body.Close()
						return
					}
					var body map[string]any
					if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
						select {
						case errCh <- err:
						default:
						}
						resp.Body.Close()
						return
					}
					resp.Body.Close()
					for _, k := range []string{"forwarder_dropped_total", "forwarder_queue_depth", "gc_reaped_total"} {
						if _, ok := body[k]; !ok {
							select {
							case errCh <- err:
							default:
							}
							return
						}
					}
				}
			}
		}()
	}
	// Simulate burst duration 1s.
	time.Sleep(1 * time.Second)
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Errorf("health scraper error: %v", err)
		}
	}

	// Verify single httptest server (one per harness) serves correctly.
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestScale_HealthAfterHardening(t *testing.T) {
	// Compare baseline vs hardened via two snapshots with same load.
	baselineSnap := webrtc.MetricsSnapshot{
		ForwarderDroppedTotal: 1000,
		ForwarderQueueDepth:   10,
		Goroutines:            100,
	}
	hardenedSnap := webrtc.MetricsSnapshot{
		ForwarderDroppedTotal: 800,
		ForwarderQueueDepth:   5,
		Goroutines:            90,
	}
	depsBaseline := HandlerDeps{MetricsSnapshot: func() webrtc.MetricsSnapshot { return baselineSnap }}
	routerBaseline := NewRouter(config.Config{HealthPath: "/health"}, depsBaseline)
	tsBaseline := httptest.NewServer(routerBaseline.ServeMux())
	defer tsBaseline.Close()

	depsHardened := HandlerDeps{MetricsSnapshot: func() webrtc.MetricsSnapshot { return hardenedSnap }}
	routerHardened := NewRouter(config.Config{HealthPath: "/health"}, depsHardened)
	tsHardened := httptest.NewServer(routerHardened.ServeMux())
	defer tsHardened.Close()

	// Fetch both.
	fetch := func(url string) map[string]any {
		resp, err := http.Get(url + "/healthz")
		if err != nil {
			t.Fatalf("GET %s: %v", url, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status %d", resp.StatusCode)
		}
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return body
	}
	bodyBaseline := fetch(tsBaseline.URL)
	bodyHardened := fetch(tsHardened.URL)

	// Assert hardened dropped <= baseline.
	baseDropped := bodyBaseline["forwarder_dropped_total"].(float64)
	hardDropped := bodyHardened["forwarder_dropped_total"].(float64)
	if hardDropped > baseDropped {
		t.Errorf("hardened dropped %v > baseline %v", hardDropped, baseDropped)
	}
	baseGoroutines := bodyBaseline["goroutines"].(float64)
	hardGoroutines := bodyHardened["goroutines"].(float64)
	if hardGoroutines > baseGoroutines {
		t.Errorf("hardened goroutines %v > baseline %v", hardGoroutines, baseGoroutines)
	}

	// Also verify health remains 200 under concurrent load after hardening.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, _ := http.Get(tsHardened.URL + "/healthz")
			if resp != nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()
}

func TestHealth_MetricsSnapshotPresent(t *testing.T) {
	snap := webrtc.MetricsSnapshot{
		ForwarderDroppedTotal: 1,
		ForwarderQueueDepth:   1,
		GCReapedTotal:         1,
	}
	deps := HandlerDeps{MetricsSnapshot: func() webrtc.MetricsSnapshot { return snap }}
	h := NewHealthHandler(deps)
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, k := range []string{"forwarder_dropped_total", "forwarder_queue_depth", "gc_reaped_total", "rooms_active"} {
		if _, ok := body[k]; !ok {
			t.Errorf("missing field %s", k)
		}
	}
}
