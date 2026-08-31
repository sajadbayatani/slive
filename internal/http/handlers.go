package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"

	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

// DiagnosticsSnapshoter provides a point-in-time metrics snapshot for diagnostics.
//
// Implementations must return a copy without holding handler locks during
// encoding, allowing the health handler to write the response without blocking
// forwarders. The returned [webrtc.MetricsSnapshot] is safe to encode without
// additional synchronization.
//
// Lock safety: Snapshot must copy all gauges/counters under RLock then release
// before returning, so callers never hold [Handler.trackForwardersMutex] or
// [domain.Room.mu] while writing the HTTP response.
type DiagnosticsSnapshoter interface {
	Snapshot() webrtc.MetricsSnapshot
}

// MetricsSnapshot is the snapshot function type for diagnostics. It mirrors
// [DiagnosticsSnapshoter.Snapshot] but allows wiring a plain function closure
// (e.g. handler.Snapshot) without an interface wrapper.
type MetricsSnapshot func() webrtc.MetricsSnapshot

// Logger is the handler logging contract. Tests can supply a lightweight fake
// rather than constructing the application's concrete logger.
type Logger interface {
	Info(msg string, args ...any)
}

// HandlerDeps contains dependencies required by HTTP handlers.
// This struct enables dependency injection for handlers.
type HandlerDeps struct {
	Log Logger
	// SignalingHandler, when set, is mounted on the configured WebSocket
	// path and upgrades clients to the signaling protocol. Injected rather
	// than imported so the HTTP layer stays decoupled from the signaling
	// package; deployments without signaling leave it nil and simply get
	// no WebSocket route.
	SignalingHandler http.Handler
	// MetricsSnapshot, when set, is called once per health request outside
	// any handler lock to obtain a point-in-time metrics copy. The snapshot
	// is then encoded without holding Handler.trackForwardersMutex or Room.mu.
	MetricsSnapshot func() webrtc.MetricsSnapshot
	// DiagnosticsSnapshoter is an alternative to MetricsSnapshot. If both are
	// set, MetricsSnapshot takes precedence. Implementations must return a
	// copy without holding locks during the subsequent ResponseWriter write.
	DiagnosticsSnapshoter DiagnosticsSnapshoter
}

// HealthHandler handles requests to the /health and /healthz endpoints.
//
// It exposes in-process resource signals (goroutines, uptime) and Phase 7
// metrics (rooms, participants, tracks, forwarder gauges, connection
// counters, GC reaps) via a safe, read-only snapshot. The snapshot is
// acquired once outside any handler lock and then encoded without holding
// Handler.trackForwardersMutex or Room.mu, so concurrent scrapers never block
// TrackForwarder.WriteRTP or WebSocket publish/subscribe/GC.
//
// Response shape (JSON, primary):
//
//	{"status":"ok","uptime_seconds":123,"rooms_active":1,"participants_active":2,
//	 "tracks_published":1,"forwarder_subscribers":2,"forwarder_dropped_total":5,
//	 "forwarder_queue_depth":3,"connection_attempts_total":10,
//	 "connection_failures_total":1,"gc_reaped_total":0,"goroutines":42}
//
// When Accept: text/plain is requested, a simple Prometheus-compatible text
// exposition (metric_name value lines) is returned instead.
type HealthHandler struct {
	deps HandlerDeps
}

// NewHealthHandler creates a new HealthHandler with the provided dependencies.
func NewHealthHandler(deps HandlerDeps) *HealthHandler {
	return &HealthHandler{deps: deps}
}

// healthResponse is the JSON envelope for diagnostics. MetricsSnapshot is
// embedded so its fields are flattened alongside status.
type healthResponse struct {
	Status string `json:"status"`
	webrtc.MetricsSnapshot
}

// ServeHTTP implements http.Handler for the health endpoint.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.deps.Log != nil {
		h.deps.Log.Info("Health check requested", "method", r.Method, "path", r.URL.Path)
	}

	// CORS for smeeting phone testing (different port = cross-origin)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Acquire snapshot once outside any handler lock. Never hold
	// Handler.trackForwardersMutex or Room.mu while writing.
	var snap webrtc.MetricsSnapshot
	switch {
	case h.deps.MetricsSnapshot != nil:
		snap = h.deps.MetricsSnapshot()
	case h.deps.DiagnosticsSnapshoter != nil:
		snap = h.deps.DiagnosticsSnapshoter.Snapshot()
	default:
		// Fallback: minimal resource signals without forwarder/room gauges.
		snap = webrtc.MetricsSnapshot{
			UptimeSeconds: int64(time.Since(webrtc.StartTime()).Seconds()),
			Goroutines:    runtime.NumGoroutine(),
		}
	}

	// Optional text exposition when explicitly requested.
	if strings.Contains(r.Header.Get("Accept"), "text/plain") {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		// No locks held during write.
		_, _ = fmt.Fprintf(w, "status %s\n", "ok")
		_, _ = fmt.Fprintf(w, "uptime_seconds %d\n", snap.UptimeSeconds)
		_, _ = fmt.Fprintf(w, "rooms_active %d\n", snap.RoomsActive)
		_, _ = fmt.Fprintf(w, "participants_active %d\n", snap.ParticipantsActive)
		_, _ = fmt.Fprintf(w, "tracks_published %d\n", snap.TracksPublished)
		_, _ = fmt.Fprintf(w, "forwarder_subscribers %d\n", snap.ForwarderSubscribers)
		_, _ = fmt.Fprintf(w, "forwarder_dropped_total %d\n", snap.ForwarderDroppedTotal)
		_, _ = fmt.Fprintf(w, "forwarder_queue_depth %d\n", snap.ForwarderQueueDepth)
		_, _ = fmt.Fprintf(w, "connection_attempts_total %d\n", snap.ConnectionAttemptsTotal)
		_, _ = fmt.Fprintf(w, "connection_failures_total %d\n", snap.ConnectionFailuresTotal)
		_, _ = fmt.Fprintf(w, "gc_reaped_total %d\n", snap.GCReapedTotal)
		_, _ = fmt.Fprintf(w, "goroutines %d\n", snap.Goroutines)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	resp := healthResponse{
		Status:          "ok",
		MetricsSnapshot: snap,
	}
	// Encode directly to ResponseWriter without holding locks.
	_ = json.NewEncoder(w).Encode(resp)
}
