package slive

import (
	"time"

	"github.com/sajadbayatani/slive/internal/signaling"
	"github.com/sajadbayatani/slive/internal/webrtc"
)

// NewRoomManager creates a new RoomManager. It is a stable wrapper for
// signaling.NewRoomManager.
func NewRoomManager() *RoomManager {
	return signaling.NewRoomManager()
}

// NewHandler creates a new Handler with the given RoomManager and options. It
// is a stable wrapper for signaling.NewHandler.
func NewHandler(rm *RoomManager, opts ...HandlerOption) *Handler {
	return signaling.NewHandler(rm, opts...)
}

// WithGCTTL sets the ghost-participant GC TTL on a Handler. A TTL of 0 disables
// GC. Default is 60s. Mirrors signaling.WithGCTTL.
func WithGCTTL(d time.Duration) HandlerOption {
	return signaling.WithGCTTL(d)
}

// WithMetricsSnapshot is a stable option for wiring a snapshot function as a
// health source. For Handler it is a no-op (health wiring belongs to the HTTP
// layer via WithMetricsSnapshot on the server); it exists so pkg/slive callers
// can depend on a frozen symbol without importing internal/http.
//
// Prefer passing Handler.Snapshot directly to your HTTP layer, or use
// Client.Snapshot when using the SDK Client.
func WithMetricsSnapshot(fn func() MetricsSnapshot) HandlerOption {
	return func(h *Handler) {}
}

// DiagnosticsSnapshoter provides a point-in-time metrics snapshot for
// diagnostics. Implementations must return a copy without holding handler locks.
type DiagnosticsSnapshoter interface {
	Snapshot() MetricsSnapshot
}

// WithForwarderConfig sets the ForwarderConfig used for every TrackForwarder
// created by the handler. It mirrors signaling.WithForwarderConfig.
func WithForwarderConfig(cfg ForwarderConfig) HandlerOption {
	return signaling.WithForwarderConfig(webrtc.ForwarderConfig(cfg))
}

// WithDiagnosticsSnapshoter wires a DiagnosticsSnapshoter as a health source.
// For Handler it is a no-op; it exists for API stability. See
// DiagnosticsSnapshoter and MetricsSnapshot for the response shape.
func WithDiagnosticsSnapshoter(s DiagnosticsSnapshoter) HandlerOption {
	return func(h *Handler) {}
}

// WithAllowedOrigins sets the allowlist for cross-origin WebSocket requests.
// It mirrors signaling.WithAllowedOrigins.
func WithAllowedOrigins(origins []string) HandlerOption {
	return signaling.WithAllowedOrigins(origins)
}

// WithWSReadTimeout sets the WebSocket read deadline on a Handler.
// It mirrors signaling.WithWSReadTimeout.
func WithWSReadTimeout(d time.Duration) HandlerOption {
	return signaling.WithWSReadTimeout(d)
}

// WithWSPingInterval sets the WebSocket ping interval on a Handler.
// It mirrors signaling.WithWSPingInterval.
func WithWSPingInterval(d time.Duration) HandlerOption {
	return signaling.WithWSPingInterval(d)
}

// WithWSWriteTimeout sets the WebSocket write deadline on a Handler.
// It mirrors signaling.WithWSWriteTimeout.
func WithWSWriteTimeout(d time.Duration) HandlerOption {
	return signaling.WithWSWriteTimeout(d)
}
