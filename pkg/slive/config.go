package slive

import (
	"log/slog"
	"time"

	"github.com/sajadbayatani/slive/internal/config"
	"github.com/sajadbayatani/slive/internal/webrtc"
)

// SDKConfig is the public configuration for a Slive client.
//
// It is the stable surface; config.Config (internal/config) remains the env
// wiring for cmd/slive. STUNServers, GCParticipantTTL, QueueSize and Logger
// are the only fields that SDK consumers need to set. Zero values are
// normalized to defaults by NewClient via DefaultSDKConfig.
type SDKConfig struct {
	// STUNServers is the list of STUN server URLs. Nil or empty uses the
	// signaling default (stun.l.google.com:19302 for production, or
	// STUN-free ICEServers: [] for tests).
	STUNServers []string
	// GCParticipantTTL is how long a ghost participant is kept after its
	// transport drops before it is reaped. Zero uses
	// config.DefaultGCParticipantTTL (60s). Negative disables GC.
	GCParticipantTTL time.Duration
	// QueueSize is the per-subscriber RTP queue capacity for
	// webrtc.ForwarderConfig. Zero or negative uses webrtc.DefaultQueueSize
	// (64). The value is normalized by NewClient and plumbed to
	// signaling.WithForwarderConfig so every TrackForwarder uses it.
	QueueSize int
	// Logger receives structured lifecycle events. Nil uses slog.Default().
	Logger *slog.Logger
	// AllowedOrigins is the allowlist for cross-origin WebSocket requests.
	// It is additive to the D1 defaults (no-Origin and same-origin allowed).
	// Origin values are matched exactly; e.g. "https://example.com".
	AllowedOrigins []string
}

// Config is an alias for SDKConfig for compatibility with older documentation
// that refers to Config. New code should prefer SDKConfig.
type Config = SDKConfig

// DefaultSDKConfig returns the default SDK configuration. It mirrors
// config.DefaultGCParticipantTTL and webrtc.DefaultQueueSize (64) so SDK
// consumers do not need to import internal packages to discover defaults.
func DefaultSDKConfig() SDKConfig {
	return SDKConfig{
		STUNServers:      nil,
		GCParticipantTTL: config.DefaultGCParticipantTTL,
		QueueSize:        webrtc.DefaultQueueSize,
		Logger:           nil,
	}
}
