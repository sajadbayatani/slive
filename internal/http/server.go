package http

import (
	"context"
	"net/http"

	"github.com/sajadbayatani/slive/internal/config"
	"github.com/sajadbayatani/slive/internal/logger"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

// Server wraps an HTTP server with configuration and dependencies.
type Server struct {
	httpServer       *http.Server
	log              *logger.Logger
	router           *Router
	signalingHandler http.Handler
}

// Option customises the HTTP server at construction time.
type Option func(*options)

type options struct {
	signalingHandler      http.Handler
	diagnosticsSnapshoter DiagnosticsSnapshoter
	metricsSnapshot       func() webrtc.MetricsSnapshot
}

// WithSignalingHandler mounts an additional http.Handler — the WebSocket
// signaling endpoint — on the configured WebSocket path. The production
// wiring lives in cmd/slive, which builds the signaling.Handler with the
// runtime ICE-server configuration and structured logger.
func WithSignalingHandler(handler http.Handler) Option {
	return func(o *options) {
		if handler != nil {
			o.signalingHandler = handler
		}
	}
}

// WithDiagnosticsSnapshoter wires a diagnostics snapshot source for GET
// /healthz. The snapshot is called once per scrape outside any handler lock
// and encoded without holding Handler.trackForwardersMutex or Room.mu, so
// scrapers never block TrackForwarder.WriteRTP. See [DiagnosticsSnapshoter]
// and [webrtc.MetricsSnapshot] for the response shape.
func WithDiagnosticsSnapshoter(s DiagnosticsSnapshoter) Option {
	return func(o *options) {
		if s != nil {
			o.diagnosticsSnapshoter = s
		}
	}
}

// WithMetricsSnapshot wires a function snapshot source for GET /healthz.
// It is the functional alternative to [WithDiagnosticsSnapshoter]; if both
// are provided, the function takes precedence.
func WithMetricsSnapshot(fn func() webrtc.MetricsSnapshot) Option {
	return func(o *options) {
		if fn != nil {
			o.metricsSnapshot = fn
		}
	}
}

// NewServer creates a new HTTP server with the provided configuration and dependencies.
// It initializes the router with handler dependencies and sets up the HTTP server.
func NewServer(
	cfg config.Config,
	log *logger.Logger,
	opts ...Option,
) *Server {
	applied := &options{}
	for _, opt := range opts {
		if opt != nil {
			opt(applied)
		}
	}

	// Create handler dependencies
	deps := HandlerDeps{
		Log:                   log,
		SignalingHandler:      applied.signalingHandler,
		MetricsSnapshot:       applied.metricsSnapshot,
		DiagnosticsSnapshoter: applied.diagnosticsSnapshoter,
	}

	// Create and configure the router
	router := NewRouter(cfg, deps)

	return &Server{
		log:              log,
		router:           router,
		signalingHandler: applied.signalingHandler,
		httpServer: &http.Server{
			Addr:    cfg.HTTPAddr,
			Handler: router.ServeMux(),
		},
	}
}

func (s *Server) Start() error {
	err := s.httpServer.ListenAndServe()

	if err == http.ErrServerClosed {
		return nil
	}

	return err
}

// Shutdown gracefully stops the HTTP server: the listener closes and
// in-flight regular requests are drained.
//
// Before closing the HTTP listener, this method attempts to shut down the
// signaling handler (if mounted) so that active WebSocket sessions receive
// orderly close frames instead of being terminated with process exit.
func (s *Server) Shutdown(ctx context.Context) error {
	// Attempt to shut down the signaling handler if mounted, so that
	// active WebSocket sessions receive orderly close frames.
	if s.signalingHandler != nil {
		if sh, ok := s.signalingHandler.(interface{ Shutdown() error }); ok {
			if err := sh.Shutdown(); err != nil {
				s.log.Warn("signaling shutdown failed", "error", err)
			}
		}
	}

	return s.httpServer.Shutdown(ctx)
}
