package http

import (
	"net/http"

	"github.com/sajadbayatani/slive/internal/config"
	"github.com/sajadbayatani/slive/internal/logger"
)

// Router handles the registration of all HTTP routes.
// It separates route registration logic from server setup.
type Router struct {
	mux  *http.ServeMux
	deps HandlerDeps
}

// NewRouter creates a new Router with the provided dependencies.
// It registers all application routes.
func NewRouter(cfg config.Config, deps HandlerDeps) *Router {
	mux := http.NewServeMux()

	router := &Router{
		mux:  mux,
		deps: deps,
	}

	router.registerRoutes(cfg)

	return router
}

// registerRoutes registers all application routes.
// This method is called during router initialization.
func (r *Router) registerRoutes(cfg config.Config) {
	// The health path comes from runtime configuration rather than being
	// hardcoded in the HTTP layer. Both /health (legacy, TASK-014) and
	// /healthz (diagnostics, TASK-026) are always registered alongside any
	// custom HealthPath so operators can rely on /healthz for scraping while
	// existing probes on /health keep working. The snapshot is acquired
	// outside any handler lock and encoded without holding
	// Handler.trackForwardersMutex or Room.mu.
	healthHandler := NewHealthHandler(r.deps)
	healthPath := cfg.HealthPath
	if healthPath == "" {
		healthPath = config.DefaultHealthPath
	}
	// Deduplicate registrations to avoid ServeMux panic on duplicate patterns.
	healthPaths := map[string]struct{}{
		healthPath: {},
		"/health":  {},
		"/healthz": {},
	}
	for p := range healthPaths {
		r.mux.Handle(p, healthHandler)
	}

	// The WebSocket signaling endpoint is injected via HandlerDeps (see
	// cmd/slive for the production wiring). The path comes from runtime
	// configuration; the /health contract is unaffected either way.
	if r.deps.SignalingHandler != nil {
		wsPath := cfg.WebSocketPath
		if wsPath == "" {
			wsPath = config.DefaultWebSocketPath
		}
		r.mux.Handle(wsPath, r.deps.SignalingHandler)
	}
}

// ServeMux returns the underlying http.ServeMux for use with the HTTP server.
func (r *Router) ServeMux() *http.ServeMux {
	return r.mux
}

// HandlerDepsProvider is an interface for providing handler dependencies.
// This enables easier testing and mocking.
type HandlerDepsProvider interface {
	GetHandlerDeps() HandlerDeps
}

// RealHandlerDepsProvider provides real handler dependencies.
type RealHandlerDepsProvider struct {
	log *logger.Logger
}

// NewRealHandlerDepsProvider creates a new RealHandlerDepsProvider.
func NewRealHandlerDepsProvider(log *logger.Logger) *RealHandlerDepsProvider {
	return &RealHandlerDepsProvider{log: log}
}

// GetHandlerDeps returns the handler dependencies.
func (p *RealHandlerDepsProvider) GetHandlerDeps() HandlerDeps {
	return HandlerDeps{
		Log: p.log,
	}
}
