package http

import "net/http"

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
}

// HealthHandler handles requests to the /health endpoint.
// It uses the injected logger for structured logging.
type HealthHandler struct {
	deps HandlerDeps
}

// NewHealthHandler creates a new HealthHandler with the provided dependencies.
func NewHealthHandler(deps HandlerDeps) *HealthHandler {
	return &HealthHandler{deps: deps}
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
