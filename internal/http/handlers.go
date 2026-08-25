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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
