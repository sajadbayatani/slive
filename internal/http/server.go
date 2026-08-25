package http

import (
	"context"
	"net/http"

	"github.com/sajadbayatani/slive/internal/config"
	"github.com/sajadbayatani/slive/internal/logger"
)

// Server wraps an HTTP server with configuration and dependencies.
type Server struct {
	httpServer *http.Server
	log        *logger.Logger
	router     *Router
}

// NewServer creates a new HTTP server with the provided configuration and dependencies.
// It initializes the router with handler dependencies and sets up the HTTP server.
func NewServer(
	cfg config.Config,
	log *logger.Logger,
) *Server {
	// Create handler dependencies
	deps := HandlerDeps{
		Log: log,
	}

	// Create and configure the router
	router := NewRouter(cfg, deps)

	return &Server{
		log:    log,
		router: router,
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

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
