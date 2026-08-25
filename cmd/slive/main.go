package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sajadbayatani/slive/internal/config"
	apphttp "github.com/sajadbayatani/slive/internal/http"
	"github.com/sajadbayatani/slive/internal/logger"
)

func main() {
	cfg := config.Load()

	log := logger.New()

	server := apphttp.NewServer(cfg, log)

	go func() {
		log.Info("starting server", "addr", cfg.HTTPAddr)

		if err := server.Start(); err != nil {
			log.Error("server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)

	signal.Notify(
		stop,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-stop

	log.Info("shutdown signal received")

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}

	log.Info("server stopped")
}
