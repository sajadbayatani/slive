package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sajadbayatani/slive/internal/config"
	apphttp "github.com/sajadbayatani/slive/internal/http"
	"github.com/sajadbayatani/slive/internal/logger"
	"github.com/sajadbayatani/slive/internal/signaling"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Printf("slive %s (commit %s, built %s)\n", version, commit, date)
		os.Exit(0)
	}

	cfg := config.Load()

	log := logger.New()

	server := apphttp.NewServer(cfg, log,
		apphttp.WithSignalingHandler(newSignalingHandler(cfg, log)),
	)

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

// newSignalingHandler wires the WebSocket signaling endpoint with runtime
// configuration: ICE servers are translated from STUN_SERVERS/TURN_SERVERS
// and the application's structured logger is propagated into every peer
// connection and signaling session.
func newSignalingHandler(cfg config.Config, log *logger.Logger) *signaling.Handler {
	return signaling.NewHandler(
		signaling.NewRoomManager(),
		signaling.WithPeerConnectionConfig(buildPeerConnectionConfig(cfg)),
		signaling.WithLogger(log.Logger),
		signaling.WithGCTTL(cfg.GCParticipantTTL),
		signaling.WithAllowedOrigins(cfg.WSAllowedOrigins),
		signaling.WithWSReadTimeout(cfg.WSReadTimeout),
		signaling.WithWSPingInterval(cfg.WSPingInterval),
		signaling.WithWSWriteTimeout(cfg.WSWriteTimeout),
	)
}

// buildPeerConnectionConfig translates application configuration into the
// PeerConnectionConfig used for every peer connection this server creates.
// When no ICE servers are configured, the webrtc package defaults apply.
func buildPeerConnectionConfig(cfg config.Config) webrtc.PeerConnectionConfig {
	pcConfig := webrtc.DefaultPeerConnectionConfig()

	if iceServers := webrtc.ICEServersFromURLs(cfg.STUNServers, turnServersFromConfig(cfg.TURNServers)); len(iceServers) > 0 {
		pcConfig.ICEServers = iceServers
	}

	return pcConfig
}

// turnServersFromConfig maps parsed TURN configuration onto the ICE-server
// representation expected by the webrtc package.
func turnServersFromConfig(servers []config.TURNServer) []webrtc.ICETurnServer {
	out := make([]webrtc.ICETurnServer, 0, len(servers))
	for _, server := range servers {
		out = append(out, webrtc.ICETurnServer{
			URLs:       server.URLs,
			Username:   server.Username,
			Credential: server.Credential,
		})
	}
	return out
}
