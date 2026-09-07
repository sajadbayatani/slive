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
//
// DIAG-TURN: logs the effective ICE/TURN configuration at startup (TURN
// URL, username, realm, credential presence — never the secret itself) so a
// live test can prove which relay the server-side PCs gather from.
func newSignalingHandler(cfg config.Config, log *logger.Logger) *signaling.Handler {
	pcConfig := buildPeerConnectionConfig(cfg)
	logTurnDiagnostics(cfg, pcConfig, log)
	return signaling.NewHandler(
		signaling.NewRoomManager(),
		signaling.WithPeerConnectionConfig(pcConfig),
		signaling.WithLogger(log.Logger),
		signaling.WithGCTTL(cfg.GCParticipantTTL),
		signaling.WithAllowedOrigins(cfg.WSAllowedOrigins),
		signaling.WithWSReadTimeout(cfg.WSReadTimeout),
		signaling.WithWSPingInterval(cfg.WSPingInterval),
		signaling.WithWSWriteTimeout(cfg.WSWriteTimeout),
	)
}

// logTurnDiagnostics emits one startup line proving the effective ICE/TURN
// configuration. TURN URL, username and realm are logged; the credential
// itself is never logged, only whether one is set. This is diagnostic
// only: it changes no SDP, forwarding or transceiver behavior.
func logTurnDiagnostics(cfg config.Config, pcConfig webrtc.PeerConnectionConfig, log *logger.Logger) {
	var turnURLs []string
	var turnUsername string
	credentialSet := false
	for _, server := range cfg.TURNServers {
		turnURLs = append(turnURLs, server.URLs...)
		if turnUsername == "" {
			turnUsername = server.Username
		}
		if server.Credential != "" {
			credentialSet = true
		}
	}
	log.Info("ice/turn configuration",
		"event", "ice_turn_config",
		"turn_urls", turnURLs,
		"turn_username", turnUsername,
		"turn_realm", os.Getenv("TURN_REALM"),
		"turn_credential_set", credentialSet,
		"stun_servers", cfg.STUNServers,
		"ice_servers_count", len(pcConfig.ICEServers),
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
