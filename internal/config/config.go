// Package config loads Slive runtime settings from environment variables.
package config

import (
	"os"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr         string
	HealthPath       string
	WebSocketPath    string
	STUNServers      []string
	TURNServers      []TURNServer
	GCParticipantTTL time.Duration
	WSAllowedOrigins []string
	WSReadTimeout    time.Duration
	WSPingInterval   time.Duration
	WSWriteTimeout   time.Duration
}

// TURNServer describes one TURN endpoint and its optional credentials.
// A server's URLs may contain one or more TURN URLs.
type TURNServer struct {
	URLs       []string
	Username   string
	Credential string
}

const (
	DefaultHTTPAddr         = ":8080"
	DefaultHealthPath       = "/health"
	DefaultWebSocketPath    = "/ws"
	DefaultGCParticipantTTL = 60 * time.Second
	DefaultWSReadTimeout    = 60 * time.Second
	DefaultWSPingInterval   = 30 * time.Second
	DefaultWSWriteTimeout   = 10 * time.Second
)

// Load reads runtime configuration. Comma-separated STUN_SERVERS and
// TURN_SERVERS values are supported; TURN_USERNAME and TURN_CREDENTIAL apply
// to each configured TURN server.
func Load() Config {
	cfg := Config{
		HTTPAddr:         envOrDefault("HTTP_ADDR", DefaultHTTPAddr),
		HealthPath:       envOrDefault("HEALTH_PATH", DefaultHealthPath),
		WebSocketPath:    envOrDefault("WEBSOCKET_PATH", DefaultWebSocketPath),
		STUNServers:      splitServerURLs(os.Getenv("STUN_SERVERS")),
		GCParticipantTTL: parseDurationOrDefault(os.Getenv("SLIVE_GC_TTL"), DefaultGCParticipantTTL),
		WSAllowedOrigins: splitAllowedOrigins(os.Getenv("SLIVE_WS_ALLOWED_ORIGINS")),
		WSReadTimeout:    parseDurationOrDefault(os.Getenv("SLIVE_WS_READ_TIMEOUT"), DefaultWSReadTimeout),
		WSPingInterval:   parseDurationOrDefault(os.Getenv("SLIVE_WS_PING_INTERVAL"), DefaultWSPingInterval),
		WSWriteTimeout:   parseDurationOrDefault(os.Getenv("SLIVE_WS_WRITE_TIMEOUT"), DefaultWSWriteTimeout),
	}

	if cfg.WSPingInterval > cfg.WSReadTimeout/2 {
		cfg.WSPingInterval = cfg.WSReadTimeout / 2
	}

	if turnURLs := splitServerURLs(os.Getenv("TURN_SERVERS")); len(turnURLs) > 0 {
		cfg.TURNServers = []TURNServer{{
			URLs:       turnURLs,
			Username:   os.Getenv("TURN_USERNAME"),
			Credential: os.Getenv("TURN_CREDENTIAL"),
		}}
	}

	return cfg
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func splitServerURLs(value string) []string {
	var urls []string
	for _, url := range strings.Split(value, ",") {
		if url = strings.TrimSpace(url); url != "" {
			urls = append(urls, url)
		}
	}

	return urls
}

func parseDurationOrDefault(value string, fallback time.Duration) time.Duration {
	if value == "" {
		return fallback
	}
	if d, err := time.ParseDuration(value); err == nil {
		return d
	}
	return fallback
}

func splitAllowedOrigins(value string) []string {
	var out []string
	for _, v := range strings.Split(value, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}
