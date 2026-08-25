// Package config loads Slive runtime settings from environment variables.
package config

import (
	"os"
	"strings"
)

type Config struct {
	HTTPAddr      string
	HealthPath    string
	WebSocketPath string
	STUNServers   []string
	TURNServers   []TURNServer
}

// TURNServer describes one TURN endpoint and its optional credentials.
// A server's URLs may contain one or more TURN URLs.
type TURNServer struct {
	URLs       []string
	Username   string
	Credential string
}

const (
	DefaultHTTPAddr      = ":8080"
	DefaultHealthPath    = "/health"
	DefaultWebSocketPath = "/ws"
)

// Load reads runtime configuration. Comma-separated STUN_SERVERS and
// TURN_SERVERS values are supported; TURN_USERNAME and TURN_CREDENTIAL apply
// to each configured TURN server.
func Load() Config {
	cfg := Config{
		HTTPAddr:      envOrDefault("HTTP_ADDR", DefaultHTTPAddr),
		HealthPath:    envOrDefault("HEALTH_PATH", DefaultHealthPath),
		WebSocketPath: envOrDefault("WEBSOCKET_PATH", DefaultWebSocketPath),
		STUNServers:   splitServerURLs(os.Getenv("STUN_SERVERS")),
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
