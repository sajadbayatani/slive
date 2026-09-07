package config

import (
	"reflect"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	for _, name := range []string{
		"HTTP_ADDR", "HEALTH_PATH", "WEBSOCKET_PATH", "STUN_SERVERS",
		"TURN_SERVER", "TURN_SERVERS", "TURN_USERNAME", "TURN_PASSWORD", "TURN_CREDENTIAL",
	} {
		t.Setenv(name, "")
	}

	cfg := Load()
	if cfg.HTTPAddr != DefaultHTTPAddr {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, DefaultHTTPAddr)
	}
	if cfg.HealthPath != DefaultHealthPath {
		t.Errorf("HealthPath = %q, want %q", cfg.HealthPath, DefaultHealthPath)
	}
	if cfg.WebSocketPath != DefaultWebSocketPath {
		t.Errorf("WebSocketPath = %q, want %q", cfg.WebSocketPath, DefaultWebSocketPath)
	}
	if len(cfg.STUNServers) != 0 || len(cfg.TURNServers) != 0 {
		t.Errorf("unexpected ICE configuration: %#v", cfg)
	}
}

func TestLoadOverridesAndICEServers(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("HEALTH_PATH", "/ready")
	t.Setenv("WEBSOCKET_PATH", "/signal")
	t.Setenv("STUN_SERVERS", "stun:one.example:3478, stun:two.example:3478")
	t.Setenv("TURN_SERVER", "")
	t.Setenv("TURN_SERVERS", "turn:one.example:3478?transport=tcp, turn:two.example:3478")
	t.Setenv("TURN_USERNAME", "slive")
	t.Setenv("TURN_PASSWORD", "")
	t.Setenv("TURN_CREDENTIAL", "test-secret")

	cfg := Load()
	if cfg.HTTPAddr != ":9090" || cfg.HealthPath != "/ready" || cfg.WebSocketPath != "/signal" {
		t.Fatalf("route configuration not loaded: %#v", cfg)
	}
	if want := []string{"stun:one.example:3478", "stun:two.example:3478"}; !reflect.DeepEqual(cfg.STUNServers, want) {
		t.Errorf("STUNServers = %#v, want %#v", cfg.STUNServers, want)
	}
	if len(cfg.TURNServers) != 1 {
		t.Fatalf("TURNServers = %#v, want one server", cfg.TURNServers)
	}
	turn := cfg.TURNServers[0]
	if want := []string{"turn:one.example:3478?transport=tcp", "turn:two.example:3478"}; !reflect.DeepEqual(turn.URLs, want) {
		t.Errorf("TURN URLs = %#v, want %#v", turn.URLs, want)
	}
	if turn.Username != "slive" || turn.Credential != "test-secret" {
		t.Errorf("TURN credentials = %#v", turn)
	}
}

func TestLoadSingularTurnEnvNames(t *testing.T) {
	t.Setenv("TURN_SERVER", "turn:one.example:3478, turn:two.example:3478?transport=udp")
	t.Setenv("TURN_SERVERS", "")
	t.Setenv("TURN_USERNAME", "slive")
	t.Setenv("TURN_PASSWORD", "singular-secret")
	t.Setenv("TURN_CREDENTIAL", "")

	cfg := Load()
	if len(cfg.TURNServers) != 1 {
		t.Fatalf("TURNServers = %#v, want one server", cfg.TURNServers)
	}
	turn := cfg.TURNServers[0]
	if want := []string{"turn:one.example:3478", "turn:two.example:3478?transport=udp"}; !reflect.DeepEqual(turn.URLs, want) {
		t.Errorf("TURN URLs = %#v, want %#v", turn.URLs, want)
	}
	if turn.Username != "slive" || turn.Credential != "singular-secret" {
		t.Errorf("TURN credentials = %#v, want slive/singular-secret", turn)
	}
}

func TestLoadTurnSingularNamesTakePrecedence(t *testing.T) {
	t.Setenv("TURN_SERVER", "turn:preferred.example:3478")
	t.Setenv("TURN_SERVERS", "turn:legacy.example:3478")
	t.Setenv("TURN_USERNAME", "slive")
	t.Setenv("TURN_PASSWORD", "preferred-secret")
	t.Setenv("TURN_CREDENTIAL", "legacy-secret")

	cfg := Load()
	if len(cfg.TURNServers) != 1 {
		t.Fatalf("TURNServers = %#v, want one server", cfg.TURNServers)
	}
	turn := cfg.TURNServers[0]
	if want := []string{"turn:preferred.example:3478"}; !reflect.DeepEqual(turn.URLs, want) {
		t.Errorf("TURN URLs = %#v, want %#v", turn.URLs, want)
	}
	if turn.Credential != "preferred-secret" {
		t.Errorf("TURN credential = %q, want preferred-secret", turn.Credential)
	}
}

func TestLoadTurnLegacyFallbackWithoutSingular(t *testing.T) {
	t.Setenv("TURN_SERVER", "")
	t.Setenv("TURN_SERVERS", "turn:legacy.example:3478")
	t.Setenv("TURN_USERNAME", "slive")
	t.Setenv("TURN_PASSWORD", "")
	t.Setenv("TURN_CREDENTIAL", "legacy-secret")

	cfg := Load()
	if len(cfg.TURNServers) != 1 {
		t.Fatalf("TURNServers = %#v, want one server", cfg.TURNServers)
	}
	turn := cfg.TURNServers[0]
	if want := []string{"turn:legacy.example:3478"}; !reflect.DeepEqual(turn.URLs, want) {
		t.Errorf("TURN URLs = %#v, want %#v", turn.URLs, want)
	}
	if turn.Credential != "legacy-secret" {
		t.Errorf("TURN credential = %q, want legacy-secret", turn.Credential)
	}
}

func TestSplitServerURLs(t *testing.T) {
	if got := splitServerURLs(" , stun:one , ,stun:two "); !reflect.DeepEqual(got, []string{"stun:one", "stun:two"}) {
		t.Errorf("splitServerURLs() = %#v", got)
	}
}
