# Runtime configuration

Slive reads its runtime configuration from environment variables.

| Variable | Default | Purpose |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | HTTP listen address. |
| `HEALTH_PATH` | `/health` | Health-check route. |
| `WEBSOCKET_PATH` | `/ws` | Reserved WebSocket route path for consumers that register the signaling handler. |
| `STUN_SERVERS` | empty | Comma-separated STUN URLs, for example `stun:stun.example.com:3478`. |
| `TURN_SERVER` | empty | Comma-separated TURN URLs (preferred), for example `turn:turn.example.com:3478`. Falls back to `TURN_SERVERS` when unset. |
| `TURN_SERVERS` | empty | Legacy alias for `TURN_SERVER`. Ignored when `TURN_SERVER` is set. |
| `TURN_USERNAME` | empty | Username supplied with the configured TURN URLs. |
| `TURN_PASSWORD` | empty | Credential for the TURN URLs (preferred). Falls back to `TURN_CREDENTIAL` when unset. |
| `TURN_CREDENTIAL` | empty | Legacy alias for `TURN_PASSWORD`. Ignored when `TURN_PASSWORD` is set. |
| `TURN_REALM` | `slive.local` | Realm used by the local coturn container only (see `docker-compose.yml`). Not consumed by Slive — pion has no realm field. |

`Config.STUNServers` contains the configured STUN URLs. `Config.TURNServers`
contains a single credentialed TURN server entry when `TURN_SERVER` (or legacy
`TURN_SERVERS`) is set; each URL is retained in that entry so downstream
WebRTC setup can use it directly. See "Local TURN (coturn)" in the top-level
README for the Docker recipe and the exact URL format browsers need.
