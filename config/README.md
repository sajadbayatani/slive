# Runtime configuration

Slive reads its runtime configuration from environment variables.

| Variable | Default | Purpose |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | HTTP listen address. |
| `HEALTH_PATH` | `/health` | Health-check route. |
| `WEBSOCKET_PATH` | `/ws` | Reserved WebSocket route path for consumers that register the signaling handler. |
| `STUN_SERVERS` | empty | Comma-separated STUN URLs, for example `stun:stun.example.com:3478`. |
| `TURN_SERVERS` | empty | Comma-separated TURN URLs. |
| `TURN_USERNAME` | empty | Username supplied with the configured TURN URLs. |
| `TURN_CREDENTIAL` | empty | Credential supplied with the configured TURN URLs. |

`Config.STUNServers` contains the configured STUN URLs. `Config.TURNServers`
contains a single credentialed TURN server entry when `TURN_SERVERS` is set;
each URL is retained in that entry so downstream WebRTC setup can use it
directly.
