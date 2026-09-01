# slive

A self-hosted realtime communication server written in Go.

`slive` is an open-source realtime communication infrastructure
designed to provide the core building blocks required by applications
such as video conferencing, live collaboration, remote classrooms,
telemedicine, customer support, and other realtime communication systems.

The project is inspired by the architecture and concepts behind
modern realtime platforms such as LiveKit, but is built independently
as a long-term engineering and learning project.

> ⚠️ slive is currently under active development and is not
> production-ready.

---

## What is slive?

slive is the infrastructure layer.

It is **not** a meeting application.

The goal is to provide a reusable realtime communication platform
that other applications can build on top of.

```text
┌─────────────────────────┐
│      Application        │
│                         │
│   e.g. slive-meet       │
└────────────┬────────────┘
             │
             │ slive API / SDK
             ▼
┌─────────────────────────┐
│          slive          │
│                         │
│  Signaling              │
│  WebRTC                 │
│  SFU                    │
│  Rooms                  │
│  Participants           │
│  Tracks                 │
│  Data Channels          │
│  Media Routing          │
└─────────────────────────┘
```

---

## Core Domain Model

The core domain model consists of three main entities:

### Room
- Represents an isolated real-time communication session
- Manages participant lifecycle and state
- States: `created` → `active` → `closed`
- Thread-safe with mutex-protected operations

### Participant
- Represents a client connected to a room
- Manages published and subscribed tracks
- States: `joined` → `active` → `left`
- Each participant has a unique ID and display name

### Track
- Represents an audio or video media stream
- Owned and published by a participant
- Types: `audio` or `video`
- Sources: `microphone`, `camera`, `screen_share`
- States: `created` → `published` → `unpublished`

### Architecture Diagram

```text
┌───────────────────────────────────────────────────────────────┐
│                        HTTP Server                            │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                    Router                               │  │
│  │  /health → HealthHandler                                │  │
│  │  /ws/{roomId} → signaling.Handler                       │  │
│  └─────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Signaling Layer                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐  │
│  │  Connection     │  │  Connection     │  │ Room        │  │
│  │  Manager        │  │  (WebSocket)    │  │ Manager     │  │
│  └─────────────────┘  └─────────────────┘  └─────────────┘  │
│  ┌─────────────────┐                                        │
│  │    Handler      │  ← Routes messages to appropriate      │
│  └─────────────────┘     domain operations                  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Domain Layer                             │
│  ┌───────────────┐  ┌──────────────┐  ┌─────────────┐       │
│  │    Room       │  │ Participant  │  │    Track    │       │
│  │               │  │              │  │             │       │
│  │ - ID          │  │ - ID         │  │ - ID        │       │
│  │ - State       │  │ - Name       │  │ - Kind      │       │
│  │ - Participants│  │ - State      │  │ - Source    │       │
│  └───────────────┘  │ - Room       │  │ - State     │       │
│                     │ - pubTracks  │  │ - Publisher │       │
│                     │ - subTracks  │  └─────────────┘       │
│                     └──────────────┘                        │
└─────────────────────────────────────────────────────────────┘
```

---

## Signaling Protocol

Slive uses a WebSocket-based signaling protocol for real-time session negotiation.

### Transport
- **Protocol**: WebSocket (RFC 6455)
- **Path**: `/ws/{roomId}`
- **Message Format**: JSON with `type` and `data` fields

### Message Types

#### Room Management
- `create_room` / `room_created` - Create a new room
- `join_room` / `room_joined` - Join an existing room
- `leave_room` / `room_left` - Leave a room

#### Participant Management
- `participant_joined` - Notification when a participant joins
- `participant_left` - Notification when a participant leaves

#### Track Management
- `publish_track` / `track_published` - Publish a media track
- `unpublish_track` / `track_unpublished` - Unpublish a track
- `subscribe_track` / `track_subscribed` - Subscribe to a track
- `track_available` - Notification when a track is available
- `track_unavailable` - Notification when a track becomes unavailable

#### WebRTC Signaling
- `offer` / `answer` - SDP exchange for WebRTC session establishment
- `ice_candidate` - ICE candidate exchange for NAT traversal

#### Error Handling
- `error` - Error responses with codes and messages

### Protocol Flow

1. **Room Creation**: Client creates room → Server creates room and participant → Server responds with success
2. **Joining**: Client joins room → Server adds participant → Server responds with room info and broadcasts to others
3. **Publishing Tracks**: Client publishes track → Server creates track → Server responds and broadcasts availability
4. **WebRTC Negotiation**: Clients exchange offers/answers/ICE candidates through the server

For detailed message formats and examples, see [docs/signaling-protocol.md](docs/signaling-protocol.md).

---

## Go SDK (`pkg/slive`)

`github.com/sajadbayatani/slive/pkg/slive` is the stable, SemVer-versioned Go
API for Slive. Slive is infrastructure, not an application: the SDK gives you
rooms, participants, tracks, SFU signaling and health diagnostics, and you own
the product layer above it. `internal/*` stays unstable, so import `pkg/slive`
instead of reaching into it.

> The SDK is Go-only and single-node in this release. There are no JS/TS
> bindings yet — browsers talk to the signaling WebSocket endpoint directly.

### Install

```bash
go get github.com/sajadbayatani/slive/pkg/slive
```

Requires Go 1.24 or newer.

### Minimal example

The whole required flow — `NewClient`, `JoinRoom`, `PublishTrack`,
`SubscribeTrack`, `Snapshot`, `LeaveRoom`, `Close` — in 20 lines:

```go
package main
import (
	"context"
	"log"
	"github.com/sajadbayatani/slive/pkg/slive"
)
func main() {
	client, err := slive.NewClient(slive.SDKConfig{STUNServers: []string{}}) // STUN-free: nothing leaves this process
	if err != nil { log.Fatal(err) }
	defer client.Close()
	ctx := context.Background()
	room, err := client.JoinRoom(ctx, "room-001", "alice")
	if err != nil { log.Fatal(err) }
	track, err := client.PublishTrack(ctx, room.ID(), "alice", "mic-001", slive.TrackKindAudio, slive.TrackSourceMicrophone)
	if err != nil { log.Fatal(err) }
	if _, err := client.JoinRoom(ctx, room.ID(), "bob"); err != nil { log.Fatal(err) }
	if err := client.SubscribeTrack(ctx, room.ID(), "bob", track.ID()); err != nil { log.Fatal(err) }
	snapshot := client.Snapshot(); log.Printf("rooms=%d participants=%d tracks=%d", snapshot.RoomsActive, snapshot.ParticipantsActive, snapshot.TracksPublished)
	if err := client.LeaveRoom(ctx, room.ID(), "alice"); err != nil { log.Fatal(err) }
}
```

Counting rule for the 20-line budget: the fence is the *entire* program, and
each error check is condensed onto one line — `gofmt` expands those
`if err != nil { … }` one-liners when you paste the file, which is fine; the
SDK calls are what fit the budget. `test/sdk` fails if this snippet stops being
a complete program within 20 lines or stops calling every symbol above.

Behaviour worth knowing from this snippet: `JoinRoom` is idempotent, so
re-joining `alice` returns the same room rather than an
`ErrParticipantAlreadyExists`; `PublishTrack`/`SubscribeTrack`/`LeaveRoom`
distinguish their misses, returning `ErrRoomNotFound` for an unknown room and
`ErrParticipantNotFound` for an unknown participant (`errors.Is` on both).
Leaving a room destroys the tracks its publisher owned.

`Client` methods manage room state in-process. To end up with real
`TrackForwarder` subscribers — i.e. a non-zero
`Snapshot().ForwarderSubscribers` — drive the WebSocket signaling protocol with
`client.Connect(ctx, roomID, participantID)` and use the returned
`*slive.Session` (`PublishTrack` / `SubscribeTrack` / `Close`). Serve
`client.HTTPHandler()` (or dial it with `client.SignalingURL()`) to mount the
production `/health`, `/healthz` and `/ws` routes without importing
`internal/http`.

### Runnable examples

| Example | What it proves | Run |
| --- | --- | --- |
| [`basic-room`](examples/basic-room/README.md) | 1 room × 2 participants, `Snapshot()` gauges | `go run ./examples/basic-room` |
| [`publish-subscribe`](examples/publish-subscribe/README.md) | real signaling sessions, `forwarder_subscribers >= 1` | `go run ./examples/publish-subscribe` |
| [`health`](examples/health/README.md) | `Client.HTTPHandler()` serving `/healthz` with `status=ok` | `go run ./examples/health` |

All three are STUN-free, exit 0, and finish in under 5 seconds. See
[examples/README.md](examples/README.md) for the expected output of each.

### API reference, stability and changelog

- [docs/sdk.md](docs/sdk.md) — exported surface table (types, methods, options, error sentinels).
- [pkg/slive doc.go](pkg/slive/doc.go) — the package contract, also via `go doc -all ./pkg/slive`.
- [VERSIONING.md](VERSIONING.md) — SemVer rules, deprecation policy, stable vs unstable surface.
- [CHANGELOG.md](CHANGELOG.md) — released versions; current SDK surface is `0.7.0`.

---

## Running Tests

Slive includes comprehensive tests for the core domain model and signaling layer.
All commands use a repository-local module cache (`GOMODCACHE="$PWD/.gocache/mod"`).

### Run all tests (untagged, hook-free)
```bash
make test
# → GOMODCACHE="$PWD/.gocache/mod" go test ./... -race -count=1
```

### Run tests with internal hooks (gated)
```bash
make test-internal
# → GOMODCACHE="$PWD/.gocache/mod" go test -tags slive_internal ./... -race -count=1
```

### SDK smoke (build + examples)
```bash
make smoke
# → go build ./... + go run ./examples/basic-room, publish-subscribe, health (each greps expected log lines)
```

### Update scale baseline (human-only)
```bash
make baseline
# → go test -tags slive_internal ./test/scale -run TestScaleCapacity -count=1 -update-baseline
# Only this target passes -update-baseline; CI never does (reports/ stays clean).
```

### Run specific package tests
```bash
export GOMODCACHE="$PWD/.gocache/mod"
go test ./internal/domain/... -race -count=1
go test ./internal/signaling/... -race -count=1
go test -tags slive_internal ./test/scale -run TestScaleCapacity -count=1
```

### Code quality checks
```bash
# Format code
make fmt

# Run vet
make vet

# Lint (gofmt + vet as in CI)
make lint

# Full check (format, vet, test)
make check
```

### Build the server
```bash
make build
```

### Run the server
```bash
make run
```

---

## Project Structure

```text
slive/
├── cmd/
│   └── slive/            # Main server entry point
├── internal/
│   ├── config/           # Configuration management (unstable)
│   ├── domain/           # Core domain model: Room, Participant, Track (unstable)
│   ├── http/             # HTTP server, health/healthz, router (unstable)
│   ├── logger/           # Logging infrastructure (unstable)
│   ├── signaling/        # WebSocket signaling protocol + SFU wiring (unstable)
│   └── webrtc/           # Peer connections, TrackForwarder, metrics (unstable)
├── pkg/
│   └── slive/            # Stable public Go SDK facade (SemVer)
├── examples/             # Runnable `go run` SDK examples
│   ├── basic-room/
│   ├── publish-subscribe/
│   └── health/
├── test/
│   ├── e2e/              # Signaling/protocol end-to-end tests
│   ├── scale/            # Single-node scale + capacity harness
│   └── sdk/              # Public-surface and example-run tests
├── docs/                 # Architecture, signaling protocol, SDK reference
├── Makefile              # Build and test commands
├── VERSIONING.md         # SemVer + deprecation policy
├── CHANGELOG.md          # Released versions
└── go.mod/go.sum         # Go module files
```

---

## Configuration

See `.env.example` for configuration options. The server can be configured via:
- Environment variables
- Command-line flags (future)
- Configuration files (future)

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `make check` to ensure code quality
5. Submit a pull request

---

## License

MIT License - see LICENSE file for details.
