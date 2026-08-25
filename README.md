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
┌─────────────────────────────────────────────────────────────┐
│                        HTTP Server                              │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                    Router                                  │  │
│  │  /health → HealthHandler                                │  │
│  │  /ws/{roomId} → signaling.Handler                       │  │
│  └─────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Signaling Layer                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐  │
│  │  Connection      │  │  Connection      │  │ Room         │  │
│  │  Manager         │  │  (WebSocket)     │  │ Manager      │  │
│  └─────────────────┘  └─────────────────┘  └─────────────┘  │
│  ┌─────────────────┐                                      │
│  │    Handler       │  ← Routes messages to appropriate     │
│  └─────────────────┘     domain operations                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Domain Layer                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
│  │    Room      │  │ Participant  │  │    Track     │            │
│  │             │  │              │  │              │            │
│  │ - ID        │  │ - ID         │  │ - ID         │            │
│  │ - State     │  │ - Name       │  │ - Kind       │            │
│  │ - Participants││ - State      │  │ - Source     │            │
│  └─────────────┘  │ - Room       │  │ - State      │            │
│                   │ - pubTracks  │  │ - Publisher  │            │
│                   │ - subTracks  │  └─────────────┘            │
│                   └─────────────┘                                │
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

## Running Tests

Slive includes comprehensive tests for the core domain model and signaling layer.

### Run all tests
```bash
make test
```

### Run tests with race detector
```bash
make test-race
```

### Run specific package tests
```bash
go test ./internal/domain/...
go test ./internal/signaling/...
go test ./internal/http/...
```

### Code quality checks
```bash
# Format code
make fmt

# Run vet
make vet

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
│   └── server/           # Main server entry point
├── internal/
│   ├── config/           # Configuration management
│   ├── domain/           # Core domain model (Room, Participant, Track)
│   ├── http/             # HTTP server and routing
│   ├── logger/           # Logging infrastructure
│   └── signaling/        # WebSocket signaling protocol
├── docs/                 # Documentation
├── Makefile              # Build and test commands
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
