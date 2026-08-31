# Slive Go SDK reference

**Package:** `github.com/sajadbayatani/slive/pkg/slive` ·
**Since:** `v0.7.0` · **Go:** 1.24+ · **Policy:** [`VERSIONING.md`](../VERSIONING.md)

This document is the rendered inventory of everything `pkg/slive` exports, and
it is deliberately limited to that package. Nothing from `internal/*` is
documented as stable here. It is maintained by hand, and `test/sdk` keeps it
honest: `TestSDK_PublicSurface_GoDoc` fails if a symbol pinned in
`test/sdk/sdk_test.go` disappears from `go doc -all ./pkg/slive`, loses its doc
comment, or (for error sentinels, which are checked in *both* directions) is
exported without being pinned, and it also enforces the `README.md` snippet's
line budget and call list. Adding a new exported *type or function* without
extending the pinned lists is therefore still a manual step — the reverse
inventory check only exists for sentinels. The authoritative, always-current
view is `go doc`:

```bash
export GOMODCACHE="$PWD/.gocache/mod"
go doc -all ./pkg/slive
```

## Stability legend

| Mark | Meaning |
| --- | --- |
| **S** | **Stable.** Governed by `VERSIONING.md` §2: removal or signature change is a MAJOR (post-1.0) / breaking MINOR (pre-1.0). |
| **U** | **Exported but unstable.** Reachable and documented, but the shape behind it (an `internal/*` type) may change in any release, including a PATCH. Do not build long-lived code on it. |
| **N** | **No-op by design.** Stable symbol whose current implementation deliberately does nothing; see the note in its row. |

Where a row says "alias", the exported name is a Go type alias of an
`internal/*` type, so its **method set is the internal type's method set**.
Adding methods there is a MINOR; removing or changing one is a breaking change
(`VERSIONING.md` §3).

---

## 1. Install

```bash
go get github.com/sajadbayatani/slive/pkg/slive
```

Then import it:

```go
import "github.com/sajadbayatani/slive/pkg/slive"
```

The exported methods of this package speak only in standard-library types
(`context.Context`, `http.Handler`, `*slog.Logger`) and `pkg/slive` types, so
you never need a `pion/*` or `gorilla/*` import to use the SDK. The one
exception is the unstable alias `PeerConnectionConfig`, whose *fields* hold pion
types — another reason it is marked **U** in [§8](#8-signaling-wiring-and-options).

It is a pure-Go, single-process SDK: one `Client` owns one room registry and one
signaling handler inside the same process, and `Client.SignalingURL` /
`Client.HTTPHandler` serve over loopback. There is no remote mode, no
multi-node discovery and no non-Go client.

## 2. Where things are declared

| File | Contents |
| --- | --- |
| `doc.go` | Package documentation and the stability contract. |
| `client.go` | `Client`, `NewClient`, all `Client` methods, `HTTPHandler`, `SignalingURL`. |
| `helpers.go` | `Session`, `Client.Connect`, `ErrSessionClosed`. |
| `config.go` | `SDKConfig`, `Config`, `DefaultSDKConfig`. |
| `types.go` | Domain/signaling/observability type aliases and constants. |
| `options.go` | `NewRoomManager`, `NewHandler`, `WithGCTTL`, `WithMetricsSnapshot`, `WithDiagnosticsSnapshoter`, `DiagnosticsSnapshoter`. |
| `errors.go` | All `Err…` sentinels. |

---

## 3. `Client` — the entry point

```go
func NewClient(cfg SDKConfig) (*Client, error)   // S
```

`NewClient` normalizes zero values: `GCParticipantTTL` → 60s,
`QueueSize` → `DefaultQueueSize` (64, recorded only — see
[§5](#5-configuration)), `Logger` → `slog.Default()`,
`STUNServers` → signaling default (`nil` means default; an **empty slice**
forces STUN-free, which is what the examples and tests use). Validation is
deliberately lenient: it never returns a non-nil error today, but the `error`
result is part of the frozen signature and future releases may start reporting
invalid configuration through it, so keep handling it.

`Client` is safe for concurrent use. `Close` is idempotent.

| Method | Stability | Notes |
| --- | --- | --- |
| `JoinRoom(ctx, roomID, participantID string) (*Room, error)` | **S** | Gets or creates the room and joins the participant. Idempotent: an already-present `participantID` returns the same `*Room` without re-joining, and the probe and the join share one critical section, so concurrent duplicate joins also land on success instead of `ErrParticipantAlreadyExists`. Empty IDs are an error. A participant created here gets a display name of `"Participant " + participantID`. |
| `LeaveRoom(ctx, roomID, participantID string) error` | **S** | `ErrRoomNotFound` if the room is unknown, otherwise the room's `Leave` result, typically `ErrParticipantNotFound`. The two misses have distinct identities (DEF-01, fixed in `0.7.0`): a room miss does **not** match `ErrParticipantNotFound`. Leaving twice reports `ErrParticipantNotFound` the second time. |
| `PublishTrack(ctx, roomID, participantID, trackID string, kind TrackKind, source TrackSource) (*Track, error)` | **S** | Registers the track on both the participant and the room, rolling back the participant registration if the room rejects it. |
| `SubscribeTrack(ctx, roomID, participantID, trackID string) error` | **S** | Domain-level subscription bookkeeping. Does **not** create an SFU forwarder subscriber — for that use [`Session.SubscribeTrack`](#4-session--signaling-client). |
| `UnsubscribeTrack(ctx, roomID, participantID, trackID string) error` | **S** | Inverse of `SubscribeTrack`. |
| `Snapshot() MetricsSnapshot` | **S** | Point-in-time copy of all counters and gauges; never holds handler locks while encoding. See [§7](#7-observability). |
| `Close() error` | **S** | Closes open `Session`s, then the in-process signaling server, then the handler (stopping GC timers). Idempotent; safe to call concurrently with a parked `Session` round-trip; subsequent `JoinRoom`/`SignalingURL` calls fail with `"client is closed"`. |
| `Connect(ctx, roomID, participantID string) (*Session, error)` | **S** | Opens a real WebSocket signaling session. See [§4](#4-session--signaling-client). If the `Client` is closed while the handshake is in flight, the session is torn down and the call fails with an `ErrSessionClosed`-wrapped error rather than returning a dead session. |
| `HTTPHandler() http.Handler` | **S** | The production router (`/health`, `/healthz`, `/ws`) wired to this client's `Snapshot`. Serve it yourself or hand it to `httptest.NewServer`. |
| `SignalingURL() (string, error)` | **S** | Lazily starts an in-process `net/http` server on a `127.0.0.1:0` listener hosting `HTTPHandler` and returns its base URL. The SDK deliberately does not link `net/http/httptest`. `Connect` calls this for you. |
| `RoomManager() *RoomManager` | **U** | Escapes the alias for `signaling.RoomManager`. Prefer `Client` methods; the returned type is unstable, and its room misses keep the *internal* participant identity (see [§9](#9-error-sentinels)). |
| `Handler() *Handler` | **U** | Same, for the signaling handler. Note [§7](#7-observability): the handler's exported metrics-reset hooks are reachable through it and void the monotonicity promise. |

All four room-level methods (`LeaveRoom`, `PublishTrack`, `SubscribeTrack`,
`UnsubscribeTrack`) follow the same rule: an unknown `roomID` is
`ErrRoomNotFound`, and an unknown `participantID` in a known room is
`ErrParticipantNotFound`. Neither matches the other.

## 4. `Session` — signaling client

```go
func (c *Client) Connect(ctx context.Context, roomID, participantID string) (*Session, error)   // S
```

A `Session` is one WebSocket signaling connection for one participant. It
performs the documented handshake: connecting **auto-joins** the room (the
server answers `room_joined`), then each request waits for its matching
response while skipping broadcast notifications (`participant_joined`,
`track_available`, …). This is the supported way to reach the SFU: only the
signaling handler creates `TrackForwarder`s, so
`Snapshot().ForwarderSubscribers` becomes non-zero through `Session`, not
through the domain-only `Client` methods.

| Method | Stability | Notes |
| --- | --- | --- |
| `PublishTrack(ctx, trackID string, kind TrackKind, source TrackSource) error` | **S** | `publish_track` → `track_published`. The handler registers the domain track and creates its forwarder. Repeating a `trackID` fails with an error that satisfies `errors.Is(err, ErrTrackAlreadyPublished)`. |
| `SubscribeTrack(ctx, trackID string) error` | **S** | `subscribe_track` → `track_subscribed`; on return the subscriber is registered on the track's forwarder. A `trackID` the room does not know fails with an error matching `ErrTrackNotFound`. |
| `RoomID() string` / `ParticipantID() string` | **S** | Accessors for the session's identity. |
| `Close() error` | **S** | Tears down the transport only. The server keeps the participant session alive for reconnect until the ghost GC TTL (`WithGCTTL`, default 60s) elapses; use `Client.Close` for full teardown. Idempotent, and it succeeds even while a round-trip is parked waiting for a peer. |

After `Close`, methods return `ErrSessionClosed`. All `Session` methods
serialize their round-trips and are safe for concurrent use. `Session` is not
a media source: RTP arrives via WebRTC negotiation or is pushed by the server
forwarder.

**Deadlines.** Every socket operation runs against a *positive* deadline,
because gorilla reads a zero deadline as "no timeout": the caller's context
deadline wins when it is usable, otherwise a 5-second round-trip bound applies,
and an already-expiring context is floored at 100 ms so the call fails fast
instead of hanging. Two mutexes make that safe — one serializes round-trips, the
other guards the transport and is only held for a pointer snapshot — so
`Session.Close` and `Client.Close` can drop the socket (which is what releases a
parked `ReadMessage`) without queueing behind the round-trip lock.

**Errors.** Server failures arrive as `error` frames and surface as an
unexported `*sessionError` whose text is
`signaling error (<expected message type>): <server message> [<code>]`. It
implements `Unwrap`, so match the *sentinel*, never the text:

| Reply | `errors.Is` target |
| --- | --- |
| Code `room_closed` / `room_not_found` / `participant_not_found` / `track_not_found` / `peer_connection_closed` | `ErrRoomClosed` / `ErrRoomNotFound` / `ErrParticipantNotFound` / `ErrTrackNotFound` / `ErrPeerConnectionClosed` |
| Code `internal_error` (where `internal/signaling` collapses several domain errors) or any other code, with recognizable text | the sentinel whose `internal/domain` message the reply contains (e.g. `ErrTrackAlreadyPublished`, `ErrTrackAlreadySubscribed`, `ErrTrackNotPublished`, `ErrInvalidTrackKind`, `ErrInvalidTrackSource`, `ErrParticipantAlreadyExists`, `ErrParticipantLeft`, `ErrRoomClosed`) |
| Anything else | none — `Unwrap` returns `nil`, so the error matches no sentinel |

An `error` frame tagged with a `request_type` belonging to a *different*
exchange is treated as stream cross-talk: the session is torn down and the call
fails with an `ErrSessionClosed`-wrapped error rather than attributing the wrong
outcome.

## 5. Configuration

```go
type SDKConfig struct {
    STUNServers      []string       // nil = signaling default; []string{} = STUN-free
    GCParticipantTTL time.Duration  // 0 = 60s default; negative disables GC
    QueueSize        int            // <=0 = DefaultQueueSize (64); reserved, see below
    Logger           *slog.Logger   // nil = slog.Default()
}

func DefaultSDKConfig() SDKConfig   // S
type Config = SDKConfig             // S, compatibility alias; prefer SDKConfig
```

| Field | Stability | Notes |
| --- | --- | --- |
| `STUNServers` | **S** | `nil` keeps the signaling default; an **empty slice** forces STUN-free ICE. |
| `GCParticipantTTL` | **S** | Ghost-participant reconnect window. `0` → 60s, negative disables GC. |
| `QueueSize` | **S + N** | **Reserved: recorded, not applied.** `NewClient` normalizes it (`<= 0` → `DefaultQueueSize`, 64) and keeps it on the config, but no forwarder reads it: `signaling.NewHandler` accepts no forwarder option, so every `TrackForwarder` runs with `DefaultQueueSize`. Setting it changes no behaviour until a signaling option plumbs it (sprint-08). The *shape* is stable; the *effect* is pending. |
| `Logger` | **S** | Structured lifecycle events; `nil` → `slog.Default()`. |

`SDKConfig` is the stable replacement for `internal/config.Config`, which stays
the env-var wiring for `cmd/slive`. New fields may be added in a MINOR and will
document their zero-value behaviour, since zero values are normalized by
`NewClient`.

## 6. Domain model types

`Room`, `Participant` and `Track` are aliases of the `internal/domain` types, so
the method sets below **are** the internal method sets: every one of them is
part of the `pkg/slive` contract, and removing or changing one is a breaking
change (`VERSIONING.md` §3). All are safe for concurrent use; each guards its
state with an unexported `sync.RWMutex`.

### `Room` (**S**)

An isolated session that owns the participant list and the track registry. The
zero value is unusable — obtain one from `Client.JoinRoom`, which creates the
room through the room manager and leaves it in `RoomStateActive`.

| Method | Notes |
| --- | --- |
| `ID() string` | The `roomID` passed to `Client.JoinRoom`. |
| `State() RoomState` | `created` → `active` → `closed`. |
| `Create() error` / `Close() error` | Transitions; `Create` is idempotent and `Close` makes later operations fail with `ErrRoomClosed`. `Client` never exposes room closing — see the unstable `RoomManager.CloseRoom`. |
| `Join(*Participant) error` / `Leave(participantID string) error` | Presence. `Join` fails with `ErrParticipantAlreadyExists` on a duplicate ID; `Leave` with `ErrParticipantNotFound`. |
| `GetParticipant(id string) *Participant` / `Participants() []string` | Read-only views. `GetParticipant` returns `nil` when absent — an existence probe, no longer a workaround: since `0.7.0` a room miss carries its own sentinel ([§9](#9-error-sentinels)). |
| `GetTrack(id string) *Track` / `Tracks() []string` | Room track registry. `GetTrack` returns `nil` when absent. |
| `PublishTrack(*Track) error` / `UnpublishTrack(id string) error` | Registry-level publication; `Client.PublishTrack` calls both the participant and the room. |
| `SubscribeToTrack(*Participant, trackID string) error` / `UnsubscribeFromTrack(*Participant, trackID string) error` | Back `Client.SubscribeTrack` / `Client.UnsubscribeTrack`. |

### `Participant` (**S**)

| Method | Notes |
| --- | --- |
| `ID() string` / `Name() string` | Identity. `Client.JoinRoom` sets `Name` to `"Participant " + participantID`. |
| `State() ParticipantState` | `joined` → `active` → `left`. A participant created by `Client.JoinRoom` stays `joined` until `Activate()` is called; the SDK does not activate it for you. |
| `Activate()` / `Leave()` | State transitions. `Leave` also unpublishes this participant's tracks and clears their publisher, but does **not** remove the participant from the room registry — use `Client.LeaveRoom` for that. |
| `Room() *Room` / `SetRoom(*Room)` | Back-reference. |
| `PublishTrack(*Track) error` / `UnpublishTrack(id string) error` | Own-side registry; `ErrTrackAlreadyPublished` on a duplicate ID, `ErrTrackNotFound` when unpublishing an unknown ID. |
| `SubscribeTrack(*Track) error` / `UnsubscribeTrack(id string) error` | Own-side subscription; `ErrTrackAlreadySubscribed` on a duplicate, `ErrTrackNotFound` for an unknown ID. |
| `GetPublishedTrack(id string) *Track` / `PublishedTracks() []string` | Read-only views. |
| `GetSubscribedTrack(id string) *Track` / `SubscribedTracks() []string` | Read-only views. |

### `Track` (**S**)

Pure bookkeeping: identity, kind, source, state, publisher and subscribers. It
holds no transport reference — media binding lives in `internal/webrtc` and is
not exported (see [§10](#10-known-gaps-and-intentional-deferrals)).

| Method | Notes |
| --- | --- |
| `ID() string` / `Kind() TrackKind` / `Source() TrackSource` | Identity and shape. |
| `State() TrackState` | `created` → `published` → `unpublished`. |
| `Publish()` / `Unpublish()` | Move the state; room/participant registration is what makes a track reachable. |
| `Publisher() *Participant` / `SetPublisher(*Participant)` | Owning participant. |
| `AddSubscriber(*Participant) error` | `ErrTrackNotPublished` unless the track is in `TrackStatePublished`; idempotent (returns `nil`) for an already-subscribed participant. |
| `RemoveSubscriber(participantID string)` / `GetSubscriber(participantID string) *Participant` | Detach and look up subscribers; `GetSubscriber` returns `nil` when absent. |
| `Subscribers() []string` / `SubscriberCount() int` / `HasSubscribers() bool` | Read-only copies — the in-room answer to "is anyone receiving this track?". |

### Enums (**S**)

| Type | Constants (iota order) | `String()` |
| --- | --- | --- |
| `TrackKind` | `TrackKindAudio` (0), `TrackKindVideo` | `audio`, `video`, `unknown` |
| `TrackSource` | `TrackSourceMicrophone` (0), `TrackSourceCamera`, `TrackSourceScreenShare` | `microphone`, `camera`, `screen_share`, `unknown` |
| `RoomState` | `RoomStateCreated` (0), `RoomStateActive`, `RoomStateClosed` | `created`, `active`, `closed`, `unknown` |
| `ParticipantState` | `ParticipantStateJoined` (0), `ParticipantStateActive`, `ParticipantStateLeft` | `joined`, `active`, `left`, `unknown` |
| `TrackState` | `TrackStateCreated` (0), `TrackStatePublished`, `TrackStateUnpublished` | `created`, `published`, `unpublished`, `unknown` |

`TrackKind` and `TrackSource` are `int` aliases, so the zero value is a *valid*
audio track / microphone source rather than "unset". `pkg/slive` does not
re-export the `domain.NewRoom` / `NewParticipant` / `NewTrack` constructors:
create state through `Client` (or the unstable `RoomManager`) instead.

## 7. Observability

### `MetricsSnapshot` (S)

`func (c *Client) Snapshot() MetricsSnapshot` returns this struct; it is the
same shape `GET /healthz` encodes, so **the JSON keys are part of the contract**
(`VERSIONING.md` §6). All values are point-in-time observations.

| Field | JSON key | Meaning |
| --- | --- | --- |
| `ConnectionAttemptsTotal uint64` | `connection_attempts_total` | Peer connection attempts started. |
| `ConnectionFailuresTotal uint64` | `connection_failures_total` | Attempts that failed. |
| `ForwarderSubscribers int` | `forwarder_subscribers` | Subscribers currently registered on track forwarders. Non-zero only via `Session`. |
| `ForwarderDroppedTotal uint64` | `forwarder_dropped_total` | RTP packets dropped because a bounded queue was full. Monotonic on every stable code path; see the caveat about the **U** `Handler` reset hooks below. |
| `ForwarderQueueDepth int` | `forwarder_queue_depth` | Current worst/backlog queue depth. |
| `RoomsActive int` | `rooms_active` | Live rooms. |
| `ParticipantsActive int` | `participants_active` | Live participants. |
| `TracksPublished int` | `tracks_published` | Published tracks. |
| `GCReapedTotal uint64` | `gc_reaped_total` | Ghost participants reaped by the TTL GC. |
| `UptimeSeconds int64` | `uptime_seconds` | Handler uptime. |
| `Goroutines int` | `goroutines` | `runtime.NumGoroutine()` at snapshot time. |

```go
type DiagnosticsSnapshoter interface {   // S
    Snapshot() MetricsSnapshot
}
```

`Client`, `Handler` and any user type satisfy it. Implementations must return a
copy without holding caller locks.

> **Monotonicity caveat (unstable surface).** `forwarder_dropped_total` and
> `gc_reaped_total` only count *upwards* while nothing resets them. Because
> `Handler` is an alias of `signaling.Handler`, `Client.Handler()` hands out the
> concrete type, whose exported method set includes the test-only
> `ResetMetrics`, `ResetGCReapedCount`, `ArmGhostForTest` and `ReapGhostForTest`
> hooks. Anything that calls them (including this repository's own scale tests)
> legitimately breaks the counters, so an assertion that
> `forwarder_dropped_total` never decreases only holds for code that never
> touches those hooks. The `sprint-07` review recommends gating them behind a
> build tag in sprint-08; until then this note, not the **U** tier label, is
> what bounds the promise.

### Forwarder knobs

```go
type ForwarderConfig struct {   // S (alias of webrtc.ForwarderConfig)
    QueueSize int
}

const DefaultQueueSize = webrtc.DefaultQueueSize   // S, currently 64
```

`ForwarderConfig` is exported so its tuning knob is discoverable, but **no
`pkg/slive` function takes one**, and the matching `SDKConfig.QueueSize` field
is [reserved](#5-configuration): `NewClient` normalizes and records the value,
yet every `TrackForwarder` the handler builds still runs with
`DefaultQueueSize` (64) because `signaling.NewHandler` accepts no forwarder
option. Neither `SDKConfig.QueueSize` nor a hand-built `ForwarderConfig`
therefore changes queue behaviour on the `0.7.0` surface; real plumbing is a
sprint-08 item.

`PeerConnectionConfig` (alias of `webrtc.PeerConnectionConfig`) is **U** —
pion-facing plumbing. Configure ICE with `SDKConfig.STUNServers`.

## 8. Signaling wiring and options

The `HandlerOption` functions are stable symbols, but the only exported
constructor that accepts them is `NewHandler`, which takes the unstable
`*RoomManager`/`*Handler` types. A normal consumer configures a `Client` with
`SDKConfig`; these exist for advanced wiring and for keeping the option names
frozen while the handler surface settles.

```go
func NewRoomManager() *RoomManager                          // U
func NewHandler(rm *RoomManager, opts ...HandlerOption) *Handler   // U (signature) / S (option types)

type HandlerOption = signaling.HandlerOption                // S
func WithGCTTL(d time.Duration) HandlerOption               // S — ghost-participant GC TTL; 0 disables GC, default 60s
func WithMetricsSnapshot(fn func() MetricsSnapshot) HandlerOption        // S + N
func WithDiagnosticsSnapshoter(s DiagnosticsSnapshoter) HandlerOption    // S + N
```

The two **N** options are no-ops on a `Handler` by design: health wiring lives
in the HTTP layer, so they are published so callers can depend on the frozen
names without importing `internal/http`. Pass `Client.Snapshot` (or
`Handler.Snapshot`) to your HTTP layer instead. Their no-op status is
documented behaviour — see `VERSIONING.md` §4 rule 5 before changing it.

`RoomManager` (**U**) and `Handler` (**U**) are aliases of
`signaling.RoomManager` / `signaling.Handler`. They are exported for
reachability, not for long-term use: `Client.JoinRoom` and `Client.Connect` are
the supported paths, and `RoomManager.GetOrCreateRoom` / `GetRoom` may change
shape in any release.

## 9. Error sentinels

All sentinels are compared with `errors.Is`. Sixteen of them are `var` aliases
of the `internal/domain` / `internal/webrtc` values, so matching works across
both import paths; `ErrRoomNotFound` and `ErrRoomAlreadyExists` are **owned by
`pkg/slive`** (`errors.New` in `errors.go`) and exist only under this import
path. **Message strings are not part of the contract** — only identity is. The
current texts are `"room not found"` and `"room already exists"`.

| Sentinel | Returned when | Reachable from (0.7.0) |
| --- | --- | --- |
| `ErrRoomClosed` | An operation targets a closed room. | `Room` / `Participant` aliases; a `Session` reply carrying the `room_closed` code. `Client` never closes a room. |
| `ErrRoomAlreadyExists` | Reserved for a room-creation collision. | **Nothing yet** — see the DEF-01 note below: the only collision path, the unstable `RoomManager.CreateRoom`, still reports the internal participant error. |
| `ErrRoomNotFound` | A `roomID` the client's registry does not know. | `Client.LeaveRoom`, `Client.PublishTrack`, `Client.SubscribeTrack`, `Client.UnsubscribeTrack`; also the mapping target for a `room_not_found` code on a `Session` reply (the in-process handler does not emit that code today). |
| `ErrParticipantAlreadyExists` | Participant ID already in the room. | `Room.Join` (never from `Client.JoinRoom`, which treats it as the idempotent success); `RoomManager.CreateRoom` reports this internal value for a *room* collision. |
| `ErrParticipantNotFound` | Participant not in the room. | The four room-level `Client` methods above with an unknown `participantID`; `RoomManager.CloseRoom` for an unknown room (internal identity). |
| `ErrParticipantLeft` | Operation targets a participant that has left. | `Participant` / `Room` aliases, e.g. subscribing after `Participant.Leave`. |
| `ErrTrackAlreadyPublished` | Same participant re-publishes a `trackID`. | `Client.PublishTrack`; a `Session.PublishTrack` reply (mapped from the message text). |
| `ErrTrackAlreadySubscribed` | Duplicate subscription. | `Client.SubscribeTrack`; a `Session.SubscribeTrack` reply. |
| `ErrTrackNotFound` | Track not registered in the room. | `Client.SubscribeTrack` / `Client.UnsubscribeTrack`; a `Session` reply carrying the `track_not_found` code. |
| `ErrInvalidTrackKind` | `TrackKind` is neither `TrackKindAudio` nor `TrackKindVideo`. Note `TrackKindAudio` is the iota value `0`, so the zero value means audio — it is not an "unset" marker. | `Client.PublishTrack`, `Session.PublishTrack`. |
| `ErrInvalidTrackSource` | `TrackSource` is not a known source. | `Client.PublishTrack`, `Session.PublishTrack`. |
| `ErrTrackNotPublished` | Operation needs a published track. | `Track.AddSubscriber` through the `Room`/`Participant` aliases; a `Session` reply carrying the text. |
| `ErrTrackNotReady` | Track not ready for media operations. | `internal/webrtc` only — no stable `pkg/slive` symbol returns it. |
| `ErrPeerConnectionClosed` | Target peer connection is closed. | A `Session` round-trip whose reply carries the `peer_connection_closed` code; media paths otherwise. |
| `ErrNoPeerConnection` | No peer connection available. | `internal/webrtc` only — no stable `pkg/slive` symbol returns it. |
| `ErrInvalidSDP` | SDP string failed to parse. | `internal/webrtc` only (raw signaling), never surfaced by `Client` or `Session`. |
| `ErrInvalidICECandidate` | ICE candidate string failed to parse. | `internal/webrtc` only (raw signaling), never surfaced by `Client` or `Session`. |
| `ErrSessionClosed` | A `Session` method runs after teardown, or the transport was dropped because the peer mis-correlated a reply. | `Client.Connect` (rejected mid-handshake), `Session` methods, `Session.Close` callers. |

> **DEF-01 — fixed in `0.7.0`.** `ErrRoomNotFound` and `ErrRoomAlreadyExists`
> used to be `var` aliases of `domain.ErrParticipantNotFound` and
> `domain.ErrParticipantAlreadyExists`, so a room miss and a participant miss
> were indistinguishable and `ErrRoomNotFound.Error()` rendered
> `"participant not found in the room"`. Both are now package-local
> `errors.New` values: a room miss matches `ErrRoomNotFound` **only**, renders
> `"room not found"`, and no probe through `Client.RoomManager().GetRoom` is
> needed to tell the two apart. `test/sdk/errors_test.go`
> (`TestSDK_RoomSentinelIdentity`) pins the split in both directions, and
> `sharedIdentity` there is now an empty list whose job is to fail loudly if any
> two frozen sentinels are ever bound together again.
>
> **Deliberate limit.** The fix is confined to `pkg/slive`, so it covers
> `Client`-level misses only. `slive.RoomManager` — the **U** alias of
> `signaling.RoomManager` — still reports the internal identity:
> `CreateRoom` on an existing room and `CloseRoom` on an unknown one return
> `domain.ErrParticipantAlreadyExists` / `domain.ErrParticipantNotFound`, which
> match `ErrParticipantAlreadyExists` / `ErrParticipantNotFound` and *not*
> `ErrRoomAlreadyExists` / `ErrRoomNotFound`. Room misses from `Client` never
> reach that code, which is exactly why `ErrRoomAlreadyExists` is currently a
> frozen-but-never-returned sentinel.

Adding a sentinel is a MINOR; removing or rebinding one is breaking.
`Client` methods that report "closed" today (`JoinRoom`, `SignalingURL` after
`Close`) return an unexported `fmt.Errorf` value, not a sentinel — do not match
on it; that is why `Close`-state checks are `err != nil` only.

---

## 10. Known gaps and intentional deferrals

* **`SDKConfig.QueueSize` and `ForwarderConfig` are inert.** Both are frozen
  shapes with no effect: the value is normalized and recorded, every
  `TrackForwarder` still runs with `DefaultQueueSize`, and `signaling.NewHandler`
  has no forwarder option to receive it. Treat a queue-size change as a no-op
  until sprint-08 plumbs it (see [§5](#5-configuration),
  [§7](#7-observability)).
* **No RTP injection from the SDK (intentional).** The `TrackForwarder` type
  and its `WriteRTP` method are not exported on the `0.7.0` surface, so no
  `pkg/slive` symbol can push a synthetic RTP burst. TASK-032's brief asked for
  a 10-packet burst in the `publish-subscribe` example; instead the example
  asserts `forwarder_subscribers >= 1` and that `forwarder_dropped_total` is
  monotonic across snapshots (subject to the [§7](#7-observability) reset-hook
  caveat), and the burst itself stays covered by `internal/webrtc` and
  `test/scale`. Rationale: exporting the forwarder now would freeze a type
  whose locking and pooling design changed in sprint-06. This is a stability
  trade-off recorded for architect sign-off
  (`CHANGELOG.md` [0.7.0], `reports/sprint-07-architecture.md`), not an
  oversight. Exporting it later is a MINOR; the SDK path for media will be
  `Session` (or a future typed media handle), not a raw forwarder.
* **Domain-only publish does not create a forwarder.** `Client.PublishTrack`
  touches room state only. Real SFU fan-out requires `Client.Connect` +
  `Session.PublishTrack` / `Session.SubscribeTrack`.
* **`Client` has no option plumbing.** `HandlerOption` values cannot be passed
  to `NewClient`; `SDKConfig` is the only configuration channel.
* **Go-only, single-node.** No JS/TS SDK, no REST management API, no
  multi-instance room federation.

## 11. Runnable examples

| Example | Shows | Run |
| --- | --- | --- |
| [`examples/basic-room`](../examples/basic-room) | room/participant lifecycle + `Snapshot` gauges | `go run ./examples/basic-room` |
| [`examples/publish-subscribe`](../examples/publish-subscribe) | `Client.Connect`, `Session.PublishTrack`/`SubscribeTrack`, forwarder subscriber accounting | `go run ./examples/publish-subscribe` |
| [`examples/health`](../examples/health) | serving `Client.HTTPHandler()` and polling `GET /healthz` | `go run ./examples/health` |

Each has its own README with expected output. All three are STUN-free, exit 0,
and finish in under 5 seconds. See also
[`README.md` §Go SDK](../README.md#go-sdk-pkgslive).

## 12. How this contract is verified

```bash
export GOMODCACHE="$PWD/.gocache/mod"
go doc -all ./pkg/slive                      # every pinned symbol exists and is documented
go test ./test/sdk/... -race -count=1        # surface + error-contract + example + README gates
go vet ./pkg/slive/... ./examples/...        # stdlib vet checks over the public surface
gofmt -l pkg/slive examples test/sdk         # must print nothing
```

`test/sdk` is a normal package inside this module (it does not have its own
`go.mod`) and shells out to the same commands above, so a rename that would
break a consumer fails CI rather than failing a reader. Its
`TestSDK_PublicSurface_GoDoc` also re-reads `README.md`: the snippet must stay
within its line budget, keep calling the documented flow, and reference only
symbols `go doc` still reports.

What these commands do *not* do is police the alias boundary: `go vet` has no
"no internal type in a signature" check, so keeping `internal/*` out of
signatures stays a review rule backed by `VERSIONING.md` §3 and the **U** tier
in this document.

---

**See also:** [`VERSIONING.md`](../VERSIONING.md) ·
[`CHANGELOG.md`](../CHANGELOG.md) ·
[`docs/architecture.md`](architecture.md) ·
[`docs/signaling-protocol.md`](signaling-protocol.md) ·
[`reports/sprint-07-architecture.md`](../reports/sprint-07-architecture.md)
