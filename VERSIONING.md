# Slive versioning and compatibility policy

**Applies to:** the Go public surface exported by
[`pkg/slive`](pkg/slive) (import path
`github.com/sajadbayatani/slive/pkg/slive`), the signalling/health wire formats
that surface depends on, and the released module versions.
**Status:** active, ratified by the sprint-07 architecture report
(`reports/sprint-07-architecture.md`, decisions D1–D4).

This policy is human-readable and manually enforced. There is no automated
breaking-change detector; the enforcement points are the tests in
[`test/sdk`](test/sdk) plus the release checklist at the bottom.

---

## 1. What gets versioned

Slive ships one Go module (`github.com/sajadbayatani/slive`) with tags
`vMAJOR.MINOR.PATCH` in the repository root. Because `pkg/slive` lives in that
module, a consumer installs it as

```bash
go get github.com/sajadbayatani/slive/pkg/slive@v0.7.0
```

The **compatibility promise is `pkg/slive` only.** The module also contains
`cmd/slive` (the server binary) and `internal/*` (the implementation). Those
move at the same version number but carry no compatibility promise — see
[§6](#6-unstable-surface).

Slive is pre-1.0. The current release line is `0.7.x`, where the minor number
tracks the sprint that produced it (`0.7.0` = sprint-07).

No tag exists in the repository yet, so `v0.7.0` is the **first** release to be
cut (see the checklist in [§8](#8-release-checklist)); the `0.1.0`–`0.6.0`
entries in `CHANGELOG.md` are retroactive labels for sprints 01–06, not
previously published versions. Nothing has been consumed from a tag yet, so
this policy binds from `v0.7.0` forward.

---

## 2. SemVer rules applied to `pkg/slive`

| Bump | Rule | Slive examples |
| --- | --- | --- |
| **MAJOR** | Any breaking change to the exported surface of `pkg/slive`: removing or renaming an exported type, function, method, constant, variable or struct field; changing a signature (parameter, receiver or result types); narrowing documented behaviour so previously-valid calls now fail; changing the meaning of an already-released zero value. | dropping `Client.SignalingURL`; `Snapshot()` returning `(*MetricsSnapshot, error)`; removing `ErrTrackNotFound`. |
| **MINOR** | Additive, backwards-compatible surface: new exported symbols, new struct fields with a documented default, new error sentinels, new optional behaviour reachable only through a new symbol, new `MetricsSnapshot` fields. Also carries deprecation notices. | adding `Client.Connect` and `Session` in `0.7.0`; adding a field to `SDKConfig`. |
| **PATCH** | Bug fixes and performance work that change no exported signature and no documented behaviour. Dependencies, internal refactors and doc-only edits are PATCHes. | forwarder queue accounting fix inside `internal/webrtc`; `WithGCTTL` default corrections. |

### Pre-1.0 exception

While the major version is `0`, a **MINOR may break** `pkg/slive` (SemVer
§4: "initial development" software). Two conditions apply: the entry in
`CHANGELOG.md` must open with a **Breaking** section, and that section must
contain a migration snippet for every changed symbol. The deprecation process
in [§4](#4-deprecation-policy) still applies — a symbol is never removed in a
MINOR without having carried a `// Deprecated:` notice for at least one
released MINOR first.

From `v1.0.0` onward this exception is void: breaking changes require a MAJOR
and are the only thing that does.

---

## 3. Compatibility mechanism: aliases, not copies

`Room`, `Participant`, `Track`, `MetricsSnapshot`, `ForwarderConfig`,
`RoomManager`, `Handler`, `HandlerOption` and `PeerConnectionConfig` are Go
**type aliases** over `internal/*` types, and the error sentinels are `var`
aliases of `internal/domain` / `internal/webrtc` values — except
`ErrRoomNotFound` and `ErrRoomAlreadyExists`, which are `errors.New` values
owned by `pkg/slive` (that ownership is what let DEF-01 be fixed in `0.7.0`
without touching `internal/*`). This keeps
`errors.Is` working and avoids a second translation layer, but it means the
promise is only as good as the internal type behind it. Consequently:

* Moving, renaming or unexporting a method on `internal/domain.Room` **is** a
  `pkg/slive` breaking change (MAJOR / pre-1.0 MINOR), even though the file
  that changed is under `internal/`.
* **Adding** a method to an internal type is never breaking for consumers.
* The *location* of an internal package is free — `internal/domain` may be
  split, merged or renamed in any release, including a PATCH — but the promise
  travels with the aliased type: such a move must preserve its **identity and
  method set**. Moving it is allowed; changing what the alias points at is a
  breaking change.

---

## 4. Deprecation policy

1. **Notice first, remove later.** A symbol on the stable surface carries a
   `// Deprecated:` doc comment for **at least one released MINOR** before it
   is removed. Removal itself happens in a MAJOR after 1.0 (pre-1.0: in a
   MINOR with a Breaking section).
2. **Machine- and human-readable marker**, exactly in this shape, so `go vet`'s
   deprecation check and IDEs pick it up:

   ```go
   // LeaveRoom removes a participant from a room.
   //
   // Deprecated: use Session.Close instead; the in-process participant
   // registry is being replaced by connection-scoped lifecycle. Will be
   // removed in v1.1.0 (no earlier than one MINOR after v0.9.0).
   func (c *Client) LeaveRoom(ctx context.Context, roomID, participantID string) error
   ```

   The comment must say what to use instead and the earliest removal version.
3. **No silent removal.** Every deprecation and every removal gets a
   `CHANGELOG.md` line under `Deprecated` / `Removed` in the release that does
   it, plus a migration snippet.
4. **Deprecate, don't re-purpose.** Changing what a stable symbol does is
   breaking; the supported path is to deprecate it and add a new one.
5. **Options that are documented no-ops** (`WithMetricsSnapshot`,
   `WithDiagnosticsSnapshoter` on a `Handler`) are deprecated-and-removed like
   any other symbol: their no-op status is part of the published behaviour, so
   wiring them up to do something real is a **MINOR**, and removing them is a
   breaking change.

---

## 5. Stable surface (`pkg/slive`)

Everything below is covered by [§2](#2-semver-rules-applied-to-pkgslive). It is
the list mirrored by `pkg/slive/doc.go`, the table in
[`docs/sdk.md`](docs/sdk.md), and the pinned symbols in
`test/sdk/sdk_test.go`.

**Client and lifecycle**

* `Client`, `NewClient`, `DefaultSDKConfig`, `SDKConfig`, `Config`
* `Client` methods: `JoinRoom`, `LeaveRoom`, `PublishTrack`, `SubscribeTrack`,
  `UnsubscribeTrack`, `Snapshot`, `Close`, `RoomIDs`, `CloseRoom`
* `Session` and its methods `PublishTrack`, `SubscribeTrack`, `Close`,
  `RoomID`, `ParticipantID`; the `Client` helpers `Connect`, `HTTPHandler`,
  `SignalingURL`

**Domain model**

* `Room`, `Participant`, `Track`
* `TrackKind` + `TrackKindAudio`, `TrackKindVideo`
* `TrackSource` + `TrackSourceMicrophone`, `TrackSourceCamera`,
  `TrackSourceScreenShare`
* `RoomState` + `RoomStateCreated`, `RoomStateActive`, `RoomStateClosed`
* `ParticipantState` + `ParticipantStateJoined`, `ParticipantStateActive`,
  `ParticipantStateLeft`
* `TrackState` + `TrackStateCreated`, `TrackStatePublished`,
  `TrackStateUnpublished`
* the exported accessor/method sets of `Room`, `Participant` and `Track`
  (`ID`, `State`, `Participants`, `Tracks`, `Kind`, `Source`,
  `SubscriberCount`, …)

**Observability and forwarder knobs**

* `MetricsSnapshot` (all eleven fields **and their JSON keys**, which are the
  `GET /healthz` payload)
* `ForwarderConfig` (`QueueSize`), `DefaultQueueSize` — `SDKConfig.QueueSize`
  is normalized by `NewClient` and plumbed via `WithForwarderConfig` so every
  `TrackForwarder` uses it; wiring was a MINOR (sprint-08). Changing what the
  knob means remains breaking.
* `SDKConfig.AllowedOrigins` (`[]string`) — allowlist for WebSocket origin policy (sprint-08, additive).
* `DiagnosticsSnapshoter`
* `HandlerOption` and the option constructors `WithGCTTL`,
  `WithForwarderConfig`, `WithAllowedOrigins`, `WithWSReadTimeout`, `WithWSPingInterval`, `WithWSWriteTimeout`, `WithMetricsSnapshot`, `WithDiagnosticsSnapshoter`

**Errors**

* every `Err…` sentinel exported by `pkg/slive`, matched by `errors.Is`:
  `ErrRoomClosed`, `ErrRoomAlreadyExists`, `ErrRoomNotFound`,
  `ErrParticipantAlreadyExists`, `ErrParticipantNotFound`,
  `ErrParticipantLeft`, `ErrTrackAlreadyPublished`,
  `ErrTrackAlreadySubscribed`, `ErrTrackNotFound`, `ErrInvalidTrackKind`,
  `ErrInvalidTrackSource`, `ErrTrackNotPublished`, `ErrTrackNotReady`,
  `ErrPeerConnectionClosed`, `ErrNoPeerConnection`, `ErrInvalidSDP`,
  `ErrInvalidICECandidate`, `ErrSessionClosed`, `ErrClientClosed`, `ErrInvalidArgument`
* sixteen of them alias an `internal/*` value and two
  (`ErrRoomNotFound`, `ErrRoomAlreadyExists`) are owned by `pkg/slive`; which
  side a name is on is part of the promise, because it decides what the
  sentinel can be confused with. A room miss on a `Client` method matches
  `ErrRoomNotFound` only; the unstable `RoomManager` room paths still report
  the internal participant identity.
* the fact that a method returns *some* error at all (specific error values
  returned in edge cases may widen in a MINOR, they never narrow to `nil`)

`test/sdk/errors_test.go` pins this error set in both directions: a documented
sentinel that disappears stops compiling or drops out of `go doc`, a new
sentinel that is not added to the pinned list and to `docs/sdk.md` fails
`TestSDK_PublicSurface_GoDoc/errors/completeness`, and two frozen sentinels that
start sharing one value fail
`TestSDK_StableErrorSentinels/identity-classes` (`sharedIdentity` is empty as of
`0.7.0`, so there is no sanctioned aliasing left).

---

## 6. Unstable surface

These are exported (so they are discoverable and usable) but carry **no**
compatibility promise. Do not build long-lived code on them; treat any change
to them as expected churn:

* `RoomManager` (alias of `signaling.RoomManager`), `NewRoomManager`, and
  `Client.RoomManager()` — the room registry shape, its method set and its
  concurrency characteristics are still moving. `Client` methods are the
  supported way to manage rooms.
* `Handler` (alias of `signaling.Handler`), `NewHandler`, `Client.Handler()` —
  signalling internals; the supported entry point is `Client.Connect`. `Client.Handler()` is
  deprecated (use `HTTPHandler`, `Connect`, `RoomIDs`/`CloseRoom`). Test hooks
  `ResetMetrics`, `ResetGCReapedCount`, `ArmGhostForTest` and `ReapGhostForTest`
  are gated behind `//go:build slive_internal` and not reachable without the tag
  (TASK-036), so untagged consumers cannot break `forwarder_dropped_total` / `gc_reaped_total`
  monotonicity (`docs/sdk.md` §7).
* `PeerConnectionConfig` (alias of `webrtc.PeerConnectionConfig`) — pion-facing
  plumbing. Use `SDKConfig.STUNServers` instead.
* Everything under `internal/` (`config`, `domain`, `http`, `logger`,
  `signaling`, `webrtc`): unreachable to external consumers by Go's rules and
  explicitly unstable.
* `cmd/slive` flags and environment variable names (`SLIVE_*`) — the server
  binary's configuration is not the SDK contract.
* Unexported fields on every type, including `sync.RWMutex` fields and internal
  maps, which `go doc` already hides.

### Not covered by the contract at all

* **Error message strings.** Only sentinel identity via `errors.Is` is stable.
  Since `0.7.0` a room miss carries the room-owned `ErrRoomNotFound` (rendered
  `"room not found"`) and no longer matches `ErrParticipantNotFound`; DEF-01 is
  fixed, and the wording it left behind is still not a promise — a PATCH may
  reword any of these strings, so never branch on `err.Error()`.
* **Log output**: message text, level and field ordering. `slog` *event* names
  (`event=ghost_reaped`, `event=queue_dropped`, …) are operationally
  significant and are announced in `CHANGELOG.md`, but they are not a
  compatibility promise before 1.0.
* **Metrics values and timing**, which are inherently racy point-in-time
  observations.
* Anything about multi-node behaviour: Slive is single-node and the SDK is
  Go-only. There are no JS/TS bindings, no gRPC/protobuf API and no
  version-negotiation handshake on the signalling endpoint yet; adding any of
  those is a MINOR, and adding a required protocol field is breaking.

### Wire compatibility (signalling and health)

The WebSocket signalling message types documented in
[`docs/signaling-protocol.md`](docs/signaling-protocol.md) and the `GET
/healthz` JSON body are versioned with the module and follow the same rules:
additive message types and JSON keys are a MINOR; removing or renaming a
message type, or a `MetricsSnapshot` JSON key, is breaking. Unknown message
types **must** be ignored by the server (forward compatibility), and every
client-facing payload must remain decodable with unknown fields dropped.

---

## 7. Go and dependency compatibility

* **Go:** the module declares `go 1.24`. Slive supports the two most recent Go
  release lines. Dropping a Go version is a MINOR (MAJOR after 1.0) and is
  recorded in `CHANGELOG.md`.
* **pion/webrtc:** pinned to `v3` (`github.com/pion/webrtc/v3`). No exported
  `pkg/slive` **signature** mentions a pion type — parameters, results and
  receivers are stdlib or `pkg/slive` types — and that is the claim §7 rests
  on. The unstable `PeerConnectionConfig` alias *does* hold pion types in its
  fields, and [§6](#6-unstable-surface) already declares those fields
  PATCH-mutable, so pion field churn there is expected churn, not a break: the
  two statements are reconciled by the tier label, and pion can only reach a
  consumer through **U** names. Configure ICE with
  `SDKConfig.STUNServers []string`, which stays pion-free. Upgrading pion is
  therefore a PATCH unless it changes SDK-observable behaviour; a pion **major**
  bump would require an Slive MAJOR.
* **No new direct dependencies** in `pkg/slive` without a CHANGELOG note; the
  facade is deliberately stdlib + this module's own packages.

---

## 8. Release checklist

A release is not done until every step passes. All commands run from the
repository root with the repository-local module cache:

```bash
export GOMODCACHE="$PWD/.gocache/mod"

go doc -all ./pkg/slive | head -40          # surface is discoverable and documented
go test ./test/sdk/... -race -count=1       # surface + examples + error contract gates
go vet ./pkg/slive/... ./examples/...       # stdlib vet checks (the alias boundary in §3 is a review rule, not a vet check)
go build ./pkg/slive/... ./examples/...
gofmt -l pkg/slive examples test/sdk        # must print nothing
go test ./... -race -count=1                # full regression
git tag -a v0.7.0 -m "slive v0.7.0: stable pkg/slive SDK surface"
```

Then: `CHANGELOG.md` has a `## [0.7.0]` section with `Added` / `Changed` /
`Deprecated` / `Removed` / `Fixed` / `Breaking` headings, the stable list in
this file matches `docs/sdk.md`, and `reports/sprint-NN-architecture.md`
records anything the architect had to rule on.

---

## 9. Related documents

* [`docs/sdk.md`](docs/sdk.md) — the exported surface table this policy governs.
* [`pkg/slive/doc.go`](pkg/slive/doc.go) — the same contract, in godoc form.
* [`CHANGELOG.md`](CHANGELOG.md) — per-release history and migration snippets.
* [`examples/README.md`](examples/README.md) — runnable usage of the stable surface.
* [`docs/signaling-protocol.md`](docs/signaling-protocol.md) — the wire format.
* [`reports/sprint-07-architecture.md`](reports/sprint-07-architecture.md) — why
  `pkg/slive` is the facade and what was deliberately left internal.
