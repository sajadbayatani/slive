# Changelog

All notable changes to Slive are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) as
interpreted for a pre-1.0 module by [`VERSIONING.md`](VERSIONING.md): the
**compatibility promise covers `pkg/slive` only**, `internal/*` may change in
any release, and while the major version is `0` a MINOR may break with a
`Breaking` section and migration snippets.

Versions track sprints: `0.7.x` is sprint-07 (SDK/API maturity), `0.6.x` is
sprint-06 (single-node scale), and so on.

No tag exists in the repository yet, so `0.7.0` is the first version to be
tagged; the `0.6.0` and earlier entries below are retroactive history seeded
from the sprint reports and `state/STATE.yaml`, not previously published
releases.

## [Unreleased]

### Added

* **SFU config plumbing (TASK-034, sprint-08):** `SDKConfig.QueueSize` is now
  applied to every `TrackForwarder` via `signaling.WithForwarderConfig`
  (`ForwarderConfig{QueueSize: N}`); zero value keeps `DefaultQueueSize` (64).
  `pkg/slive.WithForwarderConfig` is also exported for advanced `NewHandler`
  wiring. Previously the value was normalized and recorded but forwarder queues
  stayed at 64; that "reserved, not applied" wording is removed from
  `pkg/slive/config.go`, `pkg/slive/doc.go` and `docs/sdk.md` §5/§7/§10.
* **Wire error codes (TASK-034):** `internal/signaling` now emits distinct
  codes `track_already_published`, `track_already_subscribed`,
  `track_not_published`, `participant_already_exists`, `participant_left`,
  `invalid_track_kind`, `invalid_track_source` (in addition to the existing
  `room_closed`, `participant_not_found`, `track_not_found`, …); collapsed
  `internal_error` mapping is eliminated for every client-visible domain error.
  `pkg/slive.sentinelForErrorReply` matches codes first, with text fallback
  retained as legacy belt-and-braces.
* **N-1 sentinels:** `ErrClientClosed` and `ErrInvalidArgument` (`errors.New`
  in `pkg/slive/errors.go`) now wrap the closed-client and missing-params
  errors from `Client.JoinRoom`, `Client.SignalingURL` and `Client.Connect`,
  so `errors.Is(err, slive.ErrClientClosed)` / `ErrInvalidArgument` hold.
* **WebSocket origin policy (TASK-035, validation #6):** `signaling.WithAllowedOrigins([]string)`,
  `SLIVE_WS_ALLOWED_ORIGINS` (comma-separated, `internal/config`) and
  `SDKConfig.AllowedOrigins` (additive, wired via `pkg/slive` to `NewHandler`)
  implement D1: no `Origin` header → allow; `Origin` host equal to request `Host` → allow;
  exact match against allowlist → allow; else 403. Previously `connection.go:49` returned `true` unconditionally.
* **WebSocket deadlines/keepalive (TASK-035, validation #8):** read deadline `WSReadTimeout`
  default 60s (refreshed on pong + data), ping interval `WSPingInterval` default 30s
  (enforced ≤ `ReadTimeout`/2), write deadline `WSWriteTimeout` default 10s.
  `WSReadTimeout`/`WSPingInterval`/`WSWriteTimeout` configurable via env
  `SLIVE_WS_READ_TIMEOUT` / `SLIVE_WS_PING_INTERVAL` / `SLIVE_WS_WRITE_TIMEOUT`
  and handler options `WithWSReadTimeout` / `WithWSPingInterval` / `WithWSWriteTimeout`
  (also re-exported as `pkg/slive.WithWS*`). Dead peer is reaped within `WSReadTimeout`;
  healthy peer survives via ping/pong. Existing ghost-GC path unchanged.
* **SDK lifecycle completion (TASK-036):** `Client.RoomIDs() []string` (sorted snapshot)
  and `Client.CloseRoom(ctx, roomID) error` (canonical teardown: per-participant Leave,
  forwarder stops, manager removal; `ErrRoomNotFound` for unknown, idempotent close
  returns nil). `RoomManager.RoomIDs()`/`CloseRoom()` already existed in
  `internal/signaling` (mutex-protected map ops) and `Handler.CloseRoom` now wraps them
  with SFU cleanup (peer connections, forwarders, ghost timers).
* **Test-hook gating (TASK-036, B-3):** exported hooks `ReapGhostForTest`, `ArmGhostForTest`,
  `ResetMetrics`, `ResetGCReapedCount` moved to `internal/signaling/scale_export_sliveinternal.go`
  with `//go:build slive_internal`. Unexported `reapGhost`/`armGhostTimer`/`resetMetrics`
  remain tagless for in-package tests. `Client.Handler()` now carries `// Deprecated:` pointing to
  `HTTPHandler`/`Connect`/`RoomIDs`/`CloseRoom`.

### Changed

* `docs/sdk.md` error table and §5/§7 updated to document the new codes and
  the effective `QueueSize` knob.
* `docs/sdk.md` §5 config table gains `AllowedOrigins` row; §8 gains
  `WithAllowedOrigins` / `WithWSReadTimeout` / `WithWSPingInterval` / `WithWSWriteTimeout` rows.
  Defaults keep existing tests green (in-process clients send no `Origin`).
* `docs/sdk.md` §3 adds `RoomIDs`/`CloseRoom` (S) rows and deprecates `Handler()`; §7
  monotonicity caveat updated to gated hooks. `VERSIONING.md` §5 adds `RoomIDs`/`CloseRoom`
  to stable surface; §6 notes hooks gated. `pkg/slive/doc.go` stable list updated.

Nothing else staged. Open items carried into the next sprint are listed under
each release's **Known issues**.

---

## [0.7.0] — 2026-08-31 (sprint-07: SDK and API maturity)

Freezes the first stable public Go surface for Slive and documents the
compatibility contract around it. No `internal/*` behavior changed in this
release: `pkg/slive` is a facade over the sprint-05/06 implementation.

### Added

* **`pkg/slive` — the stable Go facade** (`github.com/sajadbayatani/slive/pkg/slive`),
  SemVer-governed per [`VERSIONING.md`](VERSIONING.md):
  * `Client` with `NewClient(SDKConfig)`, `JoinRoom`, `LeaveRoom`,
    `PublishTrack`, `SubscribeTrack`, `UnsubscribeTrack`, `Snapshot`, `Close`
    (concurrency-safe; `Close` idempotent).
  * Lifecycle type aliases `Room`, `Participant`, `Track`, `TrackKind`,
    `TrackSource`, `RoomState`, `ParticipantState`, `TrackState` with their
    constants (`TrackKindAudio`, `TrackSourceMicrophone`, …).
  * Configuration shape `SDKConfig` (alias `Config`) and `DefaultSDKConfig()`,
    mirroring `config.DefaultGCParticipantTTL` (60s) and
    `webrtc.DefaultQueueSize` (64) so consumers never import `internal/*` to
    discover a default. `SDKConfig.STUNServers` accepts an empty slice to force
    STUN-free, offline ICE.
  * Observability aliases `MetricsSnapshot` (11 fields + JSON keys),
    `ForwarderConfig`, `DefaultQueueSize`, `PeerConnectionConfig`, and the
    `DiagnosticsSnapshoter` interface.
  * Handler options `HandlerOption`, `WithGCTTL`, `WithMetricsSnapshot`,
    `WithDiagnosticsSnapshoter`, plus `NewRoomManager` / `NewHandler` wrappers.
    `RoomManager` and `Handler` are exported but **explicitly unstable**.
  * Eighteen error sentinels matched by `errors.Is`, including the new
    `ErrSessionClosed`.
* **SDK signaling helpers** so the real SFU path is reachable from outside
  `internal/*`:
  * `Client.Connect(ctx, roomID, participantID)` returning a `*Session` that
    runs the documented WebSocket protocol (auto-join `room_joined` handshake,
    `PublishTrack`, `SubscribeTrack`, `Close`) against the client's handler —
    this is what registers a subscriber on a `TrackForwarder`.
  * `Client.HTTPHandler()`, composing the production router (`/health`,
    `/healthz`, `/ws`) without importing `internal/http`.
  * `Client.SignalingURL()`, lazily starting the in-process signaling server
    that sessions dial through.
* **Three runnable examples**, each STUN-free, exiting 0 in under 5 seconds
  (see [`examples/README.md`](examples/README.md)):
  * [`examples/basic-room`](examples/basic-room) — 1 room × 2 participants,
    logs `rooms_active` / `participants_active` from `Snapshot()`.
  * [`examples/publish-subscribe`](examples/publish-subscribe) — publisher and
    subscriber over real signaling sessions; asserts
    `forwarder_subscribers >= 1` and that `forwarder_dropped_total` is
    monotonic.
  * [`examples/health`](examples/health) — serves `Client.HTTPHandler()` on an
    `httptest` server and asserts `status=ok` from `GET /healthz` three times.
* **Documentation:** the Go SDK section in
  [`README.md`](README.md) (install, minimal snippet, example table with
  copy-pasteable `go run` commands), [`docs/sdk.md`](docs/sdk.md) (exported
  surface tables, error semantics, known defects),
  [`VERSIONING.md`](VERSIONING.md) (SemVer, deprecation, stable vs unstable)
  and this file; `pkg/slive/doc.go` states the package contract in godoc.
* **`test/sdk`** — integration tests that gate the public surface from the
  outside, using the toolchain rather than `internal` imports:
  * `TestSDK_PublicSurface_GoDoc` — runs `go doc -all ./pkg/slive` and asserts
    every pinned type, function and constant entry of the frozen surface is
    present **and documented** (plus the full `Client`/`Session` method sets),
    that every exported `Err…` sentinel is accounted for in both directions,
    and that the `README.md` snippet still references real symbols
    (doc-drift guard).
  * `TestSDK_ExamplesRun` — `go run ./examples/basic-room`,
    `./examples/publish-subscribe` and `./examples/health` must exit 0 and
    print `rooms_active`, `forwarder_subscribers` and `status=ok`; 45s cap per
    command, no network.
  * `TestSDK_StableErrorSentinels` — sentinel identity is matchable through a
    `%w` chain and the set of sentinels sharing one underlying value is exactly
    the documented one (empty as of this release; see **Fixed**).
  * `TestSDK_RoomSentinelIdentity` / `TestSDK_JoinRoomDuplicateIsIdempotent` —
    the DEF-01 split as a consumer sees it: every room-level `Client` miss
    matches `ErrRoomNotFound` and no participant sentinel, every participant
    miss matches `ErrParticipantNotFound` and not `ErrRoomNotFound`, and a
    duplicate `JoinRoom` is an idempotent success.
  * README gate — the SDK snippet must stay a complete program inside
    sprint-07's 20-line budget and keep calling
    `NewClient` → `JoinRoom` → `PublishTrack` → `SubscribeTrack` → `Snapshot` →
    `LeaveRoom` → `Close`.

### Changed

* Behaviour-neutral: `internal/*` was untouched. `README.md` project structure
  now lists `pkg/`, `examples/` and `test/` and flags `internal/*` as unstable.
* `Client.SignalingURL()` now starts a plain `net/http` server on a
  `127.0.0.1:0` listener instead of `net/http/httptest`, so no test-only server
  semantics sit in a consumer's binary. The
  `SignalingURL() (string, error)` signature and its loopback-only contract are
  unchanged.
* The `README.md` minimal example is now a complete program inside sprint-07's
  20-line budget and walks the full required flow including `LeaveRoom`, which
  previously had no demonstrated call site anywhere (gap G-2).
* A room miss now renders `"room not found"` instead of inheriting
  `"participant not found in the room"`. Message strings are explicitly **not**
  part of the contract (`VERSIONING.md` §6) — match sentinels with `errors.Is`;
  the text is mentioned only because DEF-01's symptom was user-visible.

### Fixed

* **DEF-01 — room error sentinels no longer alias participant sentinels.**
  `ErrRoomNotFound` and `ErrRoomAlreadyExists` are now `errors.New` values owned
  by `pkg/slive`, so `errors.Is(err, slive.ErrRoomNotFound)` on a missing room
  is true while `errors.Is(err, slive.ErrParticipantNotFound)` is false. All
  four room-level `Client` methods (`LeaveRoom`, `PublishTrack`,
  `SubscribeTrack`, `UnsubscribeTrack`) follow the split, and the
  `Client.RoomManager().GetRoom(id) == nil` probe is no longer needed to tell a
  room miss from a participant miss. Reproduce the old behaviour:
  `Client.LeaveRoom(ctx, "no-such-room", "alice")` used to match
  `ErrParticipantNotFound`. Fixed before the first tag, per the review's R2
  ruling, so nothing published ever carried the wrong identity.
  **Deliberate limit:** the fix lives in `pkg/slive`, so the **unstable**
  `RoomManager` alias keeps its internal identity — `CreateRoom` on an existing
  room and `CloseRoom` on an unknown one still report the *participant* errors,
  which also means `ErrRoomAlreadyExists` is currently frozen-but-never-returned.
* **`Session` failures are sentinel-matchable** (review blocker B-2, the defect
  behind the `docs/sdk.md` promise). A server `error` frame now surfaces as an
  unexported `*sessionError` that keeps the wire text for humans and
  `Unwrap`s to the frozen `pkg/slive` sentinel the reply identifies: the wire
  `code` is authoritative (`room_closed`, `room_not_found`,
  `participant_not_found`, `track_not_found`, `peer_connection_closed`) and the
  message text is the fallback for the replies `internal/signaling` collapses
  into `internal_error` — notably `ErrTrackAlreadyPublished`. An unrecognized
  reply maps to no sentinel at all rather than to a wrong one.
* **`Client.JoinRoom` is idempotent under concurrency** (B-4). The
  get-or-create, existence probe and join now share one critical section, so a
  duplicate join — sequential or racing — returns the same `*Room` instead of
  losing with `ErrParticipantAlreadyExists`.
* **`Client.Close` can no longer be parked by a wedged `Session`** (B-5). Every
  socket operation runs against a positive deadline (the caller's context
  deadline when usable, otherwise a **5-second** default round-trip bound,
  floored at 100 ms for an expiring context), and teardown takes a separate
  transport lock, so closing the socket releases a blocked read instead of
  queueing behind it. Pass a deadline if 5s is too long for your call.
* **`Client.Connect` no longer returns an unusable session when the client
  closes mid-handshake.** It tears the transport down and fails with an
  `ErrSessionClosed`-wrapped error instead.
* **`SDKConfig.QueueSize` is documented as reserved** (B-1). The earlier wording
  claimed `NewClient` fed the value to forwarders; it does not — the value is
  normalized and recorded, and every `TrackForwarder` still runs with
  `DefaultQueueSize` until a signaling option can receive it (sprint-08).

### Breaking

None. `pkg/slive` is new in this release, so there was no prior contract to
break.

### Known issues

* **SFU `WriteRTP` burst is not exercisable from the SDK (confirmed deferral).**
  TASK-031 deliberately does not export the handler's
  `TrackForwarder`, so `WriteRTP` lives only on the unexported
  `internal/webrtc` type and no `pkg/slive` symbol can inject RTP. TASK-032's
  brief asked for a 10-packet synthetic burst; instead the examples assert
  `forwarder_subscribers >= 1` and monotonic `forwarder_dropped_total`, and the
  burst stays covered by `internal/webrtc` and `test/scale`. The sprint-07
  review (R3) ratified this trade-off: exporting the forwarder now would freeze
  a type whose locking and pooling shape just changed in sprint-06, and a
  server-side raw-RTP handle needs an authorization design. See `docs/sdk.md`
  §Known gaps.
* `pkg/slive.WithMetricsSnapshot` and `WithDiagnosticsSnapshoter` are no-ops on
  a `Handler` by design (health wiring belongs to the HTTP layer), and
  `SDKConfig.QueueSize` / `ForwarderConfig.QueueSize` are recorded but not
  applied to any forwarder (see **Fixed** and `docs/sdk.md` §5, §7, §10).
* `Client.Handler()` hands out the concrete signaling handler, whose exported
  method set still includes the metrics-reset and ghost-GC test hooks; calling
  them legitimately breaks the `forwarder_dropped_total` monotonicity that the
  examples assert. Documented in `docs/sdk.md` §7; gating those hooks behind a
  build tag is a sprint-08 item.

---

## [0.6.0] — 2026-08-31 (sprint-06: single-node scale and capacity)

Prior release; no public Go API existed. Included for continuity.

### Added

* `test/scale` deterministic capacity harness (`ScaleHarness`) over
  `RoomManager` + `Handler` with STUN-free ICE, `WithGCTTL(60s)` and
  `ForwarderConfig{QueueSize: 64}`; 100 rooms × 16 participants profile, RTP
  bursts at 60 packets/s for 30s, JSON baselines via `t.Logf` and
  `reports/scale-baseline.md`.
* Load regressions: room/forwarder fan-out, goroutine bound, ghost GC under
  load, `/healthz` scraping during and after bursts, concurrent
  `RoomManager`/`Handler` operations — all race-clean.

### Fixed

* Forwarder allocation hot path: `rtp.Packet` pooled via `sync.Pool` and a
  per-subscriber queue fast path that avoids the clone when the queue is full.
  Measured `alloc 751MB → 150MB (-80%)` and `dropped -36%` on the sprint-06
  baseline, with the `PeerConnection.mu > TrackForwarder.mu` lock hierarchy
  preserved.

---

## [0.5.0] — 2026-08-31 (sprint-05: observability foundation)

### Added

* `MetricsSnapshot` with counters and gauges (`connection_attempts_total`,
  `connection_failures_total`, `forwarder_subscribers`,
  `forwarder_dropped_total`, `forwarder_queue_depth`, `rooms_active`,
  `participants_active`, `tracks_published`, `gc_reaped_total`,
  `uptime_seconds`, `goroutines`), snapshotted lock-free for encoding.
* Structured `slog` lifecycle events (`peer_connected`, `peer_disconnected`,
  `track_available`, `forwarder_start`, `queue_dropped` rate-limited,
  `ghost_reaped`, …).
* `GET /healthz` diagnostics endpoint returning the snapshot as JSON (and
  `text/plain` for humans), wired through `DiagnosticsSnapshoter`.

---

## [0.4.0] — 2026-08-30 (sprint-04: reliability hardening)

### Added

* Bounded per-subscriber RTP forwarding queues (`ForwarderConfig.QueueSize`,
  default 64) with non-blocking drop accounting.
* Ghost-participant garbage collection: `signaling.WithGCTTL` (default 60s),
  arm-on-disconnect / cancel-on-reconnect timers, idempotent reap, and
  `Shutdown` for graceful stop.
* WebSocket-aware graceful shutdown seam (`ConnectionManager.CloseAll`).

### Fixed

* Domain subscribe deadlock; peer-connection test SDP; forwarder lock
  hierarchy and extension deep-copy issues surfaced by the sprint-03 review.

---

## [0.3.0] — 2026-08-30 (sprint-03: SFU media routing)

### Added

* `internal/webrtc.TrackForwarder`: per-subscriber writer goroutines,
  subscriber add/remove, publisher updates and lifecycle contexts.
* Signaling-to-SFU wiring: track publish/subscribe drives forwarder creation
  and remote-track swap, with ICE/offer/answer forwarding.

---

## [0.2.0] and [0.1.0] — 2026-08-25 (sprints 01–02: domain and signaling)

### Added

* Core domain model `Room` / `Participant` / `Track` with lifecycle state
  machines and mutex-protected operations.
* WebSocket signaling protocol (`join_room`, `publish_track`,
  `subscribe_track`, `offer`, `answer`, `ice_candidate`, `error`) and the HTTP
  router with `/health` and `/ws`.
* Configuration and structured logging infrastructure (`cmd/slive`).

[Unreleased]: #unreleased
[0.7.0]: #070--2026-08-31-sprint-07-sdk-and-api-maturity
[0.6.0]: #060--2026-08-31-sprint-06-single-node-scale-and-capacity
[0.5.0]: #050--2026-08-31-sprint-05-observability-foundation
[0.4.0]: #040--2026-08-30-sprint-04-reliability-hardening
[0.3.0]: #030--2026-08-30-sprint-03-sfu-media-routing
