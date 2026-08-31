# Slive examples

Runnable, single-process examples for the stable SDK facade
`github.com/sajadbayatani/slive/pkg/slive`. All examples are STUN-free
(no external network), exit 0, and finish in about 1-2 seconds once the Go
build cache is warm.

Run from the repository root. This repository keeps its module cache under
`.gocache/mod`, so export it first:

```sh
export GOMODCACHE="$PWD/.gocache/mod"
go run ./examples/basic-room          # expect: rooms_active: 1, participants_active: 2, exit 0
go run ./examples/publish-subscribe   # expect: forwarder_subscribers: 1, forwarder_dropped_total: 0 (monotonic), exit 0
go run ./examples/health              # expect: 3x health check N: status=ok with uptime_seconds and goroutines, exit 0
```

- `basic-room` — 1 room × 2 participants joined via `Client`, logs `Snapshot()` gauges.
- `publish-subscribe` — publisher and subscriber run the real WebSocket
  signaling protocol through `Client.Connect` sessions, so the subscriber is
  registered on the Handler `TrackForwarder` (`forwarder_subscribers >= 1`).
- `health` — serves `Client.HTTPHandler()` (the production internal/http
  router: `/health`, `/healthz`, `/ws`) on an `httptest` server and polls
  `GET /healthz` three times.

These three runs are also executed by `test/sdk/TestSDK_ExamplesRun`, which
fails if any of them stops exiting 0 or stops printing the lines above.

## Not exercised here: the SFU `WriteRTP` burst

The `TrackForwarder` type and its `WriteRTP` method are deliberately not
exported on the `0.7.0` frozen surface, so no `pkg/slive` symbol can inject a
synthetic RTP burst from outside `internal/*`. The burst stays covered by
`internal/webrtc` and `test/scale` tests; the examples verify subscriber
registration (`forwarder_subscribers`) and the monotonic
`forwarder_dropped_total` counter instead. This is an intentional
stability-over-convenience trade-off recorded for architect sign-off in
`CHANGELOG.md` and `docs/sdk.md` §Known gaps, not an oversight.

## See also

- [SDK reference](../docs/sdk.md) — every exported symbol and its stability tier.
- [Versioning policy](../VERSIONING.md) — what may and may not change.
- [Changelog](../CHANGELOG.md) — the surface shipped in `0.7.0`.
- [README §Go SDK](../README.md#go-sdk-pkgslive) — install and minimal snippet.
