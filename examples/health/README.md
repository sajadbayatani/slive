# health

Serve the SDK-composed router (`Client.HTTPHandler()`, i.e. the real
`internal/http` health handler + signaling mount) on an in-process
`httptest` server and poll `/healthz` three times:

```sh
# from the repository root
export GOMODCACHE="$PWD/.gocache/mod"
go run ./examples/health
```

Expects 3 lines like
`health check N: status=ok uptime_seconds=... goroutines=... rooms_active=1`,
exit 0.

APIs used: `Client.HTTPHandler`, `Client.JoinRoom`, `Client.PublishTrack`, and
the `/healthz` body — the eleven `MetricsSnapshot` JSON keys flattened together
with a separate `status` field (12 keys total). See the
[SDK reference](../../docs/sdk.md#7-observability).

Run in CI by `test/sdk/TestSDK_ExamplesRun/health`.
