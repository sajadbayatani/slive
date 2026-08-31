# publish-subscribe

Publish an audio track and subscribe through real WebSocket signaling
sessions (`slive.Client.Connect`), so the subscriber is registered on the
Handler `TrackForwarder`:

```sh
# from the repository root
export GOMODCACHE="$PWD/.gocache/mod"
go run ./examples/publish-subscribe
```

Expects `forwarder_subscribers: 1` and a monotonic
`forwarder_dropped_total`, exit 0, STUN-free (no external network).

APIs used: `Client.Connect`, `Session.PublishTrack`, `Session.SubscribeTrack`,
`Session.Close`, `Client.Snapshot`, `TrackKindAudio`,
`TrackSourceMicrophone` — see the
[SDK reference](../../docs/sdk.md#4-session--signaling-client).

Note on media: the synthetic `WriteRTP` burst is not run from the SDK because
`TrackForwarder` stays internal on the TASK-031 frozen surface. The forwarder
here is backed by a placeholder local track, so its dropped counter stays at 0;
the example asserts that it never decreases. See
[examples/README.md](../README.md#not-exercised-here-the-sfu-writertp-burst).

Run in CI by `test/sdk/TestSDK_ExamplesRun/publish-subscribe`.
