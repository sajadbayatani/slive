# basic-room

Join 1 room with 2 participants via the SDK `Client` and log active rooms:

```sh
# from the repository root
export GOMODCACHE="$PWD/.gocache/mod"
go run ./examples/basic-room
```

Expects `rooms_active: 1` and `participants_active: 2`, exit 0, STUN-free
(`cfg.STUNServers = []string{}`, so no external network is touched).

APIs used: `DefaultSDKConfig`, `NewClient`, `Client.JoinRoom`,
`Client.Snapshot`, `Room.ID` — see the
[SDK reference](../../docs/sdk.md#3-client--the-entry-point).

Run in CI by `test/sdk/TestSDK_ExamplesRun/basic-room`.
