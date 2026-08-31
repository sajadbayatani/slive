// Package slive is the stable public Go API for Slive, a Go infrastructure
// SDK for building real-time audio/video applications (meetings, calls,
// webinars, classrooms, telemedicine, and similar products).
//
// Slive is infrastructure, not an end-user application. It has no frontend
// and remains independently testable. This package is the SemVer-stable
// facade; internal/* remains unstable and may change without notice. Do not
// import internal/domain, internal/signaling, internal/webrtc, or
// internal/config directly from outside this repository — import
// github.com/sajadbayatani/slive/pkg/slive instead.
//
// # Package path
//
// The stable import path is github.com/sajadbayatani/slive/pkg/slive
// (preferred over api/). It re-exports minimal, documented surface via type
// aliases and thin wrappers that import internal packages. The dependency
// direction is pkg/slive -> internal/*, never the reverse, so no import cycle
// is introduced and existing internal tests stay green.
//
// # Platform support
//
// The SDK is Go-only and single-node in this release: one Client owns one
// room registry and one signaling handler inside one process. There are no
// JS/TS bindings, no REST management API and no multi-instance room
// federation; non-Go clients speak the WebSocket protocol documented in
// docs/signaling-protocol.md directly. There is no version negotiation on the
// wire, so client and server must be upgraded together until 1.0.
//
// # What is stable
//
//   - Config shape SDKConfig and DefaultSDKConfig (GCParticipantTTL mirrors
//     config.DefaultGCParticipantTTL, QueueSize mirrors webrtc.DefaultQueueSize
//     64 — the shape is stable, the knob is reserved: it is normalized and
//     recorded but not yet applied to forwarders).
//   - Lifecycle types Room, Participant, Track, TrackKind, TrackSource and
//     their accessor methods (ID, Kind, State, PublishTrack, SubscribeTrack,
//     etc.). sync.RWMutex fields are not exported.
//   - Client with NewClient, JoinRoom, LeaveRoom, PublishTrack, SubscribeTrack,
//     UnsubscribeTrack, Snapshot, Close.
//   - SDK helpers (TASK-032): Client.Connect returning a signaling Session
//     (PublishTrack, SubscribeTrack, Close) that runs the real WebSocket
//     protocol against the Handler, Client.HTTPHandler composing the
//     production router (/health, /healthz, /ws), and Client.SignalingURL
//     (an in-process net/http server bound to 127.0.0.1 — the SDK does not
//     link net/http/httptest). A Session round-trip always runs against a
//     positive deadline and checks the server's request_type tag before
//     attributing an error frame, and a signaling failure wraps the frozen
//     sentinel it identifies, so errors.Is(sessionErr,
//     slive.ErrTrackAlreadyPublished) matches.
//   - Observability types ForwarderConfig (QueueSize), MetricsSnapshot (whose
//     JSON keys are the /healthz payload), the DiagnosticsSnapshoter snapshot
//     interface, and Handler/HandlerOption helpers WithGCTTL,
//     WithMetricsSnapshot, WithDiagnosticsSnapshoter.
//   - Error sentinels ErrRoomClosed, ErrTrackNotFound, ErrParticipantNotFound,
//     etc. as var ErrXXX = domain.ErrXXX with errors.Is support.
//     ErrRoomNotFound and ErrRoomAlreadyExists are the exceptions: they are
//     declared with errors.New in this package, so a room miss matches only the
//     room sentinel and never a participant one. Room-level paths that still
//     report the participant errors — signaling.RoomManager CreateRoom and
//     CloseRoom via the unstable RoomManager alias — keep that identity.
//
// New error sentinels will be added as var ErrNew = errors.New(...) with
// errors.Is support; removal or rename of an existing sentinel is a breaking
// change and will be announced with a MAJOR bump and at least one MINOR of
// deprecation via // Deprecated.
//
// # Exported but not stable
//
// Three symbols are exported so advanced callers can reach them, yet carry no
// compatibility promise because they alias internal types that are still
// moving: RoomManager (with NewRoomManager and Client.RoomManager), Handler
// (with NewHandler and Client.Handler), and PeerConnectionConfig. Use the
// Client methods, Client.Connect and SDKConfig instead; these names may change
// shape in any release, including a patch.
//
// # What is not exported
//
// Internal HTTP handlers, sync.RWMutex fields, and internal/http server wiring
// are not re-exported. MetricsSnapshot is the observable shape; health HTTP
// handling stays in internal/http.
//
// The SFU TrackForwarder and its WriteRTP method are deliberately not
// exported, so no symbol in this package can inject a synthetic RTP burst;
// that stays covered by internal/webrtc and test/scale tests. Examples assert
// MetricsSnapshot.ForwarderSubscribers and the monotonic
// ForwarderDroppedTotal counter instead. Exporting a media handle is a future
// MINOR decision, not an omission.
//
// Note that domain-only calls (Client.PublishTrack, Client.SubscribeTrack)
// register room state but create no forwarder; a non-zero ForwarderSubscribers
// requires the signaling path through Client.Connect.
//
// # Usage
//
//	cfg := slive.DefaultSDKConfig()
//	cfg.STUNServers = []string{} // STUN-free: no external network
//	client, err := slive.NewClient(cfg)
//	if err != nil { log.Fatal(err) }
//	defer client.Close()
//
//	room, err := client.JoinRoom(ctx, "room-001", "alice")
//	if err != nil { log.Fatal(err) }
//	track, err := client.PublishTrack(ctx, room.ID(), "alice", "mic-001",
//		slive.TrackKindAudio, slive.TrackSourceMicrophone)
//	if err != nil { log.Fatal(err) }
//
// For the signaling/SFU path, replace the two calls above with
// session, err := client.Connect(ctx, "room-001", "alice") followed by
// session.PublishTrack(ctx, "mic-001", slive.TrackKindAudio,
// slive.TrackSourceMicrophone).
//
// Runnable examples live under examples/ (basic-room, publish-subscribe,
// health) and each prints the expected output in its own README.
//
// # Related documents
//
// README.md (SDK quick start), docs/sdk.md (full exported surface table),
// VERSIONING.md (SemVer rules, deprecation policy, stable vs unstable) and
// CHANGELOG.md (released versions; this surface shipped in 0.7.0).
package slive
