// Package webrtc provides WebRTC abstractions for the Slive project.
// It wraps the Pion WebRTC library to provide thread-safe, domain-specific
// interfaces for managing peer connections, media tracks, session descriptions,
// and ICE candidates.
//
// The package is designed to be used by the core engine to enable real-time
// audio/video communication between participants in a room.
//
// Key abstractions:
//   - PeerConnection: Manages WebRTC peer connections with thread-safe operations.
//   - WebRTCTrack: Extends the domain Track model with WebRTC-specific functionality.
//   - SessionDescription: Handles SDP offers/answers for signaling.
//   - ICECandidate: Manages ICE candidate exchange for NAT traversal.
//
// All abstractions are thread-safe and can be safely used concurrently.
//
// # Connection lifecycle
//
// A PeerConnection starts in state New when created via NewPeerConnection and
// moves through Connecting/Connected as pion reports connection-state changes
// via OnConnectionStateChange. Disconnected marks a recoverable drop
// (NeedsReconnect reports true for Disconnected and Failed); Closed is
// terminal. Close is idempotent and safe to call from any goroutine; it stops
// internal RTCP pumps and closes the underlying pion peer connection.
//
// Operations attempted on a Closed or Failed connection return
// ErrPeerConnectionClosed instead of panicking or blocking.
//
// # Negotiation flow (SFU-style: the server answers on behalf of its peers)
//
// Slive's server owns one PeerConnection per participant. Signaling messages
// carry a target_participant_id and are applied to that participant's local
// PeerConnection by the signaling layer:
//
//   - An offer from client A targeted at B is applied to B's peer connection
//     with CreateAnswer. CreateAnswer first installs the remote offer
//     (SetRemoteDescription), then generates the answer, waits for ICE
//     gathering to complete so the SDP is complete, and returns it; the
//     signaling layer relays it back to A.
//   - An answer from client A targeted at B is installed on B's peer
//     connection via SetRemoteDescription.
//   - ICE candidates from any client are added to the target peer connection
//     with AddICECandidateWithRetry, which retries transient failures a
//     bounded number of times before giving up.
//
// Outbound events flow the opposite way: whenever pion fires
// negotiation-needed, or produces a local ICE candidate, the peer connection
// pushes a "webrtc:offer" / "webrtc:ice-candidate" message through the
// configured SignalingSender. The sender can be swapped at any time with
// UpdateSignalingSender — the signaling handler does this on every reconnect
// so events follow the newest WebSocket of a participant.
//
// Note that wrong-state SetRemoteDescription failures are protocol misuse by
// the remote side, not transient errors: they are surfaced directly and are
// deliberately not retried.
//
// # Event callbacks
//
// OnNegotiationNeeded, OnICECandidate and OnTrack register application
// callbacks that are stored on the peer connection and invoked from the pion
// event handlers:
//
//   - OnNegotiationNeeded callbacks run asynchronously so user code never
//     executes on pion's dispatch goroutine.
//   - OnICECandidate receives every gathered local candidate wrapped in an
//     ICECandidate (the trailing nil gathering-complete sentinel is not
//     forwarded).
//   - OnTrack receives the WebRTCTrack wrapper registered in the
//     remote-track registry; callbacks run outside the internal lock and may
//     re-enter PeerConnection methods.
//
// Registering a negotiation-needed callback does not disable the automatic
// offer push described above; both happen when a sender is configured.
//
// # Configuration injection points
//
// PeerConnectionConfig carries everything NewPeerConnection needs: ICE
// servers and SDP semantics. DefaultPeerConnectionConfig returns the
// production default (public STUN, unified plan with fallback). Applications
// build their own configuration without importing application-level config
// packages by using ICEServersFromURLs together with ICETurnServer, which map
// plain STUN URL lists and TURN credentials onto pion's ICEServer type. The
// signaling handler accepts a PeerConnectionConfig via WithPeerConnectionConfig
// and uses it for every peer connection it creates.
//
// For deterministic offline tests use a STUN-free PeerConnectionConfig (only
// SDPSemantics set) plus an AddTransceiverFromKind(recvonly audio) so offers
// contain a media section with ICE credentials.
//
// Example usage:
//
//	// Create a new peer connection
//	config := webrtc.DefaultPeerConnectionConfig()
//	pc, err := webrtc.NewPeerConnection(config, participant, nil)
//	if err != nil {
//	    // handle error
//	}
//	defer pc.Close()
//
//	// Add a local track
//	track := webrtc.NewWebRTCTrack(domainTrack, pionTrack, codec)
//	if err := pc.AddTrack(track); err != nil {
//	    // handle error
//	}
//
//	// Create an offer
//	offer, err := pc.CreateOffer()
//	if err != nil {
//	    // handle error
//	}
//
//	// Send offer via signaling channel
//	// ...
//
//	// Set remote answer
//	if err := pc.SetRemoteDescription(answer); err != nil {
//	    // handle error
//	}
package webrtc
