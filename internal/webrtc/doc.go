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
