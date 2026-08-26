package webrtc

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// mediaTestBudget bounds every wait in this file; the waits poll instead of
// sleeping so success is observed as early as possible.
const mediaTestBudget = 20 * time.Second

// waitForCondition polls cond until it holds or the budget expires.
func waitForCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(mediaTestBudget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", mediaTestBudget, what)
}

// minimalRTPPacket builds a 12-byte RTP header plus payload by hand. Pion
// rewrites SSRC and payload type from the bound transport, so only the fixed
// fields and incrementing sequence/timestamp matter.
func minimalRTPPacket(seq uint16, timestamp uint32) []byte {
	pkt := make([]byte, 12+160)
	pkt[0] = 0x80 // version 2, no padding, no extension, 0 CSRCs
	pkt[1] = 111  // dynamic opus payload type (rewritten by pion on write)
	binary.BigEndian.PutUint16(pkt[2:4], seq)
	binary.BigEndian.PutUint32(pkt[4:8], timestamp)
	binary.BigEndian.PutUint32(pkt[8:12], 0xDEADBEEF)
	return pkt
}

// TestPeerConnectionLoopbackMediaFlow drives real end-to-end media through
// two peer connections inside the process: A publishes an opus track, B
// registers OnTrack BEFORE negotiation, SDP is exchanged (both sides gather
// host ICE candidates because the config is STUN-free), and an RTP writer
// pumps packets that must surface on B's remote track within a bounded
// window. This pins the SFU-style inbound path the signaling layer relies on.
func TestPeerConnectionLoopbackMediaFlow(t *testing.T) {
	sender := newNegotiationTestPeerConnection(t, "media-sender", "Alice")
	receiver := newNegotiationTestPeerConnection(t, "media-receiver", "Bob")

	domainTrack, err := domain.NewTrack("media-track", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("domain.NewTrack: %v", err)
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"media-track",
		"webrtc-audio",
	)
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}
	if err := sender.AddTrack(NewWebRTCTrack(domainTrack, pionTrack, webrtc.RTPCodecParameters{})); err != nil {
		t.Fatalf("sender.AddTrack: %v", err)
	}

	var packets atomic.Int64
	onTrackFired := make(chan *WebRTCTrack, 1)

	// Register OnTrack BEFORE any negotiation, matching the signaling
	// handler's wiring order.
	receiver.OnTrack(func(remote *WebRTCTrack) {
		select {
		case onTrackFired <- remote:
		default:
		}
		go func() {
			buf := make([]byte, 1500)
			for {
				if _, err := remote.Read(buf); err != nil {
					return // peer connection closed
				}
				packets.Add(1)
			}
		}()
	})

	// Full SDP exchange; CreateAnswer installs the offer as B's remote
	// description, mirroring the server-side answering flow.
	offer, err := sender.CreateOffer()
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	answer, err := receiver.CreateAnswer(offer)
	if err != nil {
		t.Fatalf("CreateAnswer: %v", err)
	}
	if err := sender.SetRemoteDescription(answer); err != nil {
		t.Fatalf("SetRemoteDescription(answer): %v", err)
	}

	// Pump RTP from the sender; writes before the DTLS association completes
	// are dropped silently by pion (no bindings yet).
	stop := make(chan struct{})
	go func() {
		seq := uint16(1)
		ts := uint32(90000)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_, _ = pionTrack.Write(minimalRTPPacket(seq, ts))
				seq++
				ts += 160
			}
		}
	}()

	var remote *WebRTCTrack
	waitForCondition(t, "OnTrack callback", func() bool {
		select {
		case remote = <-onTrackFired:
			return true
		default:
			return false
		}
	})
	if remote == nil || remote.ID() != "media-track" {
		t.Fatalf("OnTrack delivered %v, want the media-track wrapper", remote)
	}

	const wantPackets = 20
	waitForCondition(t, "at least 20 RTP packets", func() bool {
		return packets.Load() >= wantPackets
	})
	if got := receiver.GetRemoteTrack("media-track"); got != remote {
		t.Errorf("GetRemoteTrack = %v, want the OnTrack-delivered instance", got)
	}

	close(stop)
}

// TestOperationsOnClosedPCReturnErrPeerConnectionClosed completes the
// closed-state contract alongside TestOperationsOnFailedPCReturnErrPeerConnectionClosed:
// every media operation on a Closed peer connection must fail with the
// ErrPeerConnectionClosed sentinel so callers (signaling) can map codes.
func TestOperationsOnClosedPCReturnErrPeerConnectionClosed(t *testing.T) {
	pc := newNegotiationTestPeerConnection(t, "closed-pc", "Cleo")

	domainTrack, err := domain.NewTrack("closed-track", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("domain.NewTrack: %v", err)
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"closed-track",
		"webrtc-audio",
	)
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}
	wrapped := NewWebRTCTrack(domainTrack, pionTrack, webrtc.RTPCodecParameters{})

	candidate, err := NewICECandidateFromString(
		"candidate:842163049 1 udp 1677729535 192.0.2.1 31102 typ srflx")
	if err != nil {
		t.Fatalf("NewICECandidateFromString: %v", err)
	}

	if err := pc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cases := []struct {
		name string
		op   func() error
	}{
		{"CreateOffer", func() error { _, err := pc.CreateOffer(); return err }},
		{"CreateAnswer", func() error { _, err := pc.CreateAnswer(nil); return err }},
		{"SetRemoteDescription", func() error { return pc.SetRemoteDescription(&SessionDescription{}) }},
		{"AddICECandidate", func() error { return pc.AddICECandidate(candidate) }},
		{"AddTrack", func() error { return pc.AddTrack(wrapped) }},
		{"RemoveTrack", func() error { return pc.RemoveTrack("closed-track") }},
	}
	for _, tc := range cases {
		if err := tc.op(); !errors.Is(err, ErrPeerConnectionClosed) {
			t.Errorf("%s on closed PC: err = %v, want ErrPeerConnectionClosed", tc.name, err)
		}
	}
}

// TestPeerConnectionRemoveTrackUnknown pins the not-found branch of
// RemoveTrack before any close/failure state can mask it.
func TestPeerConnectionRemoveTrackUnknown(t *testing.T) {
	pc := newNegotiationTestPeerConnection(t, "remove-pc", "Rhea")

	if err := pc.RemoveTrack("never-added"); !errors.Is(err, ErrTrackNotFound) {
		t.Errorf("RemoveTrack(unknown) err = %v, want ErrTrackNotFound", err)
	}

	// Adding then removing twice exercises both branches on live state.
	domainTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("domain.NewTrack: %v", err)
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio-1",
		"webrtc-audio",
	)
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}
	if err := pc.AddTrack(NewWebRTCTrack(domainTrack, pionTrack, webrtc.RTPCodecParameters{})); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}
	if err := pc.RemoveTrack("audio-1"); err != nil {
		t.Fatalf("first RemoveTrack: %v", err)
	}
	if err := pc.RemoveTrack("audio-1"); !errors.Is(err, ErrTrackNotFound) {
		t.Errorf("second RemoveTrack err = %v, want ErrTrackNotFound", err)
	}
}
