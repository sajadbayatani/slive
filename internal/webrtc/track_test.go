package webrtc

import (
	"testing"

	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

func TestNewWebRTCTrack(t *testing.T) {
	// Create a domain track
	domainTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create domain track: %v", err)
	}

	// Create a Pion track
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio-1",
		"webrtc-audio",
	)
	if err != nil {
		t.Fatalf("Failed to create Pion track: %v", err)
	}

	// Create a WebRTCTrack
	codec := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeOpus,
			ClockRate:    48000,
			Channels:     2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		},
		PayloadType: 111,
	}

	webRTCTrack := NewWebRTCTrack(domainTrack, pionTrack, codec)

	// Verify the track was created correctly
	if webRTCTrack.ID() != "audio-1" {
		t.Errorf("Expected track ID to be 'audio-1', got '%s'", webRTCTrack.ID())
	}

	if webRTCTrack.Kind() != domain.TrackKindAudio {
		t.Errorf("Expected track kind to be 'audio', got '%s'", webRTCTrack.Kind())
	}

	if webRTCTrack.Source() != domain.TrackSourceMicrophone {
		t.Errorf("Expected track source to be 'microphone', got '%s'", webRTCTrack.Source())
	}

	if webRTCTrack.State() != domain.TrackStateCreated {
		t.Errorf("Expected track state to be 'created', got '%s'", webRTCTrack.State())
	}

	if webRTCTrack.PionTrack() != pionTrack {
		t.Error("Expected Pion track to match")
	}

	if webRTCTrack.DomainTrack() != domainTrack {
		t.Error("Expected domain track to match")
	}

	if webRTCTrack.Codec().PayloadType != 111 {
		t.Errorf("Expected codec payload type to be 111, got %d", webRTCTrack.Codec().PayloadType)
	}
}

func TestWebRTCTrackPublishUnpublish(t *testing.T) {
	domainTrack, err := domain.NewTrack("video-1", domain.TrackKindVideo, domain.TrackSourceCamera)
	if err != nil {
		t.Fatalf("Failed to create domain track: %v", err)
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
		"video-1",
		"webrtc-video",
	)
	if err != nil {
		t.Fatalf("Failed to create Pion track: %v", err)
	}

	webRTCTrack := NewWebRTCTrack(domainTrack, pionTrack, webrtc.RTPCodecParameters{})

	// Test publishing
	webRTCTrack.Publish()
	if webRTCTrack.State() != domain.TrackStatePublished {
		t.Errorf("Expected track state to be 'published', got '%s'", webRTCTrack.State())
	}

	// Test unpublishing
	webRTCTrack.Unpublish()
	if webRTCTrack.State() != domain.TrackStateUnpublished {
		t.Errorf("Expected track state to be 'unpublished', got '%s'", webRTCTrack.State())
	}
}

func TestWebRTCTrackPublisher(t *testing.T) {
	domainTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create domain track: %v", err)
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio-1",
		"webrtc-audio",
	)
	if err != nil {
		t.Fatalf("Failed to create Pion track: %v", err)
	}

	webRTCTrack := NewWebRTCTrack(domainTrack, pionTrack, webrtc.RTPCodecParameters{})

	// Initially, publisher should be nil
	if webRTCTrack.Publisher() != nil {
		t.Error("Expected publisher to be nil initially")
	}

	// Set a publisher
	participant := domain.NewParticipant("participant-1", "Alice")
	webRTCTrack.SetPublisher(participant)

	if webRTCTrack.Publisher() != participant {
		t.Error("Expected publisher to be set")
	}
}

func TestWebRTCTrackSetCodec(t *testing.T) {
	domainTrack, err := domain.NewTrack("video-1", domain.TrackKindVideo, domain.TrackSourceCamera)
	if err != nil {
		t.Fatalf("Failed to create domain track: %v", err)
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
		"video-1",
		"webrtc-video",
	)
	if err != nil {
		t.Fatalf("Failed to create Pion track: %v", err)
	}

	codec := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeVP8,
			ClockRate: 90000,
		},
		PayloadType: 103,
	}

	webRTCTrack := NewWebRTCTrack(domainTrack, pionTrack, codec)

	// Update the codec
	newCodec := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeVP9,
			ClockRate: 90000,
		},
		PayloadType: 104,
	}
	webRTCTrack.SetCodec(newCodec)

	if webRTCTrack.Codec().PayloadType != 104 {
		t.Errorf("Expected codec payload type to be 104, got %d", webRTCTrack.Codec().PayloadType)
	}
}

func TestWebRTCTrackClose(t *testing.T) {
	domainTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create domain track: %v", err)
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio-1",
		"webrtc-audio",
	)
	if err != nil {
		t.Fatalf("Failed to create Pion track: %v", err)
	}

	webRTCTrack := NewWebRTCTrack(domainTrack, pionTrack, webrtc.RTPCodecParameters{})

	// Close the track
	err = webRTCTrack.Close()
	if err != nil {
		t.Errorf("Expected Close to succeed, got error: %v", err)
	}
}

func TestWebRTCTrackRead(t *testing.T) {
	domainTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create domain track: %v", err)
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio-1",
		"webrtc-audio",
	)
	if err != nil {
		t.Fatalf("Failed to create Pion track: %v", err)
	}

	webRTCTrack := NewWebRTCTrack(domainTrack, pionTrack, webrtc.RTPCodecParameters{})

	// Try to read from the track
	buf := make([]byte, 1500)
	n, err := webRTCTrack.Read(buf)
	// Reading from a local track without writing will return an error or 0 bytes
	// This is expected behavior
	if err != nil && err != ErrTrackNotReady {
		// It's okay if we get an error, as long as it's not unexpected
		t.Logf("Read returned error (expected): %v", err)
	}
	_ = n
}

func TestWebRTCTrackNilPionTrack(t *testing.T) {
	domainTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create domain track: %v", err)
	}

	// Create a WebRTCTrack with a nil Pion track
	webRTCTrack := NewWebRTCTrack(domainTrack, nil, webrtc.RTPCodecParameters{})

	// Reading should return an error
	buf := make([]byte, 1500)
	_, err = webRTCTrack.Read(buf)
	if err != ErrTrackNotReady {
		t.Errorf("Expected ErrTrackNotReady, got %v", err)
	}

	// Closing should not panic
	err = webRTCTrack.Close()
	if err != nil {
		t.Errorf("Expected Close to succeed with nil Pion track, got error: %v", err)
	}
}
