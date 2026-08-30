package webrtc

import (
	"context"
	"errors"
	"sync"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"
)

// subscriberEntry holds the per-subscriber forwarding state.
type subscriberEntry struct {
	pc        *PeerConnection
	pionTrack *webrtc.TrackLocalStaticRTP
	webTrack  *WebRTCTrack
}

// TrackForwarder fans RTP packets from a publisher's WebRTCTrack out to
// multiple subscriber PeerConnections.
//
// Each subscriber receives its own TrackLocalStaticRTP with the same codec
// as the publisher; Pion assigns a unique SSRC per PeerConnection binding
// during negotiation (Bind), so no manual SSRC management is required.
//
// Thread-safe: all public methods may be called concurrently.
type TrackForwarder struct {
	mu          sync.RWMutex
	publisher   *WebRTCTrack
	subscribers map[*PeerConnection]*subscriberEntry

	running     bool
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	lifecycleMu sync.Mutex
}

// NewTrackForwarder creates a forwarder for the given publisher track.
// publisher must be non-nil.
func NewTrackForwarder(publisher *WebRTCTrack) (*TrackForwarder, error) {
	if publisher == nil {
		return nil, errors.New("publisher track is nil")
	}
	return &TrackForwarder{
		publisher:   publisher,
		subscribers: make(map[*PeerConnection]*subscriberEntry),
	}, nil
}

// PublisherTrack returns the publisher track this forwarder was created for.
func (f *TrackForwarder) PublisherTrack() *WebRTCTrack {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.publisher
}

// UpdatePublisher swaps the publisher track and restarts the forwarding
// goroutine if the forwarder is running. It is used to replace a
// placeholder TrackLocal publisher (created eagerly in handlePublishTrack)
// with the real TrackRemote that arrives later via PeerConnection.OnTrack.
// The forwarder keeps its existing subscribers across the swap.
func (f *TrackForwarder) UpdatePublisher(publisher *WebRTCTrack) error {
	if publisher == nil {
		return ErrTrackNotReady
	}
	f.lifecycleMu.Lock()
	defer f.lifecycleMu.Unlock()

	f.mu.Lock()
	if f.publisher == publisher {
		f.mu.Unlock()
		return nil
	}
	wasRunning := f.running
	cancel := f.cancel
	f.mu.Unlock()

	if wasRunning {
		if cancel != nil {
			cancel()
		}
		f.wg.Wait()
	}

	f.mu.Lock()
	f.publisher = publisher
	if wasRunning {
		ctx, newCancel := context.WithCancel(context.Background())
		f.ctx = ctx
		f.cancel = newCancel
		// running stays true; relaunch run with new publisher
		f.wg.Add(1)
		go f.run(ctx)
	}
	f.mu.Unlock()
	return nil
}

// IsRunning reports whether the forwarding goroutine is active.
func (f *TrackForwarder) IsRunning() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.running
}

// SubscriberCount returns the number of subscribers currently registered.
func (f *TrackForwarder) SubscriberCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.subscribers)
}

// AddSubscriber registers pc as a destination for forwarded RTP.
// It creates a TrackLocalStaticRTP that mirrors the publisher's codec,
// wraps it in a WebRTCTrack, stores it, and adds it to pc via AddTrack.
//
// If pc is already subscribed the call is idempotent and returns nil.
// Returns an error if pc is nil or closed/failed.
func (f *TrackForwarder) AddSubscriber(pc *PeerConnection) error {
	if pc == nil {
		return errors.New("peer connection is nil")
	}
	// Quick state check without holding forwarder lock.
	if st := pc.State(); st == PeerConnectionStateClosed || st == PeerConnectionStateFailed {
		return ErrPeerConnectionClosed
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.subscribers[pc]; exists {
		return nil
	}

	codec := f.publisher.Codec()
	capability := codec.RTPCodecCapability
	// Fallback when codec was never negotiated (zero value): infer mime type from kind.
	if capability.MimeType == "" {
		switch f.publisher.Kind() {
		case 0: // audio (domain.TrackKindAudio == 0)
			capability.MimeType = webrtc.MimeTypeOpus
			if capability.ClockRate == 0 {
				capability.ClockRate = 48000
			}
			if capability.Channels == 0 {
				capability.Channels = 2
			}
		default:
			capability.MimeType = webrtc.MimeTypeVP8
			if capability.ClockRate == 0 {
				capability.ClockRate = 90000
			}
		}
		if capability.SDPFmtpLine == "" && codec.SDPFmtpLine != "" {
			capability.SDPFmtpLine = codec.SDPFmtpLine
		}
	}

	trackID := f.publisher.ID()
	streamID := trackID + "-forward"

	pionTrack, err := webrtc.NewTrackLocalStaticRTP(capability, trackID, streamID)
	if err != nil {
		return err
	}

	// Preserve full codec parameters on the wrapper so subscribers can inspect them.
	webTrack := NewWebRTCTrack(f.publisher.DomainTrack(), pionTrack, codec)

	entry := &subscriberEntry{
		pc:        pc,
		pionTrack: pionTrack,
		webTrack:  webTrack,
	}

	// Add to subscriber PeerConnection before storing, so failure does not pollute state.
	if err := pc.AddTrack(webTrack); err != nil {
		return err
	}

	f.subscribers[pc] = entry
	return nil
}

// RemoveSubscriber unregisters pc. It removes the forwarded track from the
// subscriber's PeerConnection when possible and deletes the entry.
// Returns ErrTrackNotFound if pc was not a subscriber.
func (f *TrackForwarder) RemoveSubscriber(pc *PeerConnection) error {
	if pc == nil {
		return errors.New("peer connection is nil")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.subscribers[pc]; !exists {
		return ErrTrackNotFound
	}

	// Best-effort removal from the subscriber PC; ignore closed/failed errors
	// because the PC may already be shutting down.
	trackID := f.publisher.ID()
	_ = pc.RemoveTrack(trackID)

	delete(f.subscribers, pc)
	return nil
}

// Start launches the forwarding goroutine. It is idempotent: calling Start
// on an already-running forwarder returns nil. Returns an error if the
// publisher track is nil.
func (f *TrackForwarder) Start() error {
	f.lifecycleMu.Lock()
	defer f.lifecycleMu.Unlock()

	f.mu.Lock()
	if f.publisher == nil {
		f.mu.Unlock()
		return ErrTrackNotReady
	}
	if f.running {
		f.mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	f.ctx = ctx
	f.cancel = cancel
	f.running = true
	f.wg.Add(1)
	go f.run(ctx)
	f.mu.Unlock()

	return nil
}

// Stop terminates the forwarding goroutine and removes the forwarded track
// from every subscriber. It is idempotent and safe to call concurrently
// with other methods.
func (f *TrackForwarder) Stop() error {
	f.lifecycleMu.Lock()
	defer f.lifecycleMu.Unlock()

	f.mu.Lock()
	if !f.running {
		f.mu.Unlock()
		return nil
	}
	cancel := f.cancel
	f.running = false
	// Snapshot subscribers for cleanup outside lock to avoid holding forwarder
	// mu while calling PeerConnection methods (lock hierarchy).
	subs := make([]*subscriberEntry, 0, len(f.subscribers))
	for _, e := range f.subscribers {
		subs = append(subs, e)
	}
	f.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	f.wg.Wait()

	// Remove forwarded tracks from subscribers; best-effort, ignore errors
	// from closed PCs.
	trackID := f.publisher.ID()
	for _, e := range subs {
		_ = e.pc.RemoveTrack(trackID)
	}

	return nil
}

// WriteRTP forwards a single RTP packet to every subscriber.
// It clones the packet per subscriber so Pion's per-binding SSRC/payload-type
// rewrite does not race. Slow subscribers do not block others: each write is
// dispatched in its own goroutine and failures are ignored (subscriber may be
// mid-negotiation or closed).
func (f *TrackForwarder) WriteRTP(pkt *rtp.Packet) error {
	if pkt == nil {
		return errors.New("nil RTP packet")
	}

	f.mu.RLock()
	if len(f.subscribers) == 0 {
		f.mu.RUnlock()
		return nil
	}
	entries := make([]*subscriberEntry, 0, len(f.subscribers))
	for _, e := range f.subscribers {
		entries = append(entries, e)
	}
	f.mu.RUnlock()

	for _, e := range entries {
		// Clone packet for each subscriber to avoid header mutation races
		// inside pion's WriteRTP (which rewrites SSRC/PayloadType per binding).
		clone := *pkt
		// Deep-copy header extensions and payload slice to avoid sharing.
		if pkt.Payload != nil {
			payloadCopy := make([]byte, len(pkt.Payload))
			copy(payloadCopy, pkt.Payload)
			clone.Payload = payloadCopy
		}
		// Extensions are rare but copy if present.
		if len(pkt.Header.Extensions) > 0 {
			clone.Header.Extensions = append([]rtp.Extension(nil), pkt.Header.Extensions...)
		}
		entry := e
		pktCopy := &clone
		go func() {
			_ = entry.pionTrack.WriteRTP(pktCopy)
		}()
	}
	return nil
}

// Write forwards a raw RTP buffer to every subscriber by unmarshaling it
// into a packet and calling WriteRTP. See WriteRTP for backpressure semantics.
func (f *TrackForwarder) Write(b []byte) (int, error) {
	pkt := &rtp.Packet{}
	if err := pkt.Unmarshal(b); err != nil {
		return 0, err
	}
	if err := f.WriteRTP(pkt); err != nil {
		return 0, err
	}
	return len(b), nil
}

// ForwardRTPPacket is an alias for WriteRTP kept for readability in tests
// and for callers that already hold a parsed packet.
func (f *TrackForwarder) ForwardRTPPacket(pkt *rtp.Packet) error {
	return f.WriteRTP(pkt)
}

// run is the forwarding loop for remote publisher tracks. It reads RTP
// packets from the publisher (TrackRemote) and fans them out via WriteRTP.
// For local publisher tracks (TrackLocalStaticRTP) Read always returns
// ErrTrackNotReady, so the loop exits quickly and forwarding proceeds via
// the explicit WriteRTP path instead.
func (f *TrackForwarder) run(ctx context.Context) {
	defer f.wg.Done()

	buf := make([]byte, 1500)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Perform Read in a helper goroutine so context cancellation can
		// unblock the loop even while TrackRemote.Read is blocked waiting
		// for the next packet.
		type result struct {
			n   int
			err error
		}
		// Snapshot publisher under RLock so UpdatePublisher can swap
		// concurrently without data race; the run loop will be restarted
		// with the new publisher when a swap occurs.
		f.mu.RLock()
		pub := f.publisher
		f.mu.RUnlock()
		if pub == nil {
			return
		}
		ch := make(chan result, 1)
		go func() {
			n, err := pub.Read(buf)
			ch <- result{n, err}
		}()

		select {
		case <-ctx.Done():
			return
		case res := <-ch:
			if res.err != nil {
				// Track closed, EOF, or local track (not readable): stop loop.
				// For TrackLocal publishers this is expected; forwarding
				// continues via WriteRTP directly.
				return
			}
			pkt := &rtp.Packet{}
			if err := pkt.Unmarshal(buf[:res.n]); err != nil {
				continue
			}
			_ = f.WriteRTP(pkt)
		}
	}
}
