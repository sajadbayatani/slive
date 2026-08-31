package webrtc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"
)

// DefaultQueueSize is the default per-subscriber RTP queue capacity.
const DefaultQueueSize = 64

// ForwarderConfig holds tunable parameters for TrackForwarder.
type ForwarderConfig struct {
	QueueSize int
}

// extMirror mirrors rtp.Extension layout (id uint8 + payload []byte) to
// allow deep-copy of the unexported payload field in pion/rtp v1.8.7.
type extMirror struct {
	id      uint8
	payload []byte
}

// Lock hierarchy (must be respected to avoid deadlocks):
//
//	PeerConnection.mu > TrackForwarder.mu > WebRTCTrack.mu
//	Handler.trackForwardersMutex > TrackForwarder.lifecycleMu > TrackForwarder.mu
//
// TrackForwarder must never call Handler while holding mu, and must call
// PeerConnection.AddTrack/RemoveTrack outside f.mu (snapshot codec/trackID
// under RLock, release, call, re-lock with double-checked CAS for idempotency).
// Handler always acquires peerConnectionsMutex before trackForwardersMutex
// (see Handler.ensurePeerConnection) — go vet lock order: peerConnectionsMutex > trackForwardersMutex > lifecycleMu > mu.
//
// TrackForwarder dual modes: TrackRemote publisher → run goroutine pumps
// Read → WriteRTP; TrackLocal placeholder → no run, IsRunning==false,
// forwarding only via explicit WriteRTP (IsRemote check via WebRTCTrack.IsRemote).
//
// subscriberEntry holds the per-subscriber forwarding state.
type subscriberEntry struct {
	pc           *PeerConnection
	pionTrack    *webrtc.TrackLocalStaticRTP
	webTrack     *WebRTCTrack
	queue        chan *rtp.Packet
	droppedCount uint64
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
}

func (e *subscriberEntry) runWriter() {
	defer close(e.done)
	for {
		select {
		case <-e.ctx.Done():
			// Drain remaining queued packets best-effort before exit.
			for {
				select {
				case pkt := <-e.queue:
					_ = e.pionTrack.WriteRTP(pkt)
				default:
					return
				}
			}
		case pkt := <-e.queue:
			_ = e.pionTrack.WriteRTP(pkt)
		}
	}
}

// clonePacket deep-copies Payload and each Extension.Payload.
func clonePacket(pkt *rtp.Packet) *rtp.Packet {
	clone := *pkt
	if pkt.Payload != nil {
		payloadCopy := make([]byte, len(pkt.Payload))
		copy(payloadCopy, pkt.Payload)
		clone.Payload = payloadCopy
	}
	if len(pkt.Header.Extensions) > 0 {
		exts := make([]rtp.Extension, len(pkt.Header.Extensions))
		for i, ext := range pkt.Header.Extensions {
			exts[i] = ext
			origPayload := (*extMirror)(unsafe.Pointer(&ext)).payload
			if origPayload != nil {
				copied := append([]byte(nil), origPayload...)
				(*extMirror)(unsafe.Pointer(&exts[i])).payload = copied
			}
		}
		clone.Header.Extensions = exts
	}
	return &clone
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
	queueSize   int
}

// NewTrackForwarder creates a forwarder for the given publisher track.
// publisher must be non-nil.
func NewTrackForwarder(publisher *WebRTCTrack) (*TrackForwarder, error) {
	return NewTrackForwarderWithConfig(publisher, ForwarderConfig{QueueSize: DefaultQueueSize})
}

// NewTrackForwarderWithConfig creates a forwarder with explicit queue sizing.
func NewTrackForwarderWithConfig(publisher *WebRTCTrack, cfg ForwarderConfig) (*TrackForwarder, error) {
	if publisher == nil {
		return nil, errors.New("publisher track is nil")
	}
	qs := cfg.QueueSize
	if qs <= 0 {
		qs = DefaultQueueSize
	}
	return &TrackForwarder{
		publisher:   publisher,
		subscribers: make(map[*PeerConnection]*subscriberEntry),
		queueSize:   qs,
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
//
// Lock ordering: lifecycleMu > mu (Handler.trackForwardersMutex > lifecycleMu > mu).
// Dual modes: TrackLocal → TrackRemote transitions running false→true and
// launches run; TrackRemote → TrackLocal stops run and sets running=false.
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

	isRemoteNew := publisher.IsRemote()

	f.mu.Lock()
	f.publisher = publisher
	if wasRunning && isRemoteNew {
		// Remote -> Remote restart: keep running true, relaunch.
		ctx, newCancel := context.WithCancel(context.Background())
		f.ctx = ctx
		f.cancel = newCancel
		f.wg.Add(1)
		go f.run(ctx)
	} else if wasRunning && !isRemoteNew {
		// Remote -> Local: stop, clear running.
		f.running = false
		f.ctx = nil
		f.cancel = nil
	} else if !wasRunning && isRemoteNew {
		// Local -> Remote: transition false->true and launch.
		ctx, newCancel := context.WithCancel(context.Background())
		f.ctx = ctx
		f.cancel = newCancel
		f.running = true
		f.wg.Add(1)
		go f.run(ctx)
	} else {
		// Local -> Local: keep not running.
		f.running = false
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
// Lock hierarchy: must call pc.AddTrack outside f.mu (f.mu > pc.mu would
// invert PeerConnection.mu > TrackForwarder.mu). Snapshot codec/trackID
// under RLock, release, call pc.AddTrack, then re-lock with CAS for
// idempotency (double-checked after pc call).
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

	// Snapshot under RLock then release before calling pc.AddTrack (lock hierarchy).
	f.mu.RLock()
	if _, exists := f.subscribers[pc]; exists {
		f.mu.RUnlock()
		return nil
	}
	codec := f.publisher.Codec()
	trackID := f.publisher.ID()
	domainTrack := f.publisher.DomainTrack()
	kind := f.publisher.Kind()
	f.mu.RUnlock()

	capability := codec.RTPCodecCapability
	// Fallback when codec was never negotiated (zero value): infer mime type from kind.
	if capability.MimeType == "" {
		switch kind {
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

	streamID := trackID + "-forward"

	pionTrack, err := webrtc.NewTrackLocalStaticRTP(capability, trackID, streamID)
	if err != nil {
		return err
	}

	// Preserve full codec parameters on the wrapper so subscribers can inspect them.
	webTrack := NewWebRTCTrack(domainTrack, pionTrack, codec)

	queueSize := f.queueSize
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}
	q := make(chan *rtp.Packet, queueSize)
	ctx, cancel := context.WithCancel(context.Background())
	entry := &subscriberEntry{
		pc:        pc,
		pionTrack: pionTrack,
		webTrack:  webTrack,
		queue:     q,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
	}

	// Call outside f.mu to respect PeerConnection.mu > TrackForwarder.mu.
	if err := pc.AddTrack(webTrack); err != nil {
		cancel()
		return err
	}

	f.mu.Lock()
	// Double-checked CAS: if another goroutine inserted same pc while we were outside lock, undo our AddTrack.
	if _, exists := f.subscribers[pc]; exists {
		f.mu.Unlock()
		_ = pc.RemoveTrack(trackID)
		cancel()
		return nil
	}
	f.subscribers[pc] = entry
	go entry.runWriter()
	f.mu.Unlock()
	return nil
}

// RemoveSubscriber unregisters pc. It stops the per-subscriber writer,
// removes the forwarded track from the subscriber's PeerConnection when
// possible and deletes the entry. Returns ErrTrackNotFound if pc was not a
// subscriber. Drains queue best-effort before removing track.
//
// Lock hierarchy: calls pc.RemoveTrack outside f.mu.
func (f *TrackForwarder) RemoveSubscriber(pc *PeerConnection) error {
	if pc == nil {
		return errors.New("peer connection is nil")
	}

	f.mu.Lock()
	entry, exists := f.subscribers[pc]
	if !exists {
		f.mu.Unlock()
		return ErrTrackNotFound
	}
	delete(f.subscribers, pc)
	trackID := f.publisher.ID()
	f.mu.Unlock()

	if entry.cancel != nil {
		entry.cancel()
	}
	// Wait for writer to exit (drain done) without holding lock.
	<-entry.done

	// Call outside f.mu to respect PeerConnection.mu > TrackForwarder.mu.
	_ = pc.RemoveTrack(trackID)

	return nil
}

// Start launches the forwarding goroutine. It is idempotent: calling Start
// on an already-running forwarder returns nil. Returns an error if the
// publisher track is nil.
//
// Dual modes: TrackLocal placeholder → no goroutine, running stays false,
// forwarding only via WriteRTP. TrackRemote → run pumps Read → WriteRTP.
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
	if !f.publisher.IsRemote() {
		// TrackLocal placeholder: no run goroutine, IsRunning stays false.
		f.running = false
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

// Stop terminates the forwarding goroutine and all per-subscriber writers,
// then removes the forwarded track from every subscriber. It is idempotent
// and safe to call concurrently with other methods. For TrackLocal publishers
// where IsRunning is false, Stop still drains writers and removes tracks.
func (f *TrackForwarder) Stop() error {
	f.lifecycleMu.Lock()
	defer f.lifecycleMu.Unlock()

	f.mu.Lock()
	wasRunning := f.running
	cancel := f.cancel
	if wasRunning {
		f.running = false
		f.ctx = nil
		f.cancel = nil
	}
	// Snapshot subscribers for cleanup outside lock to avoid holding forwarder
	// mu while calling PeerConnection methods (lock hierarchy).
	subs := make([]*subscriberEntry, 0, len(f.subscribers))
	for _, e := range f.subscribers {
		subs = append(subs, e)
	}
	// Clear map now so concurrent WriteRTP snapshots do not include stopped entries.
	for _, e := range subs {
		delete(f.subscribers, e.pc)
	}
	f.mu.Unlock()

	if wasRunning {
		if cancel != nil {
			cancel()
		}
		f.wg.Wait()
	}

	// Stop per-subscriber writers.
	for _, e := range subs {
		if e.cancel != nil {
			e.cancel()
		}
	}
	// Wait for all writers to drain and exit via per-entry done channel.
	for _, e := range subs {
		<-e.done
	}

	if len(subs) == 0 {
		return nil
	}

	// Remove forwarded tracks from subscribers; best-effort, ignore errors
	// from closed PCs.
	trackID := ""
	f.mu.RLock()
	if f.publisher != nil {
		trackID = f.publisher.ID()
	}
	f.mu.RUnlock()
	// Fallback to entry's track ID if publisher already nil (should not happen).
	if trackID == "" && len(subs) > 0 && subs[0].webTrack != nil {
		trackID = subs[0].webTrack.ID()
	}
	for _, e := range subs {
		_ = e.pc.RemoveTrack(trackID)
	}

	return nil
}

// WriteRTP forwards a single RTP packet to every subscriber via bounded
// per-subscriber queues. It clones the packet per subscriber so Pion's
// per-binding SSRC/payload-type rewrite does not race. Slow subscribers
// never block fast ones: enqueue is non-blocking; on full queue the packet
// is dropped and DroppedCount increments.
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
		clone := clonePacket(pkt)
		select {
		case e.queue <- clone:
		default:
			atomic.AddUint64(&e.droppedCount, 1)
		}
	}
	return nil
}

// DroppedCount returns the number of packets dropped for the given subscriber
// due to queue full. Returns 0 if pc is not a subscriber.
func (f *TrackForwarder) DroppedCount(pc *PeerConnection) uint64 {
	f.mu.RLock()
	entry, ok := f.subscribers[pc]
	f.mu.RUnlock()
	if !ok {
		return 0
	}
	return atomic.LoadUint64(&entry.droppedCount)
}

// QueueDepth returns the current queue depth for the given subscriber.
// Returns 0 if pc is not a subscriber.
func (f *TrackForwarder) QueueDepth(pc *PeerConnection) int {
	f.mu.RLock()
	entry, ok := f.subscribers[pc]
	f.mu.RUnlock()
	if !ok {
		return 0
	}
	return len(entry.queue)
}

// TotalDropped returns the sum of dropped packets across all subscribers.
func (f *TrackForwarder) TotalDropped() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var total uint64
	for _, e := range f.subscribers {
		total += atomic.LoadUint64(&e.droppedCount)
	}
	return total
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
//
// Per-iteration buf allocation avoids data race where helper goroutine holds
// slice while loop reuses buffer.
func (f *TrackForwarder) run(ctx context.Context) {
	defer f.wg.Done()
	defer func() {
		f.mu.Lock()
		f.running = false
		f.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Per-iteration buffer to avoid sharing across loop iterations.
		buf := make([]byte, 1500)

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
		go func(b []byte) {
			n, err := pub.Read(b)
			ch <- result{n, err}
		}(buf)

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
