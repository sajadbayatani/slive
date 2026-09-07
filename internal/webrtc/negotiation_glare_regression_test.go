package webrtc

import (
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// glareTestRig wires one Slive-side PC (the unit under test, with a
// capturing signaling sender) plus a loopback "browser" PC that plays the
// remote side with raw pion calls.
type glareTestRig struct {
	t      *testing.T
	slive  *PeerConnection
	brow   *pionwebrtc.PeerConnection
	offers atomic.Int64 // webrtc:offer sends from slive
	answer atomic.Int64 // webrtc:answer sends from slive (deferred answers)
	// mu guards lastAnswerSDP/lastOfferSDP (written by the sender, read by tests).
	mu            sync.Mutex
	lastAnswerSDP string
	lastOfferSDP  string
}

func newGlareTestRig(t *testing.T, id string) *glareTestRig {
	t.Helper()
	rig := &glareTestRig{t: t}
	sender := func(msgType string, data interface{}) error {
		raw, _ := json.Marshal(data)
		var payload struct {
			SDP string `json:"sdp"`
		}
		_ = json.Unmarshal(raw, &payload)
		switch msgType {
		case "webrtc:offer":
			rig.offers.Add(1)
			rig.mu.Lock()
			rig.lastOfferSDP = payload.SDP
			rig.mu.Unlock()
		case "webrtc:answer":
			rig.answer.Add(1)
			rig.mu.Lock()
			rig.lastAnswerSDP = payload.SDP
			rig.mu.Unlock()
		}
		return nil
	}
	pc, err := NewPeerConnection(PeerConnectionConfig{
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	}, domain.NewParticipant(id, "Glare"), sender)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	if _, err := pc.PionPeerConnection().AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("slive transceiver: %v", err)
	}
	rig.slive = pc

	brow, err := pionwebrtc.NewPeerConnection(pionwebrtc.Configuration{
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	})
	if err != nil {
		t.Fatalf("browser pc: %v", err)
	}
	t.Cleanup(func() { _ = brow.Close() })
	if _, err := brow.AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("browser transceiver: %v", err)
	}
	rig.brow = brow
	rig.settle()
	return rig
}

// settle absorbs setup-time async pion negotiation fires: the transceiver
// added above fires OnNegotiationNeeded on pion's ops worker, which can win
// the race against the scenario's first gate check. Every outstanding server
// offer is answered via a helper browser until the PC is stably quiet; flag
// state is then normalized and counters zeroed so each test starts from a
// deterministic clean stable.
func (r *glareTestRig) settle() {
	r.t.Helper()
	helper, err := pionwebrtc.NewPeerConnection(pionwebrtc.Configuration{
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	})
	if err != nil {
		r.t.Fatalf("settle helper pc: %v", err)
	}
	defer helper.Close()
	if _, err := helper.AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		r.t.Fatalf("settle helper transceiver: %v", err)
	}
	quiet := 0
	for i := 0; i < 20 && quiet < 3; i++ {
		time.Sleep(100 * time.Millisecond)
		if glareState(r) == pionwebrtc.SignalingStateHaveLocalOffer {
			quiet = 0
			local := r.slive.PionPeerConnection().LocalDescription()
			if local == nil {
				continue
			}
			if err := helper.SetRemoteDescription(pionwebrtc.SessionDescription{
				Type: pionwebrtc.SDPTypeOffer, SDP: local.SDP,
			}); err != nil {
				r.t.Fatalf("settle helper SetRemote: %v", err)
			}
			ans, err := helper.CreateAnswer(nil)
			if err != nil {
				r.t.Fatalf("settle helper CreateAnswer: %v", err)
			}
			if err := helper.SetLocalDescription(ans); err != nil {
				r.t.Fatalf("settle helper SetLocal: %v", err)
			}
			if err := r.slive.ProcessBrowserAnswer(
				NewSessionDescription(helper.LocalDescription())); err != nil {
				r.t.Fatalf("settle ProcessBrowserAnswer: %v", err)
			}
			continue
		}
		quiet++
	}
	if st := glareState(r); st != pionwebrtc.SignalingStateStable {
		r.t.Fatalf("settle: slive state = %s, want stable", st)
	}
	r.slive.mu.Lock()
	r.slive.isNegotiating = false
	r.slive.pendingNegotiation = false
	r.slive.pendingInboundOffer = nil
	r.slive.mu.Unlock()
	r.mu.Lock()
	r.lastAnswerSDP = ""
	r.lastOfferSDP = ""
	r.mu.Unlock()
	r.offers.Store(0)
	r.answer.Store(0)
}

// browserOffer makes the loopback browser create + locally install a publisher
// offer (browser enters have-local-offer) and returns it wrapped for Slive.
func (r *glareTestRig) browserOffer() *SessionDescription {
	r.t.Helper()
	offer, err := r.brow.CreateOffer(nil)
	if err != nil {
		r.t.Fatalf("browser CreateOffer: %v", err)
	}
	if err := r.brow.SetLocalDescription(offer); err != nil {
		r.t.Fatalf("browser SetLocal: %v", err)
	}
	return NewSessionDescription(r.brow.LocalDescription())
}

// browserAcceptAnswer installs Slive's answer on the loopback browser.
func (r *glareTestRig) browserAcceptAnswer(answer *SessionDescription) {
	r.t.Helper()
	if err := r.brow.SetRemoteDescription(pionwebrtc.SessionDescription{
		Type: pionwebrtc.SDPTypeAnswer, SDP: answer.SDP(),
	}); err != nil {
		r.t.Fatalf("browser SetRemote(answer): %v", err)
	}
}

// answerOutstandingOffer answers Slive's current outstanding server offer
// with the loopback browser (which must be stable) and feeds the answer back
// through the gate. No final state assertion: a coalesced flush may fire the
// next server offer synchronously, legitimately leaving have-local-offer.
func (r *glareTestRig) answerOutstandingOffer() {
	r.t.Helper()
	local := r.slive.PionPeerConnection().LocalDescription()
	if local == nil {
		r.t.Fatal("slive has no outstanding offer to answer")
	}
	if err := r.brow.SetRemoteDescription(pionwebrtc.SessionDescription{
		Type: pionwebrtc.SDPTypeOffer, SDP: local.SDP,
	}); err != nil {
		r.t.Fatalf("browser SetRemote(slive offer): %v", err)
	}
	ans, err := r.brow.CreateAnswer(nil)
	if err != nil {
		r.t.Fatalf("browser CreateAnswer: %v", err)
	}
	if err := r.brow.SetLocalDescription(ans); err != nil {
		r.t.Fatalf("browser SetLocal(answer): %v", err)
	}
	if err := r.slive.ProcessBrowserAnswer(NewSessionDescription(r.brow.LocalDescription())); err != nil {
		r.t.Fatalf("ProcessBrowserAnswer: %v", err)
	}
}

// answerSliveOffer completes the outstanding exchange and requires the PC to
// settle back to stable (i.e. no coalesced offer was flushed behind it).
func (r *glareTestRig) answerSliveOffer() {
	r.t.Helper()
	r.answerOutstandingOffer()
	if st := r.slive.PionPeerConnection().SignalingState(); st != pionwebrtc.SignalingStateStable {
		r.t.Fatalf("slive state = %s, want stable", st)
	}
}

func glareState(r *glareTestRig) pionwebrtc.SignalingState {
	return r.slive.PionPeerConnection().SignalingState()
}

// TestGlare_BrowserOfferAnsweredInline covers Flow A (normal first
// publisher): browser offer answered inline, stable afterwards, and no
// server offer emitted for a PC nobody asked to subscribe.
func TestGlare_BrowserOfferAnsweredInline(t *testing.T) {
	rig := newGlareTestRig(t, "glare-inline")

	answer, deferred, err := rig.slive.ProcessBrowserOffer(rig.browserOffer())
	if err != nil {
		t.Fatalf("ProcessBrowserOffer: %v", err)
	}
	if deferred {
		t.Fatal("browser offer deferred on a stable PC, want inline answer")
	}
	if answer == nil || answer.Type() != pionwebrtc.SDPTypeAnswer {
		t.Fatalf("answer = %+v, want SDP answer", answer)
	}
	if st := glareState(rig); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("slive state = %s, want stable", st)
	}
	rig.browserAcceptAnswer(answer)
	if got := rig.offers.Load(); got != 0 {
		t.Errorf("server offers = %d, want 0 (no subscriptions requested)", got)
	}
	// A subscriber trigger means a real subscription: AddTrack plus the gate
	// trigger. (A trigger without any sender change is skipped by the
	// generation guard — it would duplicate the last offer.)
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "glare-inline-audio", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	deadline := time.Now().Add(5 * time.Second)
	for rig.offers.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := rig.offers.Load(); got == 0 {
		t.Error("no server offer after publish completed, want 1")
	}
}

// TestGlare_OfferBeforePublishTrackThenSubscribe is the room-tg5a4jgfbv
// regression: the browser's publisher offer arrives and is answered BEFORE
// publish_track is processed (the client sends the offer first). A publish
// expectation recorded afterwards would be stale — the expected offer
// already completed — and under the old gate it deferred every later
// subscriber offer forever (first participant rendered nothing of the late
// joiner). There is no expectation mechanism anymore: subscribes made after
// a completed publish must offer immediately.
func TestGlare_OfferBeforePublishTrackThenSubscribe(t *testing.T) {
	rig := newGlareTestRig(t, "glare-stale-order")
	// 1. Browser publishes first, before any publish_track: answered inline.
	browserOffer := rig.browserOffer()
	answer, deferred, err := rig.slive.ProcessBrowserOffer(browserOffer)
	if err != nil || deferred || answer == nil {
		t.Fatalf("ProcessBrowserOffer: answer=%v deferred=%v err=%v, want inline answer", answer, deferred, err)
	}
	rig.browserAcceptAnswer(answer)
	// publish_track lands here in production (stale: its offer already
	// completed). Nothing records it; the gate must stay open.
	// 2. Subscribes after the completed publish must offer at once — this
	// stuck forever under the stale expectation.
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "glare-stale-audio", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	deadline := time.Now().Add(5 * time.Second)
	for rig.offers.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := rig.offers.Load(); got == 0 {
		t.Fatal("REGRESSION: no server offer after subscribe on stable PC (stale publish expectation deadlock)")
	}
	// 3. Answer it → stable, nothing pending.
	rig.answerSliveOffer()
	rig.slive.mu.RLock()
	stillPending := rig.slive.pendingNegotiation
	rig.slive.mu.RUnlock()
	if stillPending {
		t.Error("pendingNegotiation still true after answer, want false")
	}
}

// TestGlare_ServerOfferThenBrowserOffer covers the residual glare case: our
// subscriber offer is outstanding (have-local-offer) when the browser's
// publisher offer arrives (publish_track unseen — publish expectation could
// not gate it). The inbound offer must be stored, never answered with
// InvalidModificationError; once our offer completes, the stored offer is
// answered with priority and delivered over the signaling sender.
func TestGlare_ServerOfferThenBrowserOffer(t *testing.T) {
	rig := newGlareTestRig(t, "glare-residual")
	// NOTE: no NotePublishRequested — the sliver geometry. The outstanding
	// subscriber offer follows a real subscription (AddTrack), as in
	// production.
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "glare-residual-audio", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}

	// Drive our subscriber offer to have-local-offer.
	rig.slive.handleNegotiationNeeded()
	deadline := time.Now().Add(5 * time.Second)
	for glareState(rig) != pionwebrtc.SignalingStateHaveLocalOffer && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if st := glareState(rig); st != pionwebrtc.SignalingStateHaveLocalOffer {
		t.Fatalf("slive state = %s, want have-local-offer", st)
	}
	if got := rig.offers.Load(); got == 0 {
		t.Fatal("no server offer emitted")
	}

	// Browser publisher offer arrives behind ours: must defer, not fail.
	inbound := rig.browserOffer()
	// Prove the geometry is the true production glare first: raw pion
	// rejects SetRemote(offer) in have-local-offer with the exact reported
	// InvalidModificationError.
	rawErr := rig.slive.PionPeerConnection().SetRemoteDescription(pionwebrtc.SessionDescription{
		Type: pionwebrtc.SDPTypeOffer, SDP: inbound.SDP(),
	})
	if rawErr == nil || !strings.Contains(rawErr.Error(), "InvalidModificationError") {
		t.Fatalf("raw pion SetRemote behind local offer: err=%v, want InvalidModificationError (test geometry must match production)", rawErr)
	}
	answer, deferred, err := rig.slive.ProcessBrowserOffer(inbound)
	if err != nil {
		t.Fatalf("ProcessBrowserOffer behind local offer: %v", err)
	}
	if !deferred || answer != nil {
		t.Fatalf("got answer=%v deferred=%v, want (nil, true)", answer, deferred)
	}
	rig.slive.mu.RLock()
	stored := rig.slive.pendingInboundOffer != nil
	rig.slive.mu.RUnlock()
	if !stored {
		t.Fatal("pendingInboundOffer not stored")
	}

	// Complete our offer with a SECOND (stable) browser peer — the real
	// first browser is itself have-local-offer and cannot answer, mirroring
	// mutual glare. Flushing must answer the stored offer first.
	helper, err := pionwebrtc.NewPeerConnection(pionwebrtc.Configuration{
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	})
	if err != nil {
		t.Fatalf("helper pc: %v", err)
	}
	defer helper.Close()
	if _, err := helper.AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("helper transceiver: %v", err)
	}
	local := rig.slive.PionPeerConnection().LocalDescription()
	if err := helper.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeOffer, SDP: local.SDP}); err != nil {
		t.Fatalf("helper SetRemote: %v", err)
	}
	ans, err := helper.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("helper CreateAnswer: %v", err)
	}
	if err := helper.SetLocalDescription(ans); err != nil {
		t.Fatalf("helper SetLocal: %v", err)
	}
	if err := rig.slive.ProcessBrowserAnswer(NewSessionDescription(helper.LocalDescription())); err != nil {
		t.Fatalf("ProcessBrowserAnswer: %v", err)
	}
	// The stored browser offer must have been answered with priority and
	// delivered over the sender.
	deadline = time.Now().Add(5 * time.Second)
	for rig.answer.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := rig.answer.Load(); got == 0 {
		t.Fatal("deferred browser offer was never answered")
	}
	if st := glareState(rig); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("slive state = %s, want stable", st)
	}
	rig.slive.mu.RLock()
	stored = rig.slive.pendingInboundOffer != nil
	rig.slive.mu.RUnlock()
	if stored {
		t.Error("pendingInboundOffer not cleared after flush")
	}
}

// TestGlare_AnswerWithoutOffer returns the typed sentinel (never a pion
// InvalidModificationError) and changes no state.
func TestGlare_AnswerWithoutOffer(t *testing.T) {
	rig := newGlareTestRig(t, "glare-nooffer")
	// Mint a syntactically valid answer SDP on an independent browser pair:
	// helper offers, rig browser answers, rig browser holds a local answer.
	helper, err := pionwebrtc.NewPeerConnection(pionwebrtc.Configuration{
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	})
	if err != nil {
		t.Fatalf("helper pc: %v", err)
	}
	defer helper.Close()
	if _, err := helper.AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("helper transceiver: %v", err)
	}
	hOffer, err := helper.CreateOffer(nil)
	if err != nil {
		t.Fatalf("helper offer: %v", err)
	}
	if err := helper.SetLocalDescription(hOffer); err != nil {
		t.Fatalf("helper SetLocal: %v", err)
	}
	if err := rig.brow.SetRemoteDescription(pionwebrtc.SessionDescription{
		Type: pionwebrtc.SDPTypeOffer, SDP: helper.LocalDescription().SDP,
	}); err != nil {
		t.Fatalf("browser SetRemote: %v", err)
	}
	bAns, err := rig.brow.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("browser CreateAnswer: %v", err)
	}
	if err := rig.brow.SetLocalDescription(bAns); err != nil {
		t.Fatalf("browser SetLocal: %v", err)
	}
	// Slive is stable with no outstanding server offer: the answer is stale.
	err = rig.slive.ProcessBrowserAnswer(NewSessionDescription(rig.brow.LocalDescription()))
	if err == nil {
		t.Fatal("ProcessBrowserAnswer on stable PC succeeded, want ErrNoPendingOffer")
	}
	if !strings.Contains(err.Error(), "no outstanding server offer") {
		t.Fatalf("err = %q, want ErrNoPendingOffer", err)
	}
	if strings.Contains(err.Error(), "InvalidModificationError") {
		t.Fatalf("err leaks pion glare: %q", err)
	}
	if st := glareState(rig); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("slive state = %s, want stable (unchanged)", st)
	}
}
