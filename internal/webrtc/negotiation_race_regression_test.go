package webrtc

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// waitOfferCount polls until the captured offer counter reaches want.
func waitOfferCount(t *testing.T, counter *int32, want int32, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt32(counter) < want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadInt32(counter); got < want {
		t.Fatalf("%s: offers = %d, want >= %d", what, got, want)
	}
}

// answerSubscriberOffer completes the PC's current outstanding subscriber
// offer with a fresh helper PC and installs the answer through the legacy
// SetRemoteDescription completion path.
func answerSubscriberOffer(t *testing.T, pc *PeerConnection, cfg PeerConnectionConfig, round int) {
	t.Helper()
	local := pc.PionPeerConnection().LocalDescription()
	if local == nil {
		t.Fatalf("round %d: no local description to answer", round)
	}
	helper, err := NewPeerConnection(cfg, domain.NewParticipant(fmt.Sprintf("helper-%d", round), "H"), nil)
	if err != nil {
		t.Fatalf("round %d: helper pc: %v", round, err)
	}
	defer helper.Close()
	answer, err := helper.CreateAnswer(NewSessionDescription(local))
	if err != nil {
		t.Fatalf("round %d: helper CreateAnswer: %v", round, err)
	}
	if err := pc.SetRemoteDescription(answer); err != nil {
		t.Fatalf("round %d: SetRemoteDescription(answer): %v", round, err)
	}
}

// waitOfferStable polls until no new offer has been sent for quiet
// consecutive time (bounded by budget) and returns the settled count.
// Flushed/coalesced offers arrive asynchronously, so fixed sleeps alone
// cannot prove quiescence on loaded runners.
func waitOfferStable(t *testing.T, counter *int32, quiet, budget time.Duration) int32 {
	t.Helper()
	deadline := time.Now().Add(budget)
	last := atomic.LoadInt32(counter)
	lastChange := time.Now()
	for {
		time.Sleep(20 * time.Millisecond)
		cur := atomic.LoadInt32(counter)
		if cur != last {
			last = cur
			lastChange = time.Now()
		}
		if time.Since(lastChange) >= quiet {
			return cur
		}
		if time.Now().After(deadline) {
			t.Fatalf("offers never stabilized (last count %d)", cur)
		}
	}
}

// TestRegression_NegotiationCoalescing verifies that a negotiation request
// arriving while signalingState==have-local-offer is queued and not lost.
// This covers the reported "Skipping offer for p-f8d2a6b3; signalingState=have-local-offer".
func TestRegression_NegotiationCoalescing(t *testing.T) {
	participant := domain.NewParticipant("p-f8d2a6b3", "B")
	cfg := PeerConnectionConfig{SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback}
	var offerCount int32
	sender := func(msgType string, data interface{}) error {
		if msgType == "webrtc:offer" {
			atomic.AddInt32(&offerCount, 1)
		}
		return nil
	}
	pc, err := NewPeerConnection(cfg, participant, sender)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	defer pc.Close()

	if err := pc.AddTrack(newTestLocalTrack(t, "sub-a", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}
	pc.handleNegotiationNeeded()
	waitOfferCount(t, &offerCount, 1, "first offer")

	// While the first exchange is in flight (isNegotiating, have-local-offer),
	// a second subscription lands: its trigger must be queued, not dropped.
	if err := pc.AddTrack(newTestLocalTrack(t, "sub-b", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack second: %v", err)
	}
	pc.handleNegotiationNeeded()
	time.Sleep(200 * time.Millisecond)
	if atomic.LoadInt32(&offerCount) != 1 {
		t.Logf("offerCount after second trigger = %d (want 1, queued)", offerCount)
	}
	pc.mu.RLock()
	pending := pc.pendingNegotiation
	negotiating := pc.isNegotiating
	pc.mu.RUnlock()
	if !pending {
		t.Errorf("pendingNegotiation = false, want true after second trigger while offer pending; negotiating=%v", negotiating)
	}
	if !negotiating {
		t.Errorf("isNegotiating = false, want true while offer pending")
	}

	// Completion flushes the queued subscription as exactly one more offer.
	answerSubscriberOffer(t, pc, cfg, 1)
	waitOfferCount(t, &offerCount, 2, "flushed second offer")
	if atomic.LoadInt32(&offerCount) != 2 {
		t.Errorf("offerCount after flush = %d, want exactly 2 (second coalesced offer)", offerCount)
	}
	// Complete the second exchange: its answer drains any late echo through
	// the generation guard without producing a third offer.
	answerSubscriberOffer(t, pc, cfg, 2)
	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt32(&offerCount); got != 2 {
		t.Errorf("offerCount after second answer = %d, want exactly 2", got)
	}
	pc.mu.RLock()
	stillPending := pc.pendingNegotiation
	pc.mu.RUnlock()
	if stillPending {
		t.Error("pendingNegotiation still true after flush, want false")
	}
}

// TestRegression_MultipleSubscriptionsCloseTogether verifies that rapid
// subscription-driven triggers coalesce (exactly one offer per outstanding
// exchange) and that no subscription is ever lost: whatever the burst's
// first offer carries, the converged offer after the exchange carries ALL
// added tracks, and a later real subscription still produces exactly one
// more offer.
//
// Scheduling note: pion dispatches its first-ever OnNegotiationNeeded
// asynchronously on its operations queue, so under load that echo can win
// the race against the synchronous triggers below and emit the burst's
// first offer from a partial generation (e.g. only sub-a added so far).
// That echo is a legitimate offer for the state it snapshotted — the
// coalescing machinery (isNegotiating + generation guard + answer flush)
// converges it — so this test asserts convergence properties that hold
// under EVERY interleaving instead of assuming the sync driver wins.
func TestRegression_MultipleSubscriptionsCloseTogether(t *testing.T) {
	participant := domain.NewParticipant("p-59750466", "A")
	cfg := PeerConnectionConfig{SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback}
	var offers int32
	sender := func(msgType string, data interface{}) error {
		if msgType == "webrtc:offer" {
			atomic.AddInt32(&offers, 1)
		}
		return nil
	}
	pc, err := NewPeerConnection(cfg, participant, sender)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	defer pc.Close()

	for _, id := range []string{"sub-a", "sub-b", "sub-c"} {
		if err := pc.AddTrack(newTestLocalTrack(t, id, domain.TrackKindAudio)); err != nil {
			t.Fatalf("AddTrack %s: %v", id, err)
		}
	}
	// Fire 3 negotiations in quick succession (simulating participant_joined
	// + 2 track subscribes): whoever creates the first offer (sync driver or
	// async pion echo), the other triggers must coalesce behind it — exactly
	// one offer while the exchange is outstanding.
	pc.handleNegotiationNeeded()
	pc.handleNegotiationNeeded()
	pc.handleNegotiationNeeded()
	waitOfferCount(t, &offers, 1, "burst offer")
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt32(&offers); got != 1 {
		t.Fatalf("offers after rapid triggers = %d, want 1 (burst coalesced)", got)
	}

	// Answer the outstanding burst offer. If it was partial (async echo won),
	// the armed coalesced need flushes exactly one converged offer; if the
	// sync driver already covered everything, the generation guard skips the
	// flush. Either way there is at most one more offer, and the latest offer
	// carries all three subscriptions — nothing lost. Settle actively: the
	// flush runs asynchronously, so a fixed sleep cannot prove quiescence on
	// loaded runners.
	answerSubscriberOffer(t, pc, cfg, 1)
	settled := waitOfferStable(t, &offers, 300*time.Millisecond, 8*time.Second)
	if settled > 2 {
		t.Fatalf("offers after burst answer = %d, want <= 2 (at most one converged flush)", settled)
	}
	if egress := countEgressMIDs(t, pc.PionPeerConnection().LocalDescription().SDP); egress != 3 {
		t.Errorf("converged offer carries %d egress m-lines, want 3 (all subscriptions negotiated, none lost)", egress)
	}

	// After the exchange completes, a further subscription must still get
	// exactly one of its own offers — the coalesced need is never dropped.
	// If the burst's first offer was partial, the converged flush above left
	// a second offer outstanding: answer it so the check below starts from
	// a clean stable state in every interleaving.
	if pc.PionPeerConnection().SignalingState() == webrtc.SignalingStateHaveLocalOffer {
		answerSubscriberOffer(t, pc, cfg, 2)
		time.Sleep(300 * time.Millisecond)
	}
	before := atomic.LoadInt32(&offers)
	if err := pc.AddTrack(newTestLocalTrack(t, "sub-d", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack sub-d: %v", err)
	}
	pc.handleNegotiationNeeded()
	waitOfferCount(t, &offers, before+1, "post-stable offer")
	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt32(&offers); got != before+1 {
		t.Fatalf("offers after stable subscription = %d, want %d (exactly one more)", got, before+1)
	}
}

// TestRegression_NoLostRenegotiationAfterStable ensures that after each
// completed exchange a fresh subscription in stable state produces a new
// offer — the queued/coalesced machinery never eats a later renegotiation.
func TestRegression_NoLostRenegotiationAfterStable(t *testing.T) {
	participant := domain.NewParticipant("p-1", "A")
	cfg := PeerConnectionConfig{SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback}
	var offers int32
	sender := func(msgType string, data interface{}) error {
		if msgType == "webrtc:offer" {
			atomic.AddInt32(&offers, 1)
		}
		return nil
	}
	pc, err := NewPeerConnection(cfg, participant, sender)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	defer pc.Close()

	for round, id := range []string{"sub-a", "sub-b", "sub-c"} {
		before := atomic.LoadInt32(&offers)
		if err := pc.AddTrack(newTestLocalTrack(t, id, domain.TrackKindAudio)); err != nil {
			t.Fatalf("round %d: AddTrack: %v", round+1, err)
		}
		pc.handleNegotiationNeeded()
		waitOfferCount(t, &offers, before+1, fmt.Sprintf("round %d offer", round+1))
		answerSubscriberOffer(t, pc, cfg, round+1)
		time.Sleep(300 * time.Millisecond)
		pc.mu.RLock()
		pending := pc.pendingNegotiation
		pc.mu.RUnlock()
		if pending {
			t.Errorf("round %d: pendingNegotiation still true after flush, want false", round+1)
		}
	}
	if got := atomic.LoadInt32(&offers); got != 3 {
		t.Errorf("total offers after 3 complete exchanges = %d, want 3 (one per subscription)", got)
	}
}
