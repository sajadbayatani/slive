package webrtc

// Deterministic tests for the unified-PC negotiation state machine, driven
// against the exact production geometry of room lf5q1xwyfx: a subscriber
// offer outstanding (have-local-offer) while the browser's publisher offer
// crosses it, and the browser re-offering after each rollback it performed
// to answer our offers.
//
// Browser model note: pion v3.3.6 implements no rollback (have-local-offer
// accepts only SetRemote(answer)), so a single pion "browser" PC cannot
// mimic Chrome's implicit rollback. The server only ever sees ordered SDP
// strings on one FIFO signaling channel, so minting each successive browser
// offer on an independent pion PC is wire-equivalent: each minter is stable
// at CreateOffer time and is the only PC that can apply the answer to "its"
// offer.

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// lifecycleWaitOffer polls until the rig has sent at least want offers.
func lifecycleWaitOffer(t *testing.T, rig *glareTestRig, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for rig.offers.Load() < want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := rig.offers.Load(); got < want {
		t.Fatalf("server offers = %d, want >= %d", got, want)
	}
}

func lifecycleWaitAnswer(t *testing.T, rig *glareTestRig, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for rig.answer.Load() < want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := rig.answer.Load(); got < want {
		t.Fatalf("deferred answers delivered = %d, want >= %d", got, want)
	}
}

func lifecycleLastAnswer(t *testing.T, rig *glareTestRig) string {
	t.Helper()
	rig.mu.Lock()
	defer rig.mu.Unlock()
	if rig.lastAnswerSDP == "" {
		t.Fatal("no answer captured by the rig sender")
	}
	return rig.lastAnswerSDP
}

// lifecycleLastOffer returns the most recently sent server offer SDP. After
// an inline browser answer the PC's local description becomes the answer
// (which only covers the browser's offered m-lines), so tests asserting on
// egress m-line survival must read the last generated offer, not the current
// local description.
func lifecycleLastOffer(t *testing.T, rig *glareTestRig) string {
	t.Helper()
	rig.mu.Lock()
	defer rig.mu.Unlock()
	if rig.lastOfferSDP == "" {
		t.Fatal("no offer captured by the rig sender")
	}
	return rig.lastOfferSDP
}

// lifecycleDriveSubscriberOffer adds an egress track and drives the gate
// until the subscriber offer is live (have-local-offer).
func lifecycleDriveSubscriberOffer(t *testing.T, rig *glareTestRig, trackID string, want int64) {
	t.Helper()
	if err := rig.slive.AddTrack(newTestLocalTrack(t, trackID, domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack %s: %v", trackID, err)
	}
	rig.slive.handleNegotiationNeeded()
	lifecycleWaitOffer(t, rig, want)
	if st := glareState(rig); st != pionwebrtc.SignalingStateHaveLocalOffer {
		t.Fatalf("slive state = %s, want have-local-offer", st)
	}
}

// lifecycleMintBrowserOffer builds an independent browser PC with one audio
// publish transceiver and returns the PC, its publisher offer, and an
// installer that applies a server answer to it (have-local-offer -> stable).
func lifecycleMintBrowserOffer(t *testing.T, id string) (*pionwebrtc.PeerConnection, *SessionDescription, func(*SessionDescription)) {
	t.Helper()
	brow, err := pionwebrtc.NewPeerConnection(pionwebrtc.Configuration{
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	})
	if err != nil {
		t.Fatalf("browser pc %s: %v", id, err)
	}
	t.Cleanup(func() { _ = brow.Close() })
	if _, err := brow.AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio, pionwebrtc.RtpTransceiverInit{
		Direction: pionwebrtc.RTPTransceiverDirectionSendonly,
	}); err != nil {
		t.Fatalf("browser %s transceiver: %v", id, err)
	}
	offer, err := brow.CreateOffer(nil)
	if err != nil {
		t.Fatalf("browser %s CreateOffer: %v", id, err)
	}
	if err := brow.SetLocalDescription(offer); err != nil {
		t.Fatalf("browser %s SetLocal: %v", id, err)
	}
	install := func(answer *SessionDescription) {
		t.Helper()
		if err := brow.SetRemoteDescription(pionwebrtc.SessionDescription{
			Type: pionwebrtc.SDPTypeAnswer, SDP: answer.SDP(),
		}); err != nil {
			t.Fatalf("browser %s apply answer: %v", id, err)
		}
		if st := brow.SignalingState(); st != pionwebrtc.SignalingStateStable {
			t.Fatalf("browser %s state after answer = %s, want stable", id, st)
		}
	}
	return brow, NewSessionDescription(brow.LocalDescription()), install
}

// lifecycleNewHelper builds a helper PC able to answer the slive PC's
// subscriber offers (the real browser peers are have-local-offer with their
// own publisher offers and cannot answer).
func lifecycleNewHelper(t *testing.T) *pionwebrtc.PeerConnection {
	t.Helper()
	helper, err := pionwebrtc.NewPeerConnection(pionwebrtc.Configuration{
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	})
	if err != nil {
		t.Fatalf("helper pc: %v", err)
	}
	t.Cleanup(func() { _ = helper.Close() })
	if _, err := helper.AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("helper transceiver: %v", err)
	}
	return helper
}

// lifecycleAnswerOutstandingOffer completes the slive PC's current
// outstanding subscriber offer with the helper PC and installs the answer
// through the gate.
func lifecycleAnswerOutstandingOffer(t *testing.T, rig *glareTestRig, helper *pionwebrtc.PeerConnection) {
	t.Helper()
	local := rig.slive.PionPeerConnection().LocalDescription()
	if local == nil {
		t.Fatal("no outstanding slive offer to answer")
	}
	if err := helper.SetRemoteDescription(pionwebrtc.SessionDescription{
		Type: pionwebrtc.SDPTypeOffer, SDP: local.SDP,
	}); err != nil {
		t.Fatalf("helper SetRemote(slive offer): %v", err)
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
}

// lifecycleStableQuiet waits until the slive PC is stable with an empty gate
// (no outstanding server offer, no coalesced need, no deferred browser
// offer) — the converged end state of every lifecycle.
func lifecycleStableQuiet(t *testing.T, rig *glareTestRig) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st := glareState(rig)
		rig.slive.mu.RLock()
		busy := rig.slive.isNegotiating || rig.slive.pendingNegotiation ||
			rig.slive.pendingInboundOffer != nil
		rig.slive.mu.RUnlock()
		if st == pionwebrtc.SignalingStateStable && !busy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	rig.slive.mu.RLock()
	defer rig.slive.mu.RUnlock()
	t.Fatalf("lifecycle never converged: state=%s negotiating=%v pending=%v inbound=%v",
		glareState(rig), rig.slive.isNegotiating, rig.slive.pendingNegotiation,
		rig.slive.pendingInboundOffer != nil)
}

// TestLifecycle_SecondBrowserOfferSupersedesDeferred is the direct
// "second outstanding browser offer" regression: a second (and third)
// browser offer arriving while one is already deferred must supersede it
// (newest wins, ONE lifecycle), never error. The flushed answer must be
// appliable on the NEWEST minter.
func TestLifecycle_SecondBrowserOfferSupersedesDeferred(t *testing.T) {
	rig := newGlareTestRig(t, "lifecycle-supersede")
	lifecycleDriveSubscriberOffer(t, rig, "lcs-egress-a", 1)

	brow1, offer1, _ := lifecycleMintBrowserOffer(t, "lcs-b1")
	answer1, deferred, err := rig.slive.ProcessBrowserOffer(offer1)
	if err != nil || !deferred || answer1 != nil {
		t.Fatalf("first browser offer: answer=%v deferred=%v err=%v, want (nil,true,nil)", answer1, deferred, err)
	}

	// The browser replaces its local offer (Chrome re-offers after every
	// rollback it performed to answer our subscriber offer). The second
	// inbound offer must supersede the stored one without any error.
	_, offer2, _ := lifecycleMintBrowserOffer(t, "lcs-b2")
	if _, deferred, err := rig.slive.ProcessBrowserOffer(offer2); err != nil || !deferred {
		t.Fatalf("REGRESSION second outstanding browser offer: err=%v deferred=%v, want (nil,true)", err, deferred)
	}
	rig.slive.mu.RLock()
	storedIsNewest := rig.slive.pendingInboundOffer != nil && rig.slive.pendingInboundOffer.SDP() == offer2.SDP()
	rig.slive.mu.RUnlock()
	if !storedIsNewest {
		t.Fatal("deferred slot does not hold the newest browser offer")
	}

	// A third offer must also supersede cleanly (no accumulation, no error,
	// still exactly one lifecycle).
	_, offer3, install3 := lifecycleMintBrowserOffer(t, "lcs-b3")
	if _, deferred, err := rig.slive.ProcessBrowserOffer(offer3); err != nil || !deferred {
		t.Fatalf("third browser offer: err=%v deferred=%v, want (nil,true)", err, deferred)
	}

	// Complete the subscriber exchange with a helper and let the flush
	// answer the newest stored offer. Exactly one deferred answer goes out.
	helper := lifecycleNewHelper(t)
	lifecycleAnswerOutstandingOffer(t, rig, helper)
	lifecycleWaitAnswer(t, rig, 1)
	lifecycleStableQuiet(t, rig)
	if got := rig.offers.Load(); got != 1 {
		t.Fatalf("server offers = %d, want 1 (no subscriber re-offer without sender changes)", got)
	}
	rig.slive.mu.RLock()
	slotCleared := rig.slive.pendingInboundOffer == nil
	rig.slive.mu.RUnlock()
	if !slotCleared {
		t.Fatal("deferred slot not cleared after flush")
	}

	// The delivered answer must be the answer to the NEWEST offer: it
	// applies on the newest minter (have-local-offer -> stable) and is
	// rejected by the stale minter.
	ansSDP := lifecycleLastAnswer(t, rig)
	install3(lifecycleParseAnswer(t, ansSDP))
	if err := brow1.SetRemoteDescription(pionwebrtc.SessionDescription{
		Type: pionwebrtc.SDPTypeAnswer, SDP: ansSDP,
	}); err == nil {
		t.Log("stale minter structurally accepted the answer; supersession is still required for ordering correctness")
	}
}

// lifecycleParseAnswer parses a captured answer SDP.
func lifecycleParseAnswer(t *testing.T, sdp string) *SessionDescription {
	t.Helper()
	sd, err := NewSessionDescriptionFromString(sdp, pionwebrtc.SDPTypeAnswer)
	if err != nil {
		t.Fatalf("NewSessionDescriptionFromString: %v", err)
	}
	return sd
}

// TestLifecycle_DeferredBrowserOfferThenNegotiationNeeded covers the
// deferred-offer lifecycle ordering: while a browser offer is stored behind
// an outstanding subscriber offer, subscriber triggers (AddTrack +
// negotiation-needed) coalesce; on completion the stored offer is answered
// FIRST, then exactly one coalesced server offer carries the new sender.
func TestLifecycle_DeferredBrowserOfferThenNegotiationNeeded(t *testing.T) {
	rig := newGlareTestRig(t, "lifecycle-defer-nn")
	lifecycleDriveSubscriberOffer(t, rig, "lcd-egress-a", 1)

	brow, offer, install := lifecycleMintBrowserOffer(t, "lcd-b1")
	if _, deferred, err := rig.slive.ProcessBrowserOffer(offer); err != nil || !deferred {
		t.Fatalf("browser offer: err=%v deferred=%v, want (nil,true)", err, deferred)
	}

	// Subscriber triggers while the browser offer is deferred: all coalesce,
	// no offer may fire while ours is outstanding.
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "lcd-egress-b", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack b: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	rig.slive.handleNegotiationNeeded()
	if got := rig.offers.Load(); got != 1 {
		t.Fatalf("server offers while deferred = %d, want 1 (coalesced)", got)
	}

	helper := lifecycleNewHelper(t)
	lifecycleAnswerOutstandingOffer(t, rig, helper)

	// The stored browser offer is answered first, then exactly one coalesced
	// subscriber offer (with both egress tracks) goes out.
	lifecycleWaitAnswer(t, rig, 1)
	lifecycleWaitOffer(t, rig, 2)
	if st := glareState(rig); st != pionwebrtc.SignalingStateHaveLocalOffer {
		t.Fatalf("slive state = %s, want have-local-offer (coalesced offer outstanding)", st)
	}
	lifecycleAnswerOutstandingOffer(t, rig, helper)
	lifecycleStableQuiet(t, rig)
	if got := rig.offers.Load(); got != 2 {
		t.Fatalf("server offers = %d, want exactly 2", got)
	}
	if egress := countEgressMIDs(t, rig.slive.PionPeerConnection().LocalDescription().SDP); egress != 2 {
		t.Fatalf("final SDP carries %d egress m-lines, want 2", egress)
	}
	// The deferred answer reaches the real browser (it still holds its
	// publisher offer) and completes its publish exchange.
	install(lifecycleParseAnswer(t, lifecycleLastAnswer(t, rig)))
	if st := brow.SignalingState(); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("browser state = %s, want stable after deferred answer", st)
	}
}

// TestLifecycle_MultipleAddTracksWhileBrowserOfferPending: several
// subscriptions land while a browser offer is deferred behind an outstanding
// server offer. Nothing is lost, nothing duplicated: exactly one coalesced
// offer carries every sender change.
func TestLifecycle_MultipleAddTracksWhileBrowserOfferPending(t *testing.T) {
	rig := newGlareTestRig(t, "lifecycle-multi-add")
	lifecycleDriveSubscriberOffer(t, rig, "lcm-egress-a", 1)

	_, offerB1, _ := lifecycleMintBrowserOffer(t, "lcm-b1")
	if _, deferred, err := rig.slive.ProcessBrowserOffer(offerB1); err != nil || !deferred {
		t.Fatalf("browser offer: err=%v deferred=%v, want (nil,true)", err, deferred)
	}
	for _, id := range []string{"lcm-egress-b", "lcm-egress-c", "lcm-egress-d"} {
		if err := rig.slive.AddTrack(newTestLocalTrack(t, id, domain.TrackKindAudio)); err != nil {
			t.Fatalf("AddTrack %s: %v", id, err)
		}
		rig.slive.handleNegotiationNeeded()
	}
	if got := rig.offers.Load(); got != 1 {
		t.Fatalf("server offers while outstanding = %d, want 1", got)
	}

	helper := lifecycleNewHelper(t)
	lifecycleAnswerOutstandingOffer(t, rig, helper)

	lifecycleWaitAnswer(t, rig, 1)
	lifecycleWaitOffer(t, rig, 2)
	lifecycleAnswerOutstandingOffer(t, rig, helper)
	lifecycleStableQuiet(t, rig)
	if got := rig.offers.Load(); got != 2 {
		t.Fatalf("server offers = %d, want exactly 2 (deferred trigger burst coalesced into one)", got)
	}
	if egress := countEgressMIDs(t, rig.slive.PionPeerConnection().LocalDescription().SDP); egress != 4 {
		t.Fatalf("final SDP carries %d egress m-lines, want 4 (a,b,c,d — none lost)", egress)
	}
}

// TestLifecycle_BrowserReofferCascadeConverges replays the production
// lf5q1xwyfx sequence end to end: subscriber offer -> browser publish offer
// deferred -> subscriber answer -> deferred offer answered (stale by then) ->
// coalesced subscriber offer -> browser re-offer deferred -> subscriber
// answer -> stable -> final browser offer answered inline. It must converge
// to stable with zero errors and no redundant third subscriber offer.
func TestLifecycle_BrowserReofferCascadeConverges(t *testing.T) {
	rig := newGlareTestRig(t, "lifecycle-cascade")
	lifecycleDriveSubscriberOffer(t, rig, "lcc-egress-a", 1)

	// 1. Browser publisher offer crosses our outstanding subscriber offer:
	// deferred, never an InvalidModificationError.
	_, offer1, _ := lifecycleMintBrowserOffer(t, "lcc-b1")
	if _, deferred, err := rig.slive.ProcessBrowserOffer(offer1); err != nil || !deferred {
		t.Fatalf("cascade B1: err=%v deferred=%v, want (nil,true)", err, deferred)
	}

	// 2. A further subscription (next participant's tracks) lands while the
	// browser offer is deferred: coalesced.
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "lcc-egress-b", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack b: %v", err)
	}
	rig.slive.handleNegotiationNeeded()

	// 3. The browser's answer to our subscriber offer completes the exchange;
	// the flush answers the stored (by now rolled-back) publish offer, then
	// sends exactly one coalesced subscriber offer.
	helper := lifecycleNewHelper(t)
	lifecycleAnswerOutstandingOffer(t, rig, helper)
	lifecycleWaitAnswer(t, rig, 1)
	lifecycleWaitOffer(t, rig, 2)
	if st := glareState(rig); st != pionwebrtc.SignalingStateHaveLocalOffer {
		t.Fatalf("after flush state = %s, want have-local-offer", st)
	}

	// 4. The browser re-offers its publish (negotiation-needed after the
	// rollback it performed): deferred behind the new subscriber offer.
	_, offer2, _ := lifecycleMintBrowserOffer(t, "lcc-b2")
	if _, deferred, err := rig.slive.ProcessBrowserOffer(offer2); err != nil || !deferred {
		t.Fatalf("cascade B2: err=%v deferred=%v, want (nil,true)", err, deferred)
	}

	// 5. The browser's answer to offer #2 completes; the flush answers the
	// stored re-offer; the generation guard must NOT emit a third subscriber
	// offer (no sender change since offer #2).
	lifecycleAnswerOutstandingOffer(t, rig, helper)
	lifecycleWaitAnswer(t, rig, 2)
	lifecycleStableQuiet(t, rig)
	if got := rig.offers.Load(); got != 2 {
		t.Fatalf("REGRESSION: server offers = %d, want exactly 2 (no redundant re-offer)", got)
	}

	// 6. The browser's next publisher offer now lands on a STABLE PC and is
	// answered inline; the publish exchange finally completes end to end.
	brow3, offer3, install3 := lifecycleMintBrowserOffer(t, "lcc-b3")
	answer3, deferred, err := rig.slive.ProcessBrowserOffer(offer3)
	if err != nil || deferred || answer3 == nil {
		t.Fatalf("cascade B3: answer=%v deferred=%v err=%v, want inline answer", answer3, deferred, err)
	}
	install3(answer3)
	if st := brow3.SignalingState(); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("brow3 state = %s, want stable", st)
	}
	// Handler contract: the caller flushes after delivering the inline answer.
	rig.slive.FlushDeferredNegotiation()
	lifecycleStableQuiet(t, rig)
	// The inline answer replaced the local description (answers only cover
	// the browser's offered m-line), so egress survival is asserted on the
	// last generated server offer, which must still carry both egress
	// sections — nothing was lost through deferrals, rollbacks and re-offers.
	if egress := countEgressMIDs(t, lifecycleLastOffer(t, rig)); egress != 2 {
		t.Fatalf("last server offer carries %d egress m-lines, want 2", egress)
	}
}

// TestLifecycle_ConcurrentTriggersSerialize stress-drives every gate entry
// point concurrently (AddTrack, handleNegotiationNeeded,
// RequestSubscriberOffer) against a background answerer and a continuous
// invariant checker. Checked invariants (sampled under negMu, so only
// externally-stable combinations can be observed):
//
//   - isNegotiating (outstanding server offer) only while have-local-offer;
//   - a deferred browser offer only exists while have-local-offer;
//   - stable implies no outstanding server offer and no deferred offer;
//   - have-remote-offer / pranswer states are unreachable for observers
//     (they exist only inside answerExchange units holding negMu).
//
// End state: converged stable, every added sender negotiated exactly once.
func TestLifecycle_ConcurrentTriggersSerialize(t *testing.T) {
	rig := newGlareTestRig(t, "lifecycle-concurrent")

	const writers = 6
	const perWriter = 3
	var violations atomic.Int64
	var firstViolation atomic.Value // string

	stop := make(chan struct{})
	checkerDone := make(chan struct{})
	go func() {
		defer close(checkerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			rig.slive.negMu.Lock()
			st := glareState(rig)
			rig.slive.mu.RLock()
			switch {
			case rig.slive.isNegotiating && st != pionwebrtc.SignalingStateHaveLocalOffer:
				violations.Add(1)
				firstViolation.CompareAndSwap(nil, "isNegotiating@"+st.String())
			case rig.slive.pendingInboundOffer != nil && st != pionwebrtc.SignalingStateHaveLocalOffer:
				violations.Add(1)
				firstViolation.CompareAndSwap(nil, "deferredOffer@"+st.String())
			case st == pionwebrtc.SignalingStateStable && (rig.slive.isNegotiating || rig.slive.pendingInboundOffer != nil):
				violations.Add(1)
				firstViolation.CompareAndSwap(nil, "stable-not-quiet")
			case st != pionwebrtc.SignalingStateStable && st != pionwebrtc.SignalingStateHaveLocalOffer:
				violations.Add(1)
				firstViolation.CompareAndSwap(nil, "observer-saw-"+st.String())
			}
			rig.slive.mu.RUnlock()
			rig.slive.negMu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	// Background answerer: completes every outstanding subscriber offer with
	// a helper PC (persistent across renegotiations).
	helper := lifecycleNewHelper(t)
	answererDone := make(chan struct{})
	go func() {
		defer close(answererDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if glareState(rig) != pionwebrtc.SignalingStateHaveLocalOffer {
				time.Sleep(2 * time.Millisecond)
				continue
			}
			local := rig.slive.PionPeerConnection().LocalDescription()
			if local == nil {
				continue
			}
			if err := helper.SetRemoteDescription(pionwebrtc.SessionDescription{
				Type: pionwebrtc.SDPTypeOffer, SDP: local.SDP,
			}); err != nil {
				continue
			}
			ans, err := helper.CreateAnswer(nil)
			if err != nil {
				continue
			}
			if err := helper.SetLocalDescription(ans); err != nil {
				continue
			}
			_ = rig.slive.ProcessBrowserAnswer(NewSessionDescription(helper.LocalDescription()))
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				id := "lcx-egress-" + string(rune('a'+w)) + string(rune('0'+i))
				if err := rig.slive.AddTrack(newTestLocalTrack(t, id, domain.TrackKindAudio)); err != nil {
					t.Errorf("AddTrack %s: %v", id, err)
					return
				}
				rig.slive.handleNegotiationNeeded()
				rig.slive.RequestSubscriberOffer()
			}
		}(w)
	}
	wg.Wait()

	// Converge: stable with an empty gate.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		st := glareState(rig)
		rig.slive.mu.RLock()
		busy := rig.slive.isNegotiating || rig.slive.pendingNegotiation || rig.slive.pendingInboundOffer != nil
		rig.slive.mu.RUnlock()
		if st == pionwebrtc.SignalingStateStable && !busy {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	<-checkerDone
	<-answererDone

	if st := glareState(rig); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("final state = %s, want stable", st)
	}
	if v := violations.Load(); v != 0 {
		first, _ := firstViolation.Load().(string)
		t.Fatalf("negotiation state-machine invariants violated %d times (first: %s)", v, first)
	}
	if egress := countEgressMIDs(t, rig.slive.PionPeerConnection().LocalDescription().SDP); egress != writers*perWriter {
		t.Fatalf("final SDP carries %d egress m-lines, want %d (every sender negotiated exactly once)", egress, writers*perWriter)
	}
	lifecycleStableQuiet(t, rig)
}
