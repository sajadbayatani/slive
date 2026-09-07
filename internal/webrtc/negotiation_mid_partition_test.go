package webrtc

import (
	"strconv"
	"strings"
	"testing"
	"time"

	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// newTestLocalTrack builds a local audio TrackLocal wrapped in the WebRTCTrack
// wrapper (with a real domain track, since WebRTCTrack.ID dereferences it) so
// tests can drive PeerConnection.AddTrack with production geometry.
func newTestLocalTrack(t *testing.T, id string, kind domain.TrackKind) *WebRTCTrack {
	t.Helper()
	domainTrack, err := domain.NewTrack(id, kind, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("domain.NewTrack(%s): %v", id, err)
	}
	local, err := pionwebrtc.NewTrackLocalStaticRTP(
		pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		id, id+"-stream",
	)
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP(%s): %v", id, err)
	}
	codec := pionwebrtc.RTPCodecParameters{
		RTPCodecCapability: pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		PayloadType:        111,
	}
	return NewWebRTCTrack(domainTrack, local, codec)
}

// waitForOutstandingOffer blocks until the rig's slive PC has a live server
// offer (have-local-offer) and at least want offers were sent.
func waitForOutstandingOffer(r *glareTestRig, want int64) {
	r.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for (glareState(r) != pionwebrtc.SignalingStateHaveLocalOffer || r.offers.Load() < want) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if st := glareState(r); st != pionwebrtc.SignalingStateHaveLocalOffer {
		r.t.Fatalf("slive state = %s, want have-local-offer", st)
	}
	if got := r.offers.Load(); got < want {
		r.t.Fatalf("server offers = %d, want >= %d", got, want)
	}
}

// midsInSDP extracts every a=mid value from an SDP body, in order.
func midsInSDP(sdp string) []string {
	var mids []string
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "a=mid:") {
			mids = append(mids, strings.TrimPrefix(line, "a=mid:"))
		}
	}
	return mids
}

// transceiverMIDs maps the live transceivers' MIDs, skipping unassigned ones.
func transceiverMIDs(pc *PeerConnection) []string {
	var mids []string
	for _, tr := range pc.PionPeerConnection().GetTransceivers() {
		if mid := tr.Mid(); mid != "" {
			mids = append(mids, mid)
		}
	}
	return mids
}

func assertUniqueMIDs(t *testing.T, what string, mids []string) {
	t.Helper()
	seen := make(map[string]bool, len(mids))
	for _, m := range mids {
		if seen[m] {
			t.Errorf("%s: duplicate MID %q (mids=%v)", what, m, mids)
		}
		seen[m] = true
	}
}

// egressSendersFor returns the live transceivers carrying the wrapper tracks
// named in ids. Transceivers whose sender has another TrackLocal (e.g. the
// sendrecv dummy sender pion builds inside the rig's AddTransceiverFromKind
// setup call, at mid 0) are excluded: production PCs never hold one, so
// assertions stay on Slive's own egress senders.
func egressSendersFor(pc *PeerConnection, ids ...string) []*pionwebrtc.RTPTransceiver {
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	var out []*pionwebrtc.RTPTransceiver
	for _, tr := range pc.PionPeerConnection().GetTransceivers() {
		if tr.Sender() == nil || tr.Sender().Track() == nil {
			continue
		}
		if want[tr.Sender().Track().ID()] {
			out = append(out, tr)
		}
	}
	return out
}

// countEgressMIDs counts a=mid values from the server partition in an SDP.
func countEgressMIDs(t *testing.T, sdp string) int {
	t.Helper()
	n := 0
	for _, m := range midsInSDP(sdp) {
		v, err := strconv.Atoi(m)
		if err != nil {
			t.Fatalf("non-numeric MID %q", m)
		}
		if v >= egressMIDBase {
			n++
		}
	}
	return n
}

// TestMIDPartition_EgressOfferUsesServerPartition drives the bug geometry:
// the subscriber exchange runs FIRST on the unified PC, so an unpartitioned
// offer generator would hand the egress media sections the low MIDs a later
// browser publish offer (always numbered from 0 on its first offer) would
// then collide with. The egress section must instead carry a MID from
// Slive's server partition.
func TestMIDPartition_EgressOfferUsesServerPartition(t *testing.T) {
	rig := newGlareTestRig(t, "mid-partition")
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "sub-audio-a", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	waitForOutstandingOffer(rig, 1)

	egressMIDs := midsInSDP(rig.slive.PionPeerConnection().LocalDescription().SDP)
	foundEgress := false
	for _, m := range egressMIDs {
		n, err := strconv.Atoi(m)
		if err != nil {
			t.Fatalf("non-numeric MID %q in subscriber offer", m)
		}
		if n >= egressMIDBase {
			foundEgress = true
		}
	}
	if !foundEgress {
		t.Errorf("subscriber offer has no egress MID >= %d (mids=%v)", egressMIDBase, egressMIDs)
	}
	rig.answerSliveOffer()

	// Browser publish exchange afterwards: its publish media sections must
	// not collide with the egress MIDs already in use.
	answer, deferred, err := rig.slive.ProcessBrowserOffer(rig.browserOffer())
	if err != nil {
		t.Fatalf("ProcessBrowserOffer: %v", err)
	}
	if deferred {
		t.Fatal("publish offer deferred on stable PC, want inline answer")
	}
	rig.browserAcceptAnswer(answer)
	if st := glareState(rig); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("slive state = %s, want stable", st)
	}

	mids := transceiverMIDs(rig.slive)
	assertUniqueMIDs(t, "transceivers after publish exchange", mids)
	// The egress sender keeps its reserved MID across the publish exchange:
	// a replacement must never land on a publish media section.
	egress := egressSendersFor(rig.slive, "sub-audio-a")
	if len(egress) != 1 {
		t.Fatalf("egress senders for sub-audio-a = %d, want 1", len(egress))
	}
	if n := countEgressMIDs(t, rig.slive.PionPeerConnection().LocalDescription().SDP); n < 1 {
		t.Errorf("negotiated SDP carries %d egress MIDs, want >= 1", n)
	}
	mid, err := strconv.Atoi(egress[0].Mid())
	if err != nil {
		t.Fatalf("egress transceiver MID %q non-numeric", egress[0].Mid())
	}
	if mid < egressMIDBase {
		t.Errorf("egress sender MID %d < egressMIDBase %d", mid, egressMIDBase)
	}
}

// TestMIDPartition_ReconcileSwapKeepsUniqueMIDs reproduces the codec
// reconciliation geometry: a placeholder-era egress track is removed and a
// fresh TrackLocal (real codec) is added after the first exchange. The new
// sender must be negotiated without ever duplicating a MID already in use.
func TestMIDPartition_ReconcileSwapKeepsUniqueMIDs(t *testing.T) {
	rig := newGlareTestRig(t, "mid-reconcile")
	first := newTestLocalTrack(t, "sub-audio-a", domain.TrackKindAudio)
	if err := rig.slive.AddTrack(first); err != nil {
		t.Fatalf("AddTrack first: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	waitForOutstandingOffer(rig, 1)
	rig.answerSliveOffer()

	// Codec reconcile: remove the stale egress sender, add the fresh one,
	// then drive one offer through the gate covering both changes.
	if err := rig.slive.RemoveTrack(first.ID()); err != nil {
		t.Fatalf("RemoveTrack: %v", err)
	}
	second := newTestLocalTrack(t, "sub-audio-b", domain.TrackKindAudio)
	if err := rig.slive.AddTrack(second); err != nil {
		t.Fatalf("AddTrack second: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	waitForOutstandingOffer(rig, 2)
	rig.answerSliveOffer()

	mids := transceiverMIDs(rig.slive)
	assertUniqueMIDs(t, "transceivers after reconcile", mids)
	egress := egressSendersFor(rig.slive, "sub-audio-a", "sub-audio-b")
	if len(egress) != 1 {
		t.Errorf("live egress senders = %d, want 1 (stale sender must be gone)", len(egress))
		return
	}
	mid, err := strconv.Atoi(egress[0].Mid())
	if err != nil {
		t.Fatalf("reconciled sender MID %q non-numeric", egress[0].Mid())
	}
	if mid < egressMIDBase {
		t.Errorf("reconciled sender MID %d < egressMIDBase %d", mid, egressMIDBase)
	}
}

// TestGenerationGuard_StaleFlushSkipped proves a trigger that carries no
// sender change beyond the last created offer cannot install or send an
// obsolete duplicate offer, while a real change still produces exactly one.
func TestGenerationGuard_StaleFlushSkipped(t *testing.T) {
	rig := newGlareTestRig(t, "gen-guard")
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "sub-audio-a", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	waitForOutstandingOffer(rig, 1)

	// The async echo of the same AddTrack lands behind the outstanding offer:
	// it must only coalesce, never offer.
	rig.slive.handleNegotiationNeeded()
	rig.slive.handleNegotiationNeeded()
	time.Sleep(200 * time.Millisecond)
	if got := rig.offers.Load(); got != 1 {
		t.Fatalf("offers while outstanding = %d, want 1", got)
	}

	// Answering the offer flushes the coalesced echo — the generation guard
	// must skip it instead of sending a second identical offer.
	rig.answerSliveOffer()
	time.Sleep(200 * time.Millisecond)
	if got := rig.offers.Load(); got != 1 {
		t.Fatalf("offers after stale flush = %d, want 1 (guard must skip redundant offer)", got)
	}
	rig.slive.mu.RLock()
	pending := rig.slive.pendingNegotiation
	rig.slive.mu.RUnlock()
	if pending {
		t.Error("pendingNegotiation still armed after guarded flush, want false")
	}

	// Negative control: a REAL change (new sender) must still offer.
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "sub-audio-b", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack second: %v", err)
	}
	waitForOutstandingOffer(rig, 2)
	if got := rig.offers.Load(); got != 2 {
		t.Fatalf("offers after real change = %d, want exactly 2", got)
	}
	rig.answerSliveOffer()
	if st := glareState(rig); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("slive state = %s, want stable", st)
	}
}

// TestSyncDrive_SubscribesCoalesceToSingleOffer proves that subscribe events
// arriving while an offer is outstanding coalesce into exactly one follow-up
// offer — never one per event, never a lost event.
func TestSyncDrive_SubscribesCoalesceToSingleOffer(t *testing.T) {
	rig := newGlareTestRig(t, "sync-drive")
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "sub-audio-a", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack first: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	waitForOutstandingOffer(rig, 1)

	// Two more subscriptions land while the first exchange is in flight.
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "sub-audio-b", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack second: %v", err)
	}
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "sub-audio-c", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack third: %v", err)
	}
	// pion aborts its callback while not stable; the sync drive re-arms the
	// need explicitly, as the handler does after AddTrack.
	rig.slive.handleNegotiationNeeded()
	rig.slive.handleNegotiationNeeded()
	time.Sleep(200 * time.Millisecond)
	if got := rig.offers.Load(); got != 1 {
		t.Fatalf("offers while first exchange in flight = %d, want 1", got)
	}

	// Completion flushes the coalesced subscribes as EXACTLY ONE offer: the
	// answer lands and the flush re-enters have-local-offer immediately, so
	// the PC never rests at stable between the two exchanges.
	rig.answerOutstandingOffer()
	waitForOutstandingOffer(rig, 2)
	if got := rig.offers.Load(); got != 2 {
		t.Fatalf("offers after flush = %d, want exactly 2 (coalesced, not per-subscribe)", got)
	}
	if st := glareState(rig); st != pionwebrtc.SignalingStateHaveLocalOffer {
		t.Fatalf("slive state = %s, want have-local-offer (coalesced offer outstanding)", st)
	}
	rig.answerSliveOffer()
	if st := glareState(rig); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("slive state = %s, want stable", st)
	}
}

// TestDeferredAnswer_NotEmittedBeforeStable covers the deferred-INFO rule: an
// inbound browser offer stored behind an outstanding server exchange must not
// produce an answer until the PC is stable again, and then exactly once.
func TestDeferredAnswer_NotEmittedBeforeStable(t *testing.T) {
	rig := newGlareTestRig(t, "deferred-answer")
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "sub-audio-a", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	waitForOutstandingOffer(rig, 1)

	inbound := rig.browserOffer()
	answer, deferred, err := rig.slive.ProcessBrowserOffer(inbound)
	if err != nil {
		t.Fatalf("ProcessBrowserOffer: %v", err)
	}
	if !deferred || answer != nil {
		t.Fatalf("got answer=%v deferred=%v, want (nil, true)", answer, deferred)
	}
	if got := rig.answer.Load(); got != 0 {
		t.Fatalf("answers emitted while have-local-offer = %d, want 0", got)
	}

	// Complete the subscriber exchange with a helper browser (the loopback
	// browser is itself have-local-offer and cannot answer).
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
	hAns, err := helper.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("helper CreateAnswer: %v", err)
	}
	if err := helper.SetLocalDescription(hAns); err != nil {
		t.Fatalf("helper SetLocal: %v", err)
	}
	if err := rig.slive.ProcessBrowserAnswer(NewSessionDescription(helper.LocalDescription())); err != nil {
		t.Fatalf("ProcessBrowserAnswer: %v", err)
	}

	// Stable again: the stored offer is answered with priority, exactly once.
	deadline := time.Now().Add(5 * time.Second)
	for rig.answer.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if got := rig.answer.Load(); got != 1 {
		t.Fatalf("deferred answers delivered = %d, want exactly 1", got)
	}
	if st := glareState(rig); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("slive state = %s, want stable", st)
	}
	rig.slive.mu.RLock()
	stored := rig.slive.pendingInboundOffer != nil
	rig.slive.mu.RUnlock()
	if stored {
		t.Error("pendingInboundOffer not cleared after deferred answer")
	}
}

// TestMIDPartition_ReofferWithNewSectionIsAnswerable replays the live
// bot2 rejection geometry: a subscriber offer for audio (mid 100) is
// answered, then a re-offer adds a video section (mid 101) while re-offering
// the established audio section (direction now sendonly). A strict browser
// must be able to apply it; failure here means Slive emits unanswerable
// SDP (BUNDLE/MID/direction regression).
func TestMIDPartition_ReofferWithNewSectionIsAnswerable(t *testing.T) {
	rig := newGlareTestRig(t, "mid-reoffer")
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "mid-reoffer-audio", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack audio: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	waitForOutstandingOffer(rig, 1)
	rig.answerSliveOffer()

	// Second subscription: re-offer (established audio, now sendonly) plus
	// a brand-new video section.
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "mid-reoffer-video", domain.TrackKindVideo)); err != nil {
		t.Fatalf("AddTrack video: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	waitForOutstandingOffer(rig, 2)
	second := rig.slive.PionPeerConnection().LocalDescription().SDP
	mids := midsInSDP(second)
	t.Logf("re-offer mids=%v", mids)
	seen := map[string]struct{}{}
	for _, m := range mids {
		if _, dup := seen[m]; dup {
			t.Fatalf("duplicate MID %q in re-offer: %v", m, mids)
		}
		seen[m] = struct{}{}
	}
	// A fresh browser (no shared state) must accept the re-offer shape the
	// same way Chrome must: setRemote must not reject it.
	fresh, err := pionwebrtc.NewPeerConnection(pionwebrtc.Configuration{
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	})
	if err != nil {
		t.Fatalf("fresh browser pc: %v", err)
	}
	defer fresh.Close()
	if _, err := fresh.AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("fresh transceiver: %v", err)
	}
	if err := fresh.SetRemoteDescription(pionwebrtc.SessionDescription{Type: pionwebrtc.SDPTypeOffer, SDP: second}); err != nil {
		t.Fatalf("REGRESSION: fresh browser rejects re-offer: %v", err)
	}
	// And the loopback browser answers it, completing the cycle.
	rig.answerSliveOffer()
	if st := glareState(rig); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("slive state = %s, want stable", st)
	}
}
