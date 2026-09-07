package webrtc

import (
	"testing"

	"github.com/pion/rtcp"
)

// TestSetRTCPObserverHook pins the keyframe-relay observer contract: the
// hook registered via SetRTCPObserver receives every RTCP packet read from
// this PC's senders, and passing nil clears it.
func TestSetRTCPObserverHook(t *testing.T) {
	pc := newTestPeerConnection(t, "obs-1", "Obs")
	var seen []rtcp.Packet
	pc.SetRTCPObserver(func(trackID string, p rtcp.Packet) {
		if trackID != "track-x" {
			t.Errorf("observer trackID = %q, want track-x", trackID)
		}
		seen = append(seen, p)
	})
	// Invoke the stored hook the same way the AddTrack reader loop does.
	pc.mu.RLock()
	fn := pc.rtcpObserver
	pc.mu.RUnlock()
	if fn == nil {
		t.Fatal("observer not stored")
	}
	fn("track-x", &rtcp.PictureLossIndication{SenderSSRC: 7, MediaSSRC: 8})
	if len(seen) != 1 {
		t.Fatalf("observer calls = %d, want 1", len(seen))
	}
	pli, ok := seen[0].(*rtcp.PictureLossIndication)
	if !ok || pli.SenderSSRC != 7 || pli.MediaSSRC != 8 {
		t.Errorf("observer packet = %#v, want PLI 7->8", seen[0])
	}
	pc.SetRTCPObserver(nil)
	pc.mu.RLock()
	cleared := pc.rtcpObserver == nil
	pc.mu.RUnlock()
	if !cleared {
		t.Error("nil observer should clear the hook")
	}
}
