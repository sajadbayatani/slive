package signaling

import (
	"testing"

	"github.com/pion/rtcp"
)

// TestRTCPFeedbackIdentity pins the DIAG-KEYFRAME observer filter: PLI, FIR
// and NACK (with sender/media SSRCs) are observed; TWCC, SR/RR and REMB are
// skipped so the rtcp_feedback_observed log stays focused on the
// keyframe-request path.
func TestRTCPFeedbackIdentity(t *testing.T) {
	kind, sender, media, ok := rtcpFeedbackIdentity(&rtcp.PictureLossIndication{SenderSSRC: 11, MediaSSRC: 22})
	if !ok || kind != "PLI" || sender != 11 || media != 22 {
		t.Errorf("PLI identity = (%q,%d,%d,%v), want (PLI,11,22,true)", kind, sender, media, ok)
	}
	kind, sender, media, ok = rtcpFeedbackIdentity(&rtcp.FullIntraRequest{SenderSSRC: 33, MediaSSRC: 44})
	if !ok || kind != "FIR" || sender != 33 || media != 44 {
		t.Errorf("FIR identity = (%q,%d,%d,%v), want (FIR,33,44,true)", kind, sender, media, ok)
	}
	kind, _, _, ok = rtcpFeedbackIdentity(&rtcp.TransportLayerNack{SenderSSRC: 55, MediaSSRC: 66})
	if !ok || kind != "NACK" {
		t.Errorf("NACK identity = (%q,%v), want (NACK,true)", kind, ok)
	}
	for _, p := range []rtcp.Packet{
		&rtcp.TransportLayerCC{},
		&rtcp.SenderReport{},
		&rtcp.ReceiverReport{},
		&rtcp.ReceiverEstimatedMaximumBitrate{},
		&rtcp.SourceDescription{},
	} {
		if _, _, _, ok := rtcpFeedbackIdentity(p); ok {
			t.Errorf("%T should be skipped by the observer filter", p)
		}
	}
}
