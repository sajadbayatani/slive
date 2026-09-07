package signaling

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestServerOfferExtmapsMatchChromeIDs is the regression test for the Sep
// 2026 media-delivery outage: Slive's first subscriber offer (created before
// any remote description exists) declared TWCC on RTP header-extension id=1,
// while Chrome/Edge/Safari use id=1 for ssrc-audio-level. Any browser that
// then created an offer bundling a Slive-mapped m-line with its own
// (subscribe-before-publish order) got
// "A BUNDLE group contains a codec collision for header extension id=1",
// could never complete negotiation, and delivered zero media in both
// directions. Pion does not validate this; only real browsers do, so this
// test pins Slive's emitted IDs to Chrome's mapping for every shared URI.
func TestServerOfferExtmapsMatchChromeIDs(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })

	room, first := joinParticipant(t, h, "extmap-room", "extmap-a")
	_, late := joinParticipant(t, h, "extmap-room", "extmap-b")

	specs := glareSpecs("extmap-pub", 0xE100)
	for _, s := range specs {
		payload, _ := json.Marshal(PublishTrackRequest{RoomID: "extmap-room",
			ParticipantID: first.ID(),
			Track:         TrackInfo{ID: s.signaledID, Kind: s.kind, Source: s.source}})
		conn := newHeadlessConn(first.ID(), "extmap-room")
		if err := h.handleMessage(conn, room, first,
			&Message{Type: MessageTypePublishTrack, Data: payload}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	offers := make(chan string, 4)
	tap := func(msgType string, data interface{}) error {
		raw, _ := json.Marshal(data)
		var p struct {
			SDP string `json:"sdp"`
		}
		_ = json.Unmarshal(raw, &p)
		if msgType == "webrtc:offer" && p.SDP != "" {
			offers <- p.SDP
		}
		return nil
	}
	if _, err := h.ensurePeerConnection(late, tap); err != nil {
		t.Fatalf("ensure late PC: %v", err)
	}
	// Subscribe audio only first: the offer below is created with no prior
	// remote description, which is exactly the case that emitted id=1 TWCC.
	payload, _ := json.Marshal(SubscribeTrackRequest{RoomID: "extmap-room",
		ParticipantID: late.ID(), TrackID: specs[0].signaledID})
	conn := newHeadlessConn(late.ID(), "extmap-room")
	if err := h.handleMessage(conn, room, late,
		&Message{Type: MessageTypeSubscribeTrack, Data: payload}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	var offerSDP string
	select {
	case offerSDP = <-offers:
	case <-time.After(20 * time.Second):
		t.Fatal("no server offer for audio subscribe")
	}

	idToURI := map[string]string{}
	for _, line := range strings.Split(offerSDP, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "a=extmap:") {
			continue
		}
		rest := strings.TrimPrefix(line, "a=extmap:")
		parts := strings.SplitN(rest, " ", 2)
		if len(parts) != 2 {
			t.Fatalf("unparsable extmap line: %q", line)
		}
		id := strings.SplitN(parts[0], "/", 2)[0]
		if prev, dup := idToURI[id]; dup && prev != parts[1] {
			t.Fatalf("extmap id %s maps to both %q and %q", id, prev, parts[1])
		}
		idToURI[id] = parts[1]
	}

	// Chrome's audio m-line mapping for every URI Slive emits; a browser
	// bundling one of our m-lines with its own must see identical id->URI.
	want := map[string]string{
		"1": "urn:ietf:params:rtp-hdrext:ssrc-audio-level",
		"2": "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
		"3": "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01",
		"4": "urn:ietf:params:rtp-hdrext:sdes:mid",
	}
	for id, uri := range want {
		got, ok := idToURI[id]
		if !ok {
			t.Fatalf("Slive offer missing extmap id %s (want %q); got %v", id, uri, idToURI)
		}
		if got != uri {
			t.Fatalf("Slive offer extmap id %s = %q, want %q (Chrome mapping)", id, got, uri)
		}
	}
}
