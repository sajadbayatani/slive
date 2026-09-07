package signaling

// Late-joiner discovery regression tests.
//
// Root cause under test: RoomJoinedResponse used to carry participants but no
// tracks, and the frontend only subscribes on track_available broadcasts —
// which a late joiner already missed. The join state must therefore include
// currently published tracks so the joiner can subscribe immediately.

import (
	"encoding/json"
	"testing"
)

// joinStateFor runs the real sendRoomJoined path over a headless connection
// and returns the decoded RoomJoinedResponse the joiner would receive.
func joinStateFor(t *testing.T, h *Handler, roomID string, participantID string) RoomJoinedResponse {
	t.Helper()
	room, err := h.roomManager.GetOrCreateRoom(roomID)
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}
	p := room.GetParticipant(participantID)
	if p == nil {
		t.Fatalf("participant %q not in room", participantID)
	}
	conn := newHeadlessConn(participantID, roomID)
	if err := h.sendRoomJoined(conn, room, p); err != nil {
		t.Fatalf("sendRoomJoined: %v", err)
	}
	for _, m := range drainMessages(conn) {
		if m.Type == MessageTypeRoomJoined {
			var resp RoomJoinedResponse
			if err := m.UnmarshalData(&resp); err != nil {
				t.Fatalf("unmarshal room_joined: %v", err)
			}
			return resp
		}
	}
	t.Fatal("no room_joined message sent")
	return RoomJoinedResponse{}
}

func trackKindsByID(resp RoomJoinedResponse) map[string]PublishedTrackInfo {
	out := make(map[string]PublishedTrackInfo, len(resp.Tracks))
	for _, ti := range resp.Tracks {
		out[ti.Track.ID] = ti
	}
	return out
}

// TestLateJoinEmptyRoom: joining before anyone publishes yields zero tracks.
func TestLateJoinEmptyRoom(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	joinParticipant(t, h, "lj-empty", "lj-alice")

	resp := joinStateFor(t, h, "lj-empty", "lj-alice")
	if len(resp.Tracks) != 0 {
		t.Errorf("tracks = %+v, want empty", resp.Tracks)
	}
}

// TestLateJoinDiscoversExistingTracks: A publishes audio+video, B joins
// afterward and receives both with publisher/kind/source intact.
func TestLateJoinDiscoversExistingTracks(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	room, alice := joinParticipant(t, h, "lj-two", "lj-a")
	_, _ = h.ensurePeerConnection(alice, channelSender(make(chan string, 8)))
	mustPublishTrackViaHandler(t, h, room, alice, "lj-a-audio", "audio", "microphone")
	mustPublishTrackViaHandler(t, h, room, alice, "lj-a-video", "video", "camera")

	_, bob := joinParticipant(t, h, "lj-two", "lj-b")
	_, _ = h.ensurePeerConnection(bob, channelSender(make(chan string, 8)))
	resp := joinStateFor(t, h, "lj-two", "lj-b")

	got := trackKindsByID(resp)
	if len(got) != 2 {
		t.Fatalf("tracks = %+v, want 2 existing tracks", resp.Tracks)
	}
	audio, ok := got["lj-a-audio"]
	if !ok {
		t.Fatal("missing existing audio track")
	}
	if audio.ParticipantID != "lj-a" || audio.Track.Kind != "audio" || audio.Track.Source != "microphone" {
		t.Errorf("audio entry = %+v, want publisher lj-a audio/microphone", audio)
	}
	video, ok := got["lj-a-video"]
	if !ok {
		t.Fatal("missing existing video track")
	}
	if video.ParticipantID != "lj-a" || video.Track.Kind != "video" || video.Track.Source != "camera" {
		t.Errorf("video entry = %+v, want publisher lj-a video/camera", video)
	}
}

// TestLateJoinMultiplePublishers: tracks from every existing participant are
// included.
func TestLateJoinMultiplePublishers(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	room, alice := joinParticipant(t, h, "lj-multi", "lj-ma")
	_, _ = h.ensurePeerConnection(alice, channelSender(make(chan string, 8)))
	mustPublishTrackViaHandler(t, h, room, alice, "lj-ma-audio", "audio", "microphone")
	mustPublishTrackViaHandler(t, h, room, alice, "lj-ma-video", "video", "camera")
	_, carol := joinParticipant(t, h, "lj-multi", "lj-mc")
	_, _ = h.ensurePeerConnection(carol, channelSender(make(chan string, 8)))
	mustPublishTrackViaHandler(t, h, room, carol, "lj-mc-video", "video", "camera")

	_, dave := joinParticipant(t, h, "lj-multi", "lj-md")
	_, _ = h.ensurePeerConnection(dave, channelSender(make(chan string, 8)))
	resp := joinStateFor(t, h, "lj-multi", "lj-md")

	got := trackKindsByID(resp)
	if len(got) != 3 {
		t.Fatalf("tracks = %+v, want 3 existing tracks", resp.Tracks)
	}
	for _, id := range []string{"lj-ma-audio", "lj-ma-video", "lj-mc-video"} {
		if _, ok := got[id]; !ok {
			t.Errorf("missing existing track %q", id)
		}
	}
	if got["lj-mc-video"].ParticipantID != "lj-mc" {
		t.Errorf("lj-mc-video publisher = %q, want lj-mc", got["lj-mc-video"].ParticipantID)
	}
}

// TestLateJoinExcludesOwnTracks: a rejoining publisher does not discover its
// own tracks (no self-subscribe).
func TestLateJoinExcludesOwnTracks(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	room, alice := joinParticipant(t, h, "lj-own", "lj-oa")
	_, _ = h.ensurePeerConnection(alice, channelSender(make(chan string, 8)))
	mustPublishTrackViaHandler(t, h, room, alice, "lj-oa-video", "video", "camera")
	_, bob := joinParticipant(t, h, "lj-own", "lj-ob")
	_, _ = h.ensurePeerConnection(bob, channelSender(make(chan string, 8)))
	mustPublishTrackViaHandler(t, h, room, bob, "lj-ob-video", "video", "camera")

	// Bob rejoins: sees Alice's track, not his own.
	resp := joinStateFor(t, h, "lj-own", "lj-ob")
	got := trackKindsByID(resp)
	if len(got) != 1 {
		t.Fatalf("tracks = %+v, want only Alice's track", resp.Tracks)
	}
	if _, ok := got["lj-oa-video"]; !ok {
		t.Error("missing Alice's track in Bob's join state")
	}
	// Alice symmetrically sees only Bob's.
	respA := joinStateFor(t, h, "lj-own", "lj-oa")
	gotA := trackKindsByID(respA)
	if len(gotA) != 1 {
		t.Fatalf("alice tracks = %+v, want only Bob's track", respA.Tracks)
	}
	if _, ok := gotA["lj-ob-video"]; !ok {
		t.Error("missing Bob's track in Alice's join state")
	}
}

// TestLateJoinPostJoinPublishVisible: a track published after B joins is both
// broadcast (existing flow) and visible to the NEXT joiner through the same
// registry both mechanisms read.
func TestLateJoinPostJoinPublishVisible(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	room, alice := joinParticipant(t, h, "lj-late-pub", "lj-pa")
	_, _ = h.ensurePeerConnection(alice, channelSender(make(chan string, 8)))
	_, bob := joinParticipant(t, h, "lj-late-pub", "lj-pb")
	_, _ = h.ensurePeerConnection(bob, channelSender(make(chan string, 8)))

	if resp := joinStateFor(t, h, "lj-late-pub", "lj-pb"); len(resp.Tracks) != 0 {
		t.Fatalf("tracks before publish = %+v, want empty", resp.Tracks)
	}
	mustPublishTrackViaHandler(t, h, room, alice, "lj-pa-video", "video", "camera")

	resp := joinStateFor(t, h, "lj-late-pub", "lj-pb")
	got := trackKindsByID(resp)
	if _, ok := got["lj-pa-video"]; !ok {
		t.Errorf("tracks after publish = %+v, want lj-pa-video", resp.Tracks)
	}
}

// TestLateJoinUnpublishedExcluded: unpublished tracks vanish from the
// registry and are not advertised to future joiners.
func TestLateJoinUnpublishedExcluded(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	room, alice := joinParticipant(t, h, "lj-unpub", "lj-ua")
	_, _ = h.ensurePeerConnection(alice, channelSender(make(chan string, 8)))
	mustPublishTrackViaHandler(t, h, room, alice, "lj-ua-video", "video", "camera")
	mustPublishTrackViaHandler(t, h, room, alice, "lj-ua-audio", "audio", "microphone")
	mustUnpublishViaHandler(t, h, room, alice, "lj-ua-video")

	_, bob := joinParticipant(t, h, "lj-unpub", "lj-ub")
	_, _ = h.ensurePeerConnection(bob, channelSender(make(chan string, 8)))
	resp := joinStateFor(t, h, "lj-unpub", "lj-ub")
	got := trackKindsByID(resp)
	if len(got) != 1 {
		t.Fatalf("tracks = %+v, want only the still-published audio track", resp.Tracks)
	}
	if _, ok := got["lj-ua-audio"]; !ok {
		t.Error("missing surviving audio track")
	}
}

// TestLateJoinDuplicateSubscribeSafe: discovering the same track twice (join
// state + a later track_available) cannot create duplicate subscriptions:
// the domain rejects the second subscribe and the forwarder keeps one entry.
func TestLateJoinDuplicateSubscribeSafe(t *testing.T) {
	h := newTestHandler()
	t.Cleanup(func() { _ = h.Shutdown() })
	room, alice := joinParticipant(t, h, "lj-dup", "lj-da")
	_, _ = h.ensurePeerConnection(alice, channelSender(make(chan string, 8)))
	mustPublishTrackViaHandler(t, h, room, alice, "lj-da-video", "video", "camera")
	_, bob := joinParticipant(t, h, "lj-dup", "lj-db")
	subPC, _ := h.ensurePeerConnection(bob, channelSender(make(chan string, 8)))

	mustSubscribeTrackViaHandler(t, h, room, bob, "lj-da-video")
	// Second subscribe for the same track must fail loudly, not duplicate.
	payload, _ := json.Marshal(SubscribeTrackRequest{
		RoomID:        room.ID(),
		ParticipantID: bob.ID(),
		TrackID:       "lj-da-video",
	})
	conn := newHeadlessConn(bob.ID(), room.ID())
	if err := h.handleMessage(conn, room, bob, &Message{Type: MessageTypeSubscribeTrack, Data: payload}); err == nil {
		t.Error("duplicate subscribe_track should fail, got nil error")
	}
	fw := h.getForwarder("lj-da-video")
	if fw == nil {
		t.Fatal("missing forwarder")
	}
	if n := fw.SubscriberCount(); n != 1 {
		t.Errorf("forwarder subscribers = %d, want exactly 1", n)
	}
	_ = subPC
}
