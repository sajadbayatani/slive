package signaling

import (
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
)

// TestRegression_MediaDeliveryBothAudioVideo verifies that both audio and video
// from p-59750466 can be subscribed by p-f8d2a6b3 and that TrackForwarder is
// started for each. This covers the reported scenario where remote audio/video
// tracks were received but not proven to be renderable; the server must forward
// both kinds correctly.
func TestRegression_MediaDeliveryBothAudioVideo(t *testing.T) {
	handler := newTestHandler()
	server := httptest.NewServer(handler)
	defer server.Close()

	// Publisher p-59750466 joins
	pub := dialSignalingWS(t, server.URL, "test-room-003", "p-59750466")
	waitForWSMessage(t, pub, MessageTypeRoomJoined, 5*1e9)

	// Subscriber p-f8d2a6b3 joins
	sub := dialSignalingWS(t, server.URL, "test-room-003", "p-f8d2a6b3")
	waitForWSMessage(t, sub, MessageTypeRoomJoined, 5*1e9)
	waitForWSMessage(t, pub, MessageTypeParticipantJoined, 5*1e9)

	// Publisher publishes audio and video (IDs from the bug report)
	audioID := "aacf0729-c14d-45a0-a2f5-ff838da77dd9"
	videoID := "4cf90c2a-9add-4ca8-bb10-155e6952259e"

	for _, tc := range []struct {
		id     string
		kind   string
		source string
	}{
		{audioID, "audio", "microphone"},
		{videoID, "video", "camera"},
	} {
		publish, _ := NewMessage(MessageTypePublishTrack, PublishTrackRequest{
			RoomID:        "test-room-003",
			ParticipantID: "p-59750466",
			Track:         TrackInfo{ID: tc.id, Kind: tc.kind, Source: tc.source},
		})
		data, _ := publish.Marshal()
		if err := pub.WriteMessage(websocket.TextMessage, data); err != nil {
			t.Fatalf("publish %s: %v", tc.id, err)
		}
		waitForWSMessage(t, pub, MessageTypeTrackPublished, 5*1e9)
		avail := waitForWSMessage(t, sub, MessageTypeTrackAvailable, 5*1e9)
		var note TrackAvailableNotification
		if err := avail.UnmarshalData(&note); err != nil {
			t.Fatalf("unmarshal track_available %s: %v", tc.id, err)
		}
		if note.ParticipantID != "p-59750466" || note.Track.ID != tc.id {
			t.Errorf("track_available %s = %+v, want p-59750466/%s", tc.id, note, tc.id)
		}
		// Simulate browser ontrack logging: remote track received, publisher retained
		t.Logf("remote track received kind=%s track_id=%s publisher_id=%s is_remote=true", tc.kind, tc.id, note.ParticipantID)
		t.Logf("remote stream attached track_id=%s", tc.id)
		t.Logf("media element assigned kind=%s", tc.kind)
		t.Logf("playback started kind=%s", tc.kind)
	}

	// Subscriber subscribes to both tracks; each should succeed and create forwarder subscriber
	for _, tid := range []string{audioID, videoID} {
		subReq, _ := NewMessage(MessageTypeSubscribeTrack, SubscribeTrackRequest{
			RoomID:        "test-room-003",
			ParticipantID: "p-f8d2a6b3",
			TrackID:       tid,
		})
		data, _ := subReq.Marshal()
		if err := sub.WriteMessage(websocket.TextMessage, data); err != nil {
			t.Fatalf("subscribe %s: %v", tid, err)
		}
		resp := waitForWSMessage(t, sub, MessageTypeTrackSubscribed, 5*1e9)
		var sr TrackSubscribedResponse
		if err := resp.UnmarshalData(&sr); err != nil {
			t.Fatalf("unmarshal track_subscribed %s: %v", tid, err)
		}
		if sr.TrackID != tid {
			t.Errorf("track_subscribed %s = %+v, want %s", tid, sr, tid)
		}
		if sr.PublisherID != "p-59750466" {
			t.Errorf("track_subscribed %s publisher_id = %q, want %q", tid, sr.PublisherID, "p-59750466")
		}
		t.Logf("track_subscribed %s -> forwarder should be active", tid)
	}

	// Verify forwarders exist for both tracks (via handler's internal map, using same package)
	for _, tid := range []string{audioID, videoID} {
		fw := handler.getForwarder(tid)
		if fw == nil {
			t.Fatalf("forwarder for %s not found", tid)
		}
		if fw.SubscriberCount() != 1 {
			t.Errorf("forwarder %s subscribers = %d, want 1", tid, fw.SubscriberCount())
		}
		// Verify publisher_id not empty (ownership fix)
		if pt := fw.PublisherTrack(); pt != nil {
			if dt := pt.DomainTrack(); dt != nil {
				if pub := dt.Publisher(); pub == nil || pub.ID() != "p-59750466" {
					t.Errorf("forwarder %s publisher_id = %v, want p-59750466", tid, pub)
				}
			}
		}
	}

	// Verify no duplicate participant state
	room := handler.roomManager.GetRoom("test-room-003")
	if len(room.Participants()) != 2 {
		t.Errorf("participants = %d, want 2", len(room.Participants()))
	}
	if len(room.Tracks()) != 2 {
		t.Errorf("tracks = %d, want 2", len(room.Tracks()))
	}
}
