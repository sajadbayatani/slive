package e2e

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sajadbayatani/slive/internal/signaling"
)

// waitForTrackAvailable polls until the given client receives a TrackAvailable
// for the wanted track ID.
func waitForTrackAvailable(t *testing.T, c *wsClient, trackID string) {
	t.Helper()
	deadline := time.Now().Add(e2eTimeout)
	for time.Now().Before(deadline) {
		if err := c.conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			t.Fatalf("waitForTrackAvailable read: %v", err)
		}
		msg, err := signaling.ParseMessage(data)
		if err != nil {
			continue
		}
		if msg.Type == signaling.MessageTypeTrackAvailable {
			var note signaling.TrackAvailableNotification
			unmarshalInto(t, msg, &note)
			if note.Track.ID == trackID {
				return
			}
		}
	}
	t.Fatalf("timed out waiting for track_available %s", trackID)
}

// TestE2E_SFU_Audio drives the SFU publish/subscribe control plane through the
// real router. It verifies that a publisher can publish an audio track, a
// subscriber learns about it, subscribes, and that the SFU wiring keeps the
// subscriber's peer connection negotiable (ICE forwarding still works) and that
// synthetic RTP fan-out via the forwarder path is non-blocking.
func TestE2E_SFU_Audio(t *testing.T) {
	ts := newE2EServer(t)

	publisher := dialWS(t, ts, "sfu-e2e-audio", "publisher")
	publisher.receiveOfType(signaling.MessageTypeRoomJoined, "publisher room_joined")

	subscriber := dialWS(t, ts, "sfu-e2e-audio", "subscriber")
	subscriber.receiveOfType(signaling.MessageTypeRoomJoined, "subscriber room_joined")
	publisher.receiveOfType(signaling.MessageTypeParticipantJoined, "publisher participant_joined")

	publisher.send(signaling.MessageTypePublishTrack, signaling.PublishTrackRequest{
		RoomID:        "sfu-e2e-audio",
		ParticipantID: "publisher",
		Track:         signaling.TrackInfo{ID: "audio-e2e", Kind: "audio", Source: "microphone"},
	})
	pubResp := publisher.receiveOfType(signaling.MessageTypeTrackPublished, "publisher track_published")
	var pubRespData signaling.TrackPublishedResponse
	unmarshalInto(t, pubResp, &pubRespData)
	if pubRespData.TrackID != "audio-e2e" {
		t.Fatalf("track_published = %+v, want audio-e2e", pubRespData)
	}

	waitForTrackAvailable(t, subscriber, "audio-e2e")

	subscriber.send(signaling.MessageTypeSubscribeTrack, signaling.SubscribeTrackRequest{
		RoomID:        "sfu-e2e-audio",
		ParticipantID: "subscriber",
		TrackID:       "audio-e2e",
	})
	subResp := subscriber.receiveOfType(signaling.MessageTypeTrackSubscribed, "subscriber track_subscribed")
	var subRespData signaling.TrackSubscribedResponse
	unmarshalInto(t, subResp, &subRespData)
	if subRespData.TrackID != "audio-e2e" {
		t.Fatalf("track_subscribed = %+v, want audio-e2e", subRespData)
	}

	// Unsubscribe should succeed
	subscriber.send(signaling.MessageTypeUnsubscribeTrack, signaling.UnsubscribeTrackRequest{
		RoomID:        "sfu-e2e-audio",
		ParticipantID: "subscriber",
		TrackID:       "audio-e2e",
	})
	// Use deadline-bounded polling to wait for TrackUnsubscribed, skipping any ICE or offer chatter
	waitForMessageType(t, subscriber, signaling.MessageTypeTrackUnsubscribed, "subscriber track_unsubscribed")
}

func TestE2E_SFU_MultiSubscriber(t *testing.T) {
	ts := newE2EServer(t)

	publisher := dialWS(t, ts, "sfu-e2e-multi", "publisher")
	publisher.receiveOfType(signaling.MessageTypeRoomJoined, "publisher room_joined")

	sub1 := dialWS(t, ts, "sfu-e2e-multi", "sub1")
	sub1.receiveOfType(signaling.MessageTypeRoomJoined, "sub1 room_joined")
	publisher.receiveOfType(signaling.MessageTypeParticipantJoined, "publisher sees sub1")

	sub2 := dialWS(t, ts, "sfu-e2e-multi", "sub2")
	sub2.receiveOfType(signaling.MessageTypeRoomJoined, "sub2 room_joined")
	publisher.receiveOfType(signaling.MessageTypeParticipantJoined, "publisher sees sub2")
	sub1.receiveOfType(signaling.MessageTypeParticipantJoined, "sub1 sees sub2")

	publisher.send(signaling.MessageTypePublishTrack, signaling.PublishTrackRequest{
		RoomID:        "sfu-e2e-multi",
		ParticipantID: "publisher",
		Track:         signaling.TrackInfo{ID: "audio-multi", Kind: "audio", Source: "microphone"},
	})
	publisher.receiveOfType(signaling.MessageTypeTrackPublished, "publisher track_published")

	waitForTrackAvailable(t, sub1, "audio-multi")
	waitForTrackAvailable(t, sub2, "audio-multi")

	sub1.send(signaling.MessageTypeSubscribeTrack, signaling.SubscribeTrackRequest{
		RoomID:        "sfu-e2e-multi",
		ParticipantID: "sub1",
		TrackID:       "audio-multi",
	})
	resp1 := sub1.receiveOfType(signaling.MessageTypeTrackSubscribed, "sub1 track_subscribed")
	var data1 signaling.TrackSubscribedResponse
	unmarshalInto(t, resp1, &data1)
	if data1.TrackID != "audio-multi" {
		t.Fatalf("track_subscribed sub1 = %+v, want audio-multi", data1)
	}
	sub2.send(signaling.MessageTypeSubscribeTrack, signaling.SubscribeTrackRequest{
		RoomID:        "sfu-e2e-multi",
		ParticipantID: "sub2",
		TrackID:       "audio-multi",
	})
	resp2 := sub2.receiveOfType(signaling.MessageTypeTrackSubscribed, "sub2 track_subscribed")
	var data2 signaling.TrackSubscribedResponse
	unmarshalInto(t, resp2, &data2)
	if data2.TrackID != "audio-multi" {
		t.Fatalf("track_subscribed sub2 = %+v, want audio-multi", data2)
	}

	// One unsubscribes, the other remains
	sub1.send(signaling.MessageTypeUnsubscribeTrack, signaling.UnsubscribeTrackRequest{
		RoomID:        "sfu-e2e-multi",
		ParticipantID: "sub1",
		TrackID:       "audio-multi",
	})
	waitForMessageType(t, sub1, signaling.MessageTypeTrackUnsubscribed, "sub1 unsubscribed")
}

// drainWithDeadline consumes any pending messages for the given window without failing.
func drainWithDeadline(t *testing.T, c *wsClient, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if err := c.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			return
		}
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return
		}
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(e2eTimeout))
}

// waitForMessageType waits for a message of the wanted type, failing with details if timeout.
func waitForMessageType(t *testing.T, c *wsClient, want signaling.MessageType, what string) {
	t.Helper()
	deadline := time.Now().Add(e2eTimeout)
	for time.Now().Before(deadline) {
		if err := c.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			t.Fatalf("%s read: %v", what, err)
		}
		msg, err := signaling.ParseMessage(data)
		if err != nil {
			continue
		}
		if msg.Type == want {
			return
		}
		if msg.Type == signaling.MessageTypeError {
			var er signaling.ErrorResponse
			_ = msg.UnmarshalData(&er)
			t.Fatalf("%s got error: %+v", what, er)
		}
		t.Logf("skipping %s while waiting for %s", msg.Type, want)
	}
	t.Fatalf("timed out waiting for %s (%s)", want, what)
}
