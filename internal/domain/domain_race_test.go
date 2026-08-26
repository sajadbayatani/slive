package domain

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// subscriptionConsistent asserts the two-sided bookkeeping invariant: a
// track ID appears in the participant's subscribed set if and only if the
// track lists the participant as a subscriber.
func subscriptionConsistent(t *testing.T, sub *Participant, track *Track) {
	t.Helper()

	subscribed := false
	for _, id := range sub.SubscribedTracks() {
		if id == track.ID() {
			subscribed = true
		}
	}
	attached := track.GetSubscriber(sub.ID()) == sub

	if subscribed != attached {
		t.Errorf("inconsistent subscription state for %s on %s: participant=%v track=%v",
			sub.ID(), track.ID(), subscribed, attached)
	}
}

// TestRaceManySubscribersOneTrack hammers a single published track from many
// subscribing and reading goroutines; under -race this pins the lock
// discipline of the subscriber map and its snapshot accessors.
func TestRaceManySubscribersOneTrack(t *testing.T) {
	const subscribers = 32

	track := newValidTrack(t, "race-track", TrackKindAudio, TrackSourceMicrophone)
	if err := NewParticipant("publisher", "Alice").PublishTrack(track); err != nil {
		t.Fatalf("PublishTrack: %v", err)
	}

	parts := make([]*Participant, subscribers)
	for i := range parts {
		parts[i] = NewParticipant(fmt.Sprintf("subscriber-%d", i), "Sub")
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	// Writers: everyone subscribes exactly once through the two-sided API.
	for _, sub := range parts {
		wg.Add(1)
		go func(sub *Participant) {
			defer wg.Done()
			<-start
			if err := sub.SubscribeTrack(track); err != nil {
				t.Errorf("SubscribeTrack(%s): %v", sub.ID(), err)
			}
		}(sub)
	}

	// Readers: snapshot accessors run against the mutating map.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 64; j++ {
				_ = track.Subscribers()
				_ = track.SubscriberCount()
				_ = track.HasSubscribers()
				_ = track.GetSubscriber("subscriber-0")
			}
		}()
	}

	close(start)
	wg.Wait()

	if got := track.SubscriberCount(); got != subscribers {
		t.Errorf("SubscriberCount = %d, want %d", got, subscribers)
	}
	live := map[string]bool{}
	for _, id := range track.Subscribers() {
		live[id] = true
	}
	for _, sub := range parts {
		if !live[sub.ID()] {
			t.Errorf("Expected %s among subscribers, got %v", sub.ID(), track.Subscribers())
		}
		subscriptionConsistent(t, sub, track)
	}
}

// TestRaceSelfLeaveVersusSubscribe drives the full outcome set
// {nil, ErrParticipantLeft, ErrTrackNotPublished}: a participant subscribes
// to its own published track while leaving. Every outcome must leave both
// maps consistent and the test must complete without deadlock.
func TestRaceSelfLeaveVersusSubscribe(t *testing.T) {
	const rounds = 64

	for i := 0; i < rounds; i++ {
		participant := NewParticipant(fmt.Sprintf("self-%d", i), "Self")
		track := newValidTrack(t, fmt.Sprintf("self-track-%d", i), TrackKindAudio, TrackSourceMicrophone)
		if err := participant.PublishTrack(track); err != nil {
			t.Fatalf("round %d: PublishTrack: %v", i, err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		var subErr error
		go func() {
			defer wg.Done()
			<-start
			participant.Leave()
		}()
		go func() {
			defer wg.Done()
			<-start
			subErr = participant.SubscribeTrack(track)
		}()

		close(start)
		wg.Wait()

		switch {
		case subErr == nil,
			errors.Is(subErr, ErrParticipantLeft),
			errors.Is(subErr, ErrTrackNotPublished):
			// All legitimate outcomes of racing a leave.
		default:
			t.Fatalf("round %d: unexpected error %v", i, subErr)
		}

		// Leaving always cleans both sides of the bookkeeping.
		if got := len(participant.SubscribedTracks()); got != 0 {
			t.Errorf("round %d: subscribed tracks after leave = %d, want 0", i, got)
		}
		if track.HasSubscribers() {
			t.Errorf("round %d: leftover subscribers %v after leave", i, track.Subscribers())
		}
		if state := track.State(); state != TrackStateUnpublished {
			t.Errorf("round %d: track state after leave = %s, want unpublished", i, state)
		}
		if track.Publisher() != nil {
			t.Errorf("round %d: publisher reference survived leave", i)
		}
		if participant.State() != ParticipantStateLeft {
			t.Errorf("round %d: participant state = %s, want left", i, participant.State())
		}
	}
}

// TestRacePublisherDirectLeaveVersusSubscribe races a publisher's direct
// Participant.Leave (which does not fan out to subscribers) against other
// participants subscribing. Accepted outcomes are {nil, ErrTrackNotPublished};
// the two-sided bookkeeping must stay consistent regardless of who wins.
func TestRacePublisherDirectLeaveVersusSubscribe(t *testing.T) {
	const rounds = 32
	const subscriberCount = 4

	for i := 0; i < rounds; i++ {
		publisher := NewParticipant(fmt.Sprintf("pub-%d", i), "Pub")
		track := newValidTrack(t, fmt.Sprintf("pub-track-%d", i), TrackKindAudio, TrackSourceMicrophone)
		if err := publisher.PublishTrack(track); err != nil {
			t.Fatalf("round %d: PublishTrack: %v", i, err)
		}

		subs := make([]*Participant, subscriberCount)
		errs := make([]error, subscriberCount)
		for s := range subs {
			subs[s] = NewParticipant(fmt.Sprintf("pub-sub-%d-%d", i, s), "Sub")
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1 + subscriberCount)

		go func() {
			defer wg.Done()
			<-start
			publisher.Leave()
		}()
		for s, sub := range subs {
			go func(s int, sub *Participant) {
				defer wg.Done()
				<-start
				errs[s] = sub.SubscribeTrack(track)
			}(s, sub)
		}

		close(start)
		wg.Wait()

		for s, err := range errs {
			if err != nil && !errors.Is(err, ErrTrackNotPublished) {
				t.Fatalf("round %d subscriber %d: unexpected error %v", i, s, err)
			}
			subscriptionConsistent(t, subs[s], track)
		}

		if state := track.State(); state != TrackStateUnpublished {
			t.Errorf("round %d: track state after publisher leave = %s, want unpublished", i, state)
		}
		if track.Publisher() != nil {
			t.Errorf("round %d: publisher reference survived publisher leave", i)
		}
	}
}

// TestRaceRoomCloseVersusSubscribeToTrack races Room.Close against room-level
// subscriptions. Accepted outcomes are {nil, ErrRoomClosed, ErrTrackNotFound};
// afterwards every participant is fully detached.
func TestRaceRoomCloseVersusSubscribeToTrack(t *testing.T) {
	const rounds = 24
	const subscriberCount = 3

	for i := 0; i < rounds; i++ {
		room := NewRoom(fmt.Sprintf("close-race-%d", i))
		_ = room.Create()

		publisher := NewParticipant(fmt.Sprintf("closer-pub-%d", i), "Pub")
		if err := room.Join(publisher); err != nil {
			t.Fatalf("round %d: Join: %v", i, err)
		}
		track := newValidTrack(t, fmt.Sprintf("close-track-%d", i), TrackKindVideo, TrackSourceCamera)
		if err := publisher.PublishTrack(track); err != nil {
			t.Fatalf("round %d: PublishTrack: %v", i, err)
		}
		if err := room.PublishTrack(track); err != nil {
			t.Fatalf("round %d: Room.PublishTrack: %v", i, err)
		}

		subs := make([]*Participant, subscriberCount)
		errs := make([]error, subscriberCount)
		for s := range subs {
			subs[s] = NewParticipant(fmt.Sprintf("closer-sub-%d-%d", i, s), "Sub")
			if err := room.Join(subs[s]); err != nil {
				t.Fatalf("round %d: Join(sub): %v", i, err)
			}
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1 + subscriberCount)

		go func() {
			defer wg.Done()
			<-start
			if err := room.Close(); err != nil {
				t.Errorf("round %d: Close: %v", i, err)
			}
		}()
		for s, sub := range subs {
			go func(s int, sub *Participant) {
				defer wg.Done()
				<-start
				errs[s] = room.SubscribeToTrack(sub, track.ID())
			}(s, sub)
		}

		close(start)
		wg.Wait()

		for s, err := range errs {
			switch {
			case err == nil,
				errors.Is(err, ErrRoomClosed),
				errors.Is(err, ErrTrackNotFound):
			default:
				t.Fatalf("round %d subscriber %d: unexpected error %v", i, s, err)
			}
		}

		// Close detaches everyone and wipes both registries.
		for _, sub := range subs {
			if got := len(sub.SubscribedTracks()); got != 0 {
				t.Errorf("round %d: %s kept %d subscriptions after close", i, sub.ID(), got)
			}
			if sub.Room() != nil {
				t.Errorf("round %d: %s kept room association after close", i, sub.ID())
			}
		}
		if track.HasSubscribers() {
			t.Errorf("round %d: subscribers survived close: %v", i, track.Subscribers())
		}
		if got := len(room.Tracks()) + len(room.Participants()); got != 0 {
			t.Errorf("round %d: registries not wiped (tracks+participants=%d)", i, got)
		}
		// Idempotent close.
		if err := room.Close(); err != nil {
			t.Errorf("round %d: second Close: %v", i, err)
		}
	}
}

// TestRacePublisherRoomLeaveVersusSubscribeToTrack races Room.Leave of the
// publisher (which destroys the track) against room-level subscriptions.
// Accepted outcomes are {nil, ErrTrackNotFound}.
func TestRacePublisherRoomLeaveVersusSubscribeToTrack(t *testing.T) {
	const rounds = 24

	for i := 0; i < rounds; i++ {
		room := NewRoom(fmt.Sprintf("leave-race-%d", i))
		_ = room.Create()

		publisher := NewParticipant(fmt.Sprintf("leave-pub-%d", i), "Pub")
		_ = room.Join(publisher)
		track := newValidTrack(t, fmt.Sprintf("leave-track-%d", i), TrackKindAudio, TrackSourceMicrophone)
		_ = publisher.PublishTrack(track)
		_ = room.PublishTrack(track)

		sub := NewParticipant(fmt.Sprintf("leave-sub-%d", i), "Sub")
		_ = room.Join(sub)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		var leaveErr, subErr error
		go func() {
			defer wg.Done()
			<-start
			leaveErr = room.Leave(publisher.ID())
		}()
		go func() {
			defer wg.Done()
			<-start
			subErr = room.SubscribeToTrack(sub, track.ID())
		}()

		close(start)
		wg.Wait()

		if leaveErr != nil {
			t.Fatalf("round %d: Leave(publisher) = %v, want nil", i, leaveErr)
		}
		switch {
		case subErr == nil, errors.Is(subErr, ErrTrackNotFound):
		default:
			t.Fatalf("round %d: unexpected subscribe error %v", i, subErr)
		}

		// The destroyed track is gone everywhere.
		if got := room.GetTrack(track.ID()); got != nil {
			t.Errorf("round %d: destroyed track still registered", i)
		}
		subscriptionConsistent(t, sub, track)
		if track.Publisher() != nil || track.State() != TrackStateUnpublished {
			t.Errorf("round %d: track ownership/state not cleaned: pub=%v state=%s",
				i, track.Publisher(), track.State())
		}

		// Rejoin after leave works and the room stays usable.
		rejoin := NewParticipant(publisher.ID(), "Pub Again")
		if err := room.Join(rejoin); err != nil {
			t.Errorf("round %d: rejoin after leave: %v", i, err)
		}
	}
}

// TestRacePublishUnpublishWhileSubscribed flips publication state while a
// subscriber toggles its subscription. Only nil and the documented sentinels
// may surface, and both sides must agree at the end.
func TestRacePublishUnpublishWhileSubscribed(t *testing.T) {
	const rounds = 48

	for i := 0; i < rounds; i++ {
		publisher := NewParticipant(fmt.Sprintf("toggle-pub-%d", i), "Pub")
		sub := NewParticipant(fmt.Sprintf("toggle-sub-%d", i), "Sub")
		track := newValidTrack(t, fmt.Sprintf("toggle-track-%d", i), TrackKindAudio, TrackSourceMicrophone)
		_ = publisher.PublishTrack(track)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			<-start
			// Flip raw publication state without touching participant books.
			for j := 0; j < 16; j++ {
				track.Unpublish()
				track.Publish()
			}
		}()
		var subErr error
		go func() {
			defer wg.Done()
			<-start
			// Toggle subscription; each iteration is a full round trip.
			for j := 0; j < 16; j++ {
				err := sub.SubscribeTrack(track)
				switch {
				case err == nil:
					if uerr := sub.UnsubscribeTrack(track.ID()); uerr != nil &&
						!errors.Is(uerr, ErrTrackNotFound) {
						t.Errorf("round %d iter %d: UnsubscribeTrack: %v", i, j, uerr)
					}
				case errors.Is(err, ErrTrackNotPublished),
					errors.Is(err, ErrParticipantLeft):
					subErr = err
				default:
					t.Errorf("round %d iter %d: SubscribeTrack: %v", i, j, err)
				}
			}
		}()

		close(start)
		wg.Wait()

		_ = subErr // recorded but every legal value already passed the switch
		subscriptionConsistent(t, sub, track)
	}
}

// TestRaceDoubleLeaveAndRejoin exercises duplicate Room.Leave and re-Join of
// the same participant ID from concurrent goroutines. Only documented errors
// may surface and the room roster must stay internally consistent.
func TestRaceDoubleLeaveAndRejoin(t *testing.T) {
	const rounds = 48

	for i := 0; i < rounds; i++ {
		room := NewRoom(fmt.Sprintf("rejoin-room-%d", i))
		_ = room.Create()

		id := fmt.Sprintf("rejoiner-%d", i)
		first := NewParticipant(id, "First")
		if err := room.Join(first); err != nil {
			t.Fatalf("round %d: initial Join: %v", i, err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(3)

		leaveErrs := make([]error, 2)
		for l := range leaveErrs {
			go func(l int) {
				defer wg.Done()
				<-start
				leaveErrs[l] = room.Leave(id)
			}(l)
		}
		var joinErr error
		go func() {
			defer wg.Done()
			<-start
			joinErr = room.Join(NewParticipant(id, "Second"))
		}()

		close(start)
		wg.Wait()

		for l, err := range leaveErrs {
			switch {
			case err == nil,
				errors.Is(err, ErrParticipantNotFound),
				errors.Is(err, ErrRoomClosed):
			default:
				t.Fatalf("round %d leave %d: unexpected error %v", i, l, err)
			}
		}
		switch {
		case joinErr == nil,
			errors.Is(joinErr, ErrParticipantAlreadyExists):
		default:
			t.Fatalf("round %d: unexpected join error %v", i, joinErr)
		}

		// Roster consistency: the map and the lookup agree about the ID.
		inRoster := false
		for _, pid := range room.Participants() {
			if pid == id {
				inRoster = true
			}
		}
		if inRoster != (room.GetParticipant(id) != nil) {
			t.Errorf("round %d: roster inconsistency for %s", i, id)
		}
	}
}
