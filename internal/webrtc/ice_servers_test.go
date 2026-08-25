package webrtc

import (
	"reflect"
	"testing"

	"github.com/pion/webrtc/v3"
)

func TestICEServersFromURLs(t *testing.T) {
	tests := []struct {
		name        string
		stunURLs    []string
		turnServers []ICETurnServer
		want        []webrtc.ICEServer
	}{
		{
			name: "empty input yields empty list",
			want: []webrtc.ICEServer{},
		},
		{
			name:     "stun urls grouped into a single credential-free server",
			stunURLs: []string{"stun:stun1.slive.local:3478", "stun:stun2.slive.local:3478"},
			want: []webrtc.ICEServer{
				{URLs: []string{"stun:stun1.slive.local:3478", "stun:stun2.slive.local:3478"}},
			},
		},
		{
			name: "turn servers keep their credentials",
			turnServers: []ICETurnServer{
				{
					URLs:       []string{"turn:turn.slive.local:3478", "turns:turn.slive.local:5349"},
					Username:   "slive-user",
					Credential: "slive-secret",
				},
			},
			want: []webrtc.ICEServer{
				{
					URLs:       []string{"turn:turn.slive.local:3478", "turns:turn.slive.local:5349"},
					Username:   "slive-user",
					Credential: "slive-secret",
				},
			},
		},
		{
			name:     "stun and turn combine in order",
			stunURLs: []string{"stun:stun.slive.local:3478"},
			turnServers: []ICETurnServer{
				{URLs: []string{"turn:turn.slive.local:3478"}, Username: "u", Credential: "p"},
			},
			want: []webrtc.ICEServer{
				{URLs: []string{"stun:stun.slive.local:3478"}},
				{URLs: []string{"turn:turn.slive.local:3478"}, Username: "u", Credential: "p"},
			},
		},
		{
			name: "turn entries without urls are skipped",
			turnServers: []ICETurnServer{
				{Username: "orphan"},
			},
			want: []webrtc.ICEServer{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ICEServersFromURLs(tt.stunURLs, tt.turnServers)
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ICEServersFromURLs() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestICEServersFromURLsCopiesInput ensures mutating the input afterwards
// cannot change previously produced configuration.
func TestICEServersFromURLsCopiesInput(t *testing.T) {
	stunURLs := []string{"stun:stun.slive.local:3478"}
	turnURLs := []string{"turn:turn.slive.local:3478"}

	servers := ICEServersFromURLs(stunURLs, []ICETurnServer{{URLs: turnURLs, Username: "u"}})

	stunURLs[0] = "stun:tampered.slive.local:3478"
	turnURLs[0] = "turn:tampered.slive.local:3478"

	if servers[0].URLs[0] != "stun:stun.slive.local:3478" {
		t.Errorf("STUN URL was not copied: %q", servers[0].URLs[0])
	}
	if servers[1].URLs[0] != "turn:turn.slive.local:3478" {
		t.Errorf("TURN URL was not copied: %q", servers[1].URLs[0])
	}
}
