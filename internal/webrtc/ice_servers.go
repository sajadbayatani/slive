package webrtc

import "github.com/pion/webrtc/v3"

// ICETurnServer describes a TURN relay server used during ICE gathering.
type ICETurnServer struct {
	// URLs of the TURN server, e.g. "turn:turn.example.com:3478".
	URLs []string
	// Username used to authenticate against the TURN server.
	Username string
	// Credential used to authenticate against the TURN server.
	Credential string
}

// ICEServersFromURLs builds the pion ICE server list from STUN URLs and TURN
// servers. It lets callers translate application configuration into
// PeerConnectionConfig without this package depending on internal/config.
//
// All STUN URLs are grouped into a single credential-free ICEServer entry;
// every TURN server becomes its own entry carrying its credentials. Empty
// inputs are skipped, so callers may pass optional configuration verbatim.
func ICEServersFromURLs(stunURLs []string, turnServers []ICETurnServer) []webrtc.ICEServer {
	servers := make([]webrtc.ICEServer, 0, len(stunURLs)+len(turnServers))

	if len(stunURLs) > 0 {
		servers = append(servers, webrtc.ICEServer{
			URLs: append([]string(nil), stunURLs...),
		})
	}

	for _, turn := range turnServers {
		if len(turn.URLs) == 0 {
			continue
		}
		servers = append(servers, webrtc.ICEServer{
			URLs:       append([]string(nil), turn.URLs...),
			Username:   turn.Username,
			Credential: turn.Credential,
		})
	}

	return servers
}
