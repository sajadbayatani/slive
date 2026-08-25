package signaling

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

// Handler handles WebSocket signaling connections.
type Handler struct {
	roomManager          *RoomManager
	connectionManager    *ConnectionManager
	peerConnections      map[string]*webrtc.PeerConnection
	peerConnectionsMutex sync.RWMutex
}

// NewHandler creates a new Handler.
func NewHandler(roomManager *RoomManager) *Handler {
	return &Handler{
		roomManager:       roomManager,
		connectionManager: NewConnectionManager(),
		peerConnections:   make(map[string]*webrtc.PeerConnection),
	}
}

// ServeHTTP implements http.Handler for WebSocket connections.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Extract room ID and participant ID from the request
	// TODO: Implement proper path parsing and authentication
	roomID := r.URL.Query().Get("room_id")
	participantID := r.URL.Query().Get("participant_id")

	if roomID == "" || participantID == "" {
		http.Error(w, "room_id and participant_id are required", http.StatusBadRequest)
		return
	}

	// Create a new WebSocket connection
	conn, err := NewConnection(w, r, roomID, participantID)
	if err != nil {
		log.Printf("Failed to upgrade WebSocket connection: %v", err)
		return
	}

	// Register the connection
	h.connectionManager.Add(conn)
	defer h.connectionManager.Remove(conn.ID())

	// Handle the connection in a goroutine
	go h.handleConnection(conn)
}

// handleConnection handles messages from a single connection.
func (h *Handler) handleConnection(conn *Connection) {
	defer conn.Close()

	// Get or create the room
	room, err := h.roomManager.GetOrCreateRoom(conn.RoomID())
	if err != nil {
		log.Printf("Failed to get or create room: %v", err)
		_ = conn.Send(MessageTypeError, ErrorResponse{
			Error:       "Failed to get or create room",
			Code:        ErrorCodeInternalError,
			RequestType: string(MessageTypeJoinRoom),
		})
		return
	}

	// Get or create the participant
	participant := room.GetParticipant(conn.ID())
	if participant == nil {
		// Create a new participant
		participant = domain.NewParticipant(conn.ID(), "Participant "+conn.ID())
		if err := room.Join(participant); err != nil {
			log.Printf("Failed to join room: %v", err)
			_ = conn.Send(MessageTypeError, ErrorResponse{
				Error:       "Failed to join room",
				Code:        errorCodeFromDomainError(err),
				RequestType: string(MessageTypeJoinRoom),
			})
			return
		}
		participant.SetRoom(room)

		// Initialize a peer connection for the participant
		pcConfig := webrtc.DefaultPeerConnectionConfig()
		pc, err := webrtc.NewPeerConnection(pcConfig, participant, func(msgType string, data interface{}) error {
			return conn.Send(MessageType(msgType), data)
		})
		if err != nil {
			log.Printf("Failed to create peer connection: %v", err)
			_ = conn.Send(MessageTypeError, ErrorResponse{
				Error:       "Failed to create peer connection",
				Code:        ErrorCodeInternalError,
				RequestType: string(MessageTypeJoinRoom),
			})
			return
		}

		h.peerConnectionsMutex.Lock()
		h.peerConnections[participant.ID()] = pc
		h.peerConnectionsMutex.Unlock()

		// Notify other participants that a new participant has joined
		h.broadcastParticipantJoined(room, participant)
	} else {
		// Reconnect existing participant
		participant.SetRoom(room)

		// Reuse the existing peer connection or create a new one if needed
		h.peerConnectionsMutex.RLock()
		pc := h.peerConnections[participant.ID()]
		h.peerConnectionsMutex.RUnlock()

		if pc != nil {
			// Update the signaling sender to use the new connection
			pc.UpdateSignalingSender(func(msgType string, data interface{}) error {
				return conn.Send(MessageType(msgType), data)
			})
			if pc.NeedsReconnect() {
				log.Printf("signaling: reconnecting participant=%s state=%s",
					participant.ID(), pc.State())
			}
		} else {
			pcConfig := webrtc.DefaultPeerConnectionConfig()
			newPC, err := webrtc.NewPeerConnection(pcConfig, participant, func(msgType string, data interface{}) error {
				return conn.Send(MessageType(msgType), data)
			})
			if err != nil {
				log.Printf("Failed to recreate peer connection on reconnect: %v", err)
			} else {
				h.peerConnectionsMutex.Lock()
				h.peerConnections[participant.ID()] = newPC
				h.peerConnectionsMutex.Unlock()
				log.Printf("signaling: recreated peer connection for participant=%s", participant.ID())
			}
		}
	}

	// Send room joined response
	if err := h.sendRoomJoined(conn, room, participant); err != nil {
		log.Printf("Failed to send room joined response: %v", err)
		return
	}

	// Main message loop
	for {
		msg, err := conn.Receive()
		if err != nil {
			if err == ErrConnectionClosed {
				log.Printf("Connection closed for participant %s", conn.ID())
			} else {
				log.Printf("Failed to receive message: %v", err)
			}
			break
		}

		if err := h.handleMessage(conn, room, participant, msg); err != nil {
			log.Printf("Failed to handle message: %v", err)
			_ = conn.Send(MessageTypeError, ErrorResponse{
				Error:       err.Error(),
				Code:        ErrorCodeInternalError,
				RequestType: string(msg.Type),
			})
		}
	}

	// Clean up when connection closes
	h.handleConnectionClosed(room, participant)
}

// handleMessage handles a single message from a connection.
func (h *Handler) handleMessage(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	switch msg.Type {
	case MessageTypeCreateRoom:
		return h.handleCreateRoom(conn, msg)

	case MessageTypeJoinRoom:
		return h.handleJoinRoom(conn, room, participant, msg)

	case MessageTypeLeaveRoom:
		return h.handleLeaveRoom(conn, room, participant, msg)

	case MessageTypePublishTrack:
		return h.handlePublishTrack(conn, room, participant, msg)

	case MessageTypeUnpublishTrack:
		return h.handleUnpublishTrack(conn, room, participant, msg)

	case MessageTypeSubscribeTrack:
		return h.handleSubscribeTrack(conn, room, participant, msg)

	case MessageTypeOffer:
		return h.handleOffer(conn, room, participant, msg)

	case MessageTypeAnswer:
		return h.handleAnswer(conn, room, participant, msg)

	case MessageTypeICECandidate:
		return h.handleICECandidate(conn, room, participant, msg)

	default:
		log.Printf("Unknown message type: %s", msg.Type)
		return nil
	}
}

// handleCreateRoom handles a create room request.
func (h *Handler) handleCreateRoom(conn *Connection, msg *Message) error {
	var req CreateRoomRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Create the room
	room, err := h.roomManager.CreateRoom(req.RoomID)
	if err != nil {
		return err
	}

	// Create the participant
	participant := domain.NewParticipant(req.ParticipantID, req.ParticipantName)
	if err := room.Join(participant); err != nil {
		return err
	}
	participant.SetRoom(room)

	// Send response
	resp := RoomCreatedResponse{
		RoomID:        req.RoomID,
		ParticipantID: req.ParticipantID,
		Status:        "success",
	}

	return conn.Send(MessageTypeRoomCreated, resp)
}

// handleJoinRoom handles a join room request.
func (h *Handler) handleJoinRoom(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req JoinRoomRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Update participant name if provided
	if req.ParticipantName != "" {
		// TODO: Add SetName method to Participant
	}

	// Send room joined response
	return h.sendRoomJoined(conn, room, participant)
}

// handleLeaveRoom handles a leave room request.
func (h *Handler) handleLeaveRoom(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req LeaveRoomRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Remove participant from room
	if err := room.Leave(req.ParticipantID); err != nil {
		return err
	}

	// Send response
	resp := RoomLeftResponse{
		RoomID:        req.RoomID,
		ParticipantID: req.ParticipantID,
		Status:        "success",
	}

	if err := conn.Send(MessageTypeRoomLeft, resp); err != nil {
		return err
	}

	// Notify other participants
	h.broadcastParticipantLeft(room, participant)

	return nil
}

// handlePublishTrack handles a publish track request.
func (h *Handler) handlePublishTrack(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req PublishTrackRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Convert track info to domain.Track
	kind := domain.TrackKindAudio
	if req.Track.Kind == "video" {
		kind = domain.TrackKindVideo
	}

	source := domain.TrackSourceMicrophone
	switch req.Track.Source {
	case "camera":
		source = domain.TrackSourceCamera
	case "screen_share":
		source = domain.TrackSourceScreenShare
	}

	track, err := domain.NewTrack(req.Track.ID, kind, source)
	if err != nil {
		return err
	}
	if err := participant.PublishTrack(track); err != nil {
		return err
	}

	// Send response
	resp := TrackPublishedResponse{
		TrackID:       req.Track.ID,
		ParticipantID: req.ParticipantID,
		Status:        "success",
	}

	if err := conn.Send(MessageTypeTrackPublished, resp); err != nil {
		return err
	}

	// Notify other participants
	h.broadcastTrackAvailable(room, participant, track)

	return nil
}

// handleUnpublishTrack handles an unpublish track request.
func (h *Handler) handleUnpublishTrack(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req UnpublishTrackRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Unpublish the track
	if err := participant.UnpublishTrack(req.TrackID); err != nil {
		return err
	}

	// Send response
	resp := TrackUnpublishedResponse{
		TrackID:       req.TrackID,
		ParticipantID: req.ParticipantID,
		Status:        "success",
	}

	if err := conn.Send(MessageTypeTrackUnpublished, resp); err != nil {
		return err
	}

	// Notify other participants
	h.broadcastTrackUnavailable(room, participant, req.TrackID)

	return nil
}

// handleSubscribeTrack handles a subscribe track request.
func (h *Handler) handleSubscribeTrack(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req SubscribeTrackRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Find the track from the publisher
	publisher := room.GetParticipant(req.ParticipantID)
	if publisher == nil {
		return domain.ErrParticipantNotFound
	}

	track := publisher.GetPublishedTrack(req.TrackID)
	if track == nil {
		return domain.ErrTrackNotFound
	}

	// Subscribe to the track
	if err := participant.SubscribeTrack(track); err != nil {
		return err
	}

	// Send response
	resp := TrackSubscribedResponse{
		TrackID:     req.TrackID,
		PublisherID: req.ParticipantID,
		Status:      "success",
	}

	return conn.Send(MessageTypeTrackSubscribed, resp)
}

// handleOffer handles a WebRTC offer.
func (h *Handler) handleOffer(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req OfferRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Validate the offer request
	if err := ValidateOfferRequest(&req); err != nil {
		return err
	}

	// Find the target participant
	target := room.GetParticipant(req.TargetParticipantID)
	if target == nil {
		return domain.ErrParticipantNotFound
	}

	// Get the target peer connection
	h.peerConnectionsMutex.RLock()
	pc := h.peerConnections[req.TargetParticipantID]
	h.peerConnectionsMutex.RUnlock()

	if pc == nil {
		return ErrConnectionNotFound
	}

	offer, err := webrtc.NewSessionDescriptionFromString(req.SDP, pionwebrtc.SDPTypeOffer)
	if err != nil {
		return err
	}

	// CreateAnswer installs the remote offer before generating the answer.
	answer, err := pc.CreateAnswer(offer)
	if err != nil {
		return err
	}

	// Send the answer back to the source participant
	answerNotification := AnswerNotification{
		SourceParticipantID: req.TargetParticipantID,
		SDP:                 answer.SDP(),
	}

	return conn.Send(MessageTypeAnswer, answerNotification)
}

// handleAnswer handles a WebRTC answer.
func (h *Handler) handleAnswer(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req AnswerRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Validate the answer request
	if err := ValidateAnswerRequest(&req); err != nil {
		return err
	}

	// The answer is applied to the peer connection owned by its target.
	h.peerConnectionsMutex.RLock()
	pc := h.peerConnections[req.TargetParticipantID]
	h.peerConnectionsMutex.RUnlock()

	if pc == nil {
		return ErrConnectionNotFound
	}

	answer, err := webrtc.NewSessionDescriptionFromString(req.SDP, pionwebrtc.SDPTypeAnswer)
	if err != nil {
		return err
	}

	// Set the remote description on the source peer connection
	return pc.SetRemoteDescription(answer)
}

// handleICECandidate handles an ICE candidate.
func (h *Handler) handleICECandidate(conn *Connection, room *domain.Room, participant *domain.Participant, msg *Message) error {
	var req ICECandidateRequest
	if err := msg.UnmarshalData(&req); err != nil {
		return err
	}

	// Validate the ICE candidate request
	if err := ValidateICECandidateRequest(&req); err != nil {
		return err
	}

	// Get the target peer connection
	h.peerConnectionsMutex.RLock()
	pc := h.peerConnections[req.TargetParticipantID]
	h.peerConnectionsMutex.RUnlock()

	if pc == nil {
		return ErrConnectionNotFound
	}

	candidate, err := webrtc.NewICECandidateFromString(req.Candidate)
	if err != nil {
		return err
	}
	candidate.SetSDPMid(req.SDPMid)
	candidate.SetSDPMLineIndex(uint16(req.SDPMLineIndex))

	// Add the ICE candidate to the target peer connection with retry for transient failures
	return pc.AddICECandidateWithRetry(candidate)
}

// handleConnectionClosed handles cleanup when a connection closes.
func (h *Handler) handleConnectionClosed(room *domain.Room, participant *domain.Participant) {
	// Remove participant from room
	if err := room.Leave(participant.ID()); err != nil {
		log.Printf("Failed to remove participant from room: %v", err)
	}

	// Close and remove the peer connection
	h.peerConnectionsMutex.Lock()
	if pc, exists := h.peerConnections[participant.ID()]; exists {
		if err := pc.Close(); err != nil {
			log.Printf("Failed to close peer connection: %v", err)
		}
		delete(h.peerConnections, participant.ID())
	}
	h.peerConnectionsMutex.Unlock()

	// Notify other participants
	h.broadcastParticipantLeft(room, participant)
}

// sendRoomJoined sends a room joined response.
func (h *Handler) sendRoomJoined(conn *Connection, room *domain.Room, participant *domain.Participant) error {
	// Collect participant info
	participants := make([]ParticipantInfo, 0)
	for _, pID := range room.Participants() {
		p := room.GetParticipant(pID)
		if p != nil {
			participants = append(participants, ParticipantInfo{
				ID:   p.ID(),
				Name: p.Name(),
			})
		}
	}

	resp := RoomJoinedResponse{
		RoomID:        room.ID(),
		ParticipantID: participant.ID(),
		Participants:  participants,
		Status:        "success",
	}

	return conn.Send(MessageTypeRoomJoined, resp)
}

// broadcastParticipantJoined broadcasts a participant joined notification to all other participants in the room.
func (h *Handler) broadcastParticipantJoined(room *domain.Room, participant *domain.Participant) {
	notification := ParticipantJoinedNotification{
		Participant: ParticipantInfo{
			ID:   participant.ID(),
			Name: participant.Name(),
		},
	}

	h.broadcastToRoom(room, MessageTypeParticipantJoined, notification, participant.ID())
}

// broadcastParticipantLeft broadcasts a participant left notification to all other participants in the room.
func (h *Handler) broadcastParticipantLeft(room *domain.Room, participant *domain.Participant) {
	notification := ParticipantLeftNotification{
		ParticipantID: participant.ID(),
	}

	h.broadcastToRoom(room, MessageTypeParticipantLeft, notification, participant.ID())
}

// broadcastTrackAvailable broadcasts a track available notification to all other participants in the room.
func (h *Handler) broadcastTrackAvailable(room *domain.Room, participant *domain.Participant, track *domain.Track) {
	notification := TrackAvailableNotification{
		ParticipantID: participant.ID(),
		Track: TrackInfo{
			ID:     track.ID(),
			Kind:   track.Kind().String(),
			Source: track.Source().String(),
		},
	}

	h.broadcastToRoom(room, MessageTypeTrackAvailable, notification, participant.ID())
}

// broadcastTrackUnavailable broadcasts a track unavailable notification to all other participants in the room.
func (h *Handler) broadcastTrackUnavailable(room *domain.Room, participant *domain.Participant, trackID string) {
	notification := TrackUnavailableNotification{
		ParticipantID: participant.ID(),
		TrackID:       trackID,
	}

	h.broadcastToRoom(room, MessageTypeTrackUnavailable, notification, participant.ID())
}

// broadcastToRoom sends a message to all participants in a room except the excluded participant.
func (h *Handler) broadcastToRoom(room *domain.Room, msgType MessageType, data interface{}, excludeParticipantID string) {
	for _, pID := range room.Participants() {
		if pID == excludeParticipantID {
			continue
		}

		conn := h.connectionManager.Get(pID)
		if conn != nil {
			_ = conn.Send(msgType, data)
		}
	}
}

// Connection-related errors.
var (
	ErrConnectionNotFound = fmt.Errorf("connection not found")
)
