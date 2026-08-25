package signaling

import (
	"encoding/json"
	"fmt"

	"github.com/sajadbayatani/slive/internal/domain"
)

// MessageType represents the type of signaling message.
type MessageType string

const (
	// Room messages
	MessageTypeCreateRoom  MessageType = "create_room"
	MessageTypeRoomCreated MessageType = "room_created"
	MessageTypeJoinRoom    MessageType = "join_room"
	MessageTypeRoomJoined  MessageType = "room_joined"
	MessageTypeLeaveRoom   MessageType = "leave_room"
	MessageTypeRoomLeft    MessageType = "room_left"

	// Participant messages
	MessageTypeParticipantJoined MessageType = "participant_joined"
	MessageTypeParticipantLeft   MessageType = "participant_left"

	// Track messages
	MessageTypePublishTrack     MessageType = "publish_track"
	MessageTypeTrackPublished   MessageType = "track_published"
	MessageTypeUnpublishTrack   MessageType = "unpublish_track"
	MessageTypeTrackUnpublished MessageType = "track_unpublished"
	MessageTypeSubscribeTrack   MessageType = "subscribe_track"
	MessageTypeTrackSubscribed  MessageType = "track_subscribed"
	MessageTypeTrackAvailable   MessageType = "track_available"
	MessageTypeTrackUnavailable MessageType = "track_unavailable"

	// WebRTC signaling messages
	MessageTypeOffer        MessageType = "webrtc:offer"
	MessageTypeAnswer       MessageType = "webrtc:answer"
	MessageTypeICECandidate MessageType = "webrtc:ice-candidate"

	// Error messages
	MessageTypeError MessageType = "error"
)

// Message represents a signaling message.
type Message struct {
	Type MessageType     `json:"type"`
	Data json.RawMessage `json:"data"`
}

// NewMessage creates a new Message with the given type and data.
func NewMessage(msgType MessageType, data interface{}) (*Message, error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message data: %w", err)
	}

	return &Message{
		Type: msgType,
		Data: dataBytes,
	}, nil
}

// ParseMessage parses a JSON byte slice into a Message.
func ParseMessage(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return &msg, nil
}

// UnmarshalData unmarshals the message data into the provided interface.
func (m *Message) UnmarshalData(v interface{}) error {
	return json.Unmarshal(m.Data, v)
}

// Marshal serializes the message to JSON.
func (m *Message) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// --- Request/Response Types ---

// CreateRoomRequest represents a request to create a new room.
type CreateRoomRequest struct {
	RoomID          string `json:"room_id"`
	ParticipantID   string `json:"participant_id"`
	ParticipantName string `json:"participant_name"`
}

// RoomCreatedResponse represents a response to a room creation request.
type RoomCreatedResponse struct {
	RoomID        string `json:"room_id"`
	ParticipantID string `json:"participant_id"`
	Status        string `json:"status"`
}

// JoinRoomRequest represents a request to join an existing room.
type JoinRoomRequest struct {
	RoomID          string `json:"room_id"`
	ParticipantID   string `json:"participant_id"`
	ParticipantName string `json:"participant_name"`
}

// ParticipantInfo represents information about a participant.
type ParticipantInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// RoomJoinedResponse represents a response to a join room request.
type RoomJoinedResponse struct {
	RoomID        string            `json:"room_id"`
	ParticipantID string            `json:"participant_id"`
	Participants  []ParticipantInfo `json:"participants"`
	Status        string            `json:"status"`
}

// ParticipantJoinedNotification represents a notification that a participant has joined.
type ParticipantJoinedNotification struct {
	Participant ParticipantInfo `json:"participant"`
}

// LeaveRoomRequest represents a request to leave a room.
type LeaveRoomRequest struct {
	RoomID        string `json:"room_id"`
	ParticipantID string `json:"participant_id"`
}

// RoomLeftResponse represents a response to a leave room request.
type RoomLeftResponse struct {
	RoomID        string `json:"room_id"`
	ParticipantID string `json:"participant_id"`
	Status        string `json:"status"`
}

// ParticipantLeftNotification represents a notification that a participant has left.
type ParticipantLeftNotification struct {
	ParticipantID string `json:"participant_id"`
}

// TrackInfo represents information about a track.
type TrackInfo struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`   // "audio" or "video"
	Source string `json:"source"` // "microphone", "camera", "screen_share"
}

// PublishTrackRequest represents a request to publish a track.
type PublishTrackRequest struct {
	RoomID        string    `json:"room_id"`
	ParticipantID string    `json:"participant_id"`
	Track         TrackInfo `json:"track"`
}

// TrackPublishedResponse represents a response to a publish track request.
type TrackPublishedResponse struct {
	TrackID       string `json:"track_id"`
	ParticipantID string `json:"participant_id"`
	Status        string `json:"status"`
}

// TrackAvailableNotification represents a notification that a track is available.
type TrackAvailableNotification struct {
	ParticipantID string    `json:"participant_id"`
	Track         TrackInfo `json:"track"`
}

// UnpublishTrackRequest represents a request to unpublish a track.
type UnpublishTrackRequest struct {
	RoomID        string `json:"room_id"`
	ParticipantID string `json:"participant_id"`
	TrackID       string `json:"track_id"`
}

// TrackUnpublishedResponse represents a response to an unpublish track request.
type TrackUnpublishedResponse struct {
	TrackID       string `json:"track_id"`
	ParticipantID string `json:"participant_id"`
	Status        string `json:"status"`
}

// TrackUnavailableNotification represents a notification that a track is unavailable.
type TrackUnavailableNotification struct {
	ParticipantID string `json:"participant_id"`
	TrackID       string `json:"track_id"`
}

// SubscribeTrackRequest represents a request to subscribe to a track.
type SubscribeTrackRequest struct {
	RoomID        string `json:"room_id"`
	ParticipantID string `json:"participant_id"`
	TrackID       string `json:"track_id"`
}

// TrackSubscribedResponse represents a response to a subscribe track request.
type TrackSubscribedResponse struct {
	TrackID     string `json:"track_id"`
	PublisherID string `json:"publisher_id"`
	Status      string `json:"status"`
}

// OfferRequest represents a WebRTC offer.
type OfferRequest struct {
	RoomID              string   `json:"room_id"`
	ParticipantID       string   `json:"participant_id"`
	TargetParticipantID string   `json:"target_participant_id"`
	SDP                 string   `json:"sdp"`
	TrackIDs            []string `json:"track_ids"`
}

// OfferNotification represents an offer to be relayed to another participant.
type OfferNotification struct {
	SourceParticipantID string   `json:"source_participant_id"`
	SDP                 string   `json:"sdp"`
	TrackIDs            []string `json:"track_ids"`
}

// AnswerRequest represents a WebRTC answer.
type AnswerRequest struct {
	RoomID              string `json:"room_id"`
	ParticipantID       string `json:"participant_id"`
	TargetParticipantID string `json:"target_participant_id"`
	SDP                 string `json:"sdp"`
}

// AnswerNotification represents an answer to be relayed to another participant.
type AnswerNotification struct {
	SourceParticipantID string `json:"source_participant_id"`
	SDP                 string `json:"sdp"`
}

// ICECandidateRequest represents an ICE candidate.
type ICECandidateRequest struct {
	RoomID              string `json:"room_id"`
	ParticipantID       string `json:"participant_id"`
	TargetParticipantID string `json:"target_participant_id"`
	Candidate           string `json:"candidate"`
	SDPMid              string `json:"sdp_mid"`
	SDPMLineIndex       int    `json:"sdp_mline_index"`
}

// ICECandidateNotification represents an ICE candidate to be relayed to another participant.
type ICECandidateNotification struct {
	SourceParticipantID string `json:"source_participant_id"`
	Candidate           string `json:"candidate"`
	SDPMid              string `json:"sdp_mid"`
	SDPMLineIndex       int    `json:"sdp_mline_index"`
}

// ErrorResponse represents an error response.
type ErrorResponse struct {
	Error       string `json:"error"`
	Code        string `json:"code"`
	RequestType string `json:"request_type"`
}

// ErrorCodes for signaling errors.
const (
	ErrorCodeRoomNotFound         = "room_not_found"
	ErrorCodeRoomClosed           = "room_closed"
	ErrorCodeParticipantNotFound  = "participant_not_found"
	ErrorCodeTrackNotFound        = "track_not_found"
	ErrorCodeInvalidRequest       = "invalid_request"
	ErrorCodeInternalError        = "internal_error"
	ErrorCodeInvalidWebRTCMessage = "invalid_webrtc_message"
)

// WebRTC message validation constants.
const (
	MaxSDPLength       = 16384 // Maximum SDP length in bytes
	MaxCandidateLength = 1024  // Maximum ICE candidate length in bytes
)

// Convert domain errors to signaling error codes.
func errorCodeFromDomainError(err error) string {
	if err == nil {
		return ""
	}

	switch {
	case err == domain.ErrRoomClosed:
		return ErrorCodeRoomClosed
	case err == domain.ErrParticipantNotFound:
		return ErrorCodeParticipantNotFound
	case err == domain.ErrTrackNotFound:
		return ErrorCodeTrackNotFound
	default:
		return ErrorCodeInternalError
	}
}

// ValidateOfferRequest validates an offer request.
func ValidateOfferRequest(req *OfferRequest) error {
	if req == nil {
		return fmt.Errorf("offer request cannot be nil")
	}

	if req.ParticipantID == "" {
		return fmt.Errorf("participant_id is required")
	}

	if req.TargetParticipantID == "" {
		return fmt.Errorf("target_participant_id is required")
	}

	if req.SDP == "" {
		return fmt.Errorf("sdp is required")
	}

	if len(req.SDP) > MaxSDPLength {
		return fmt.Errorf("sdp exceeds maximum length of %d bytes", MaxSDPLength)
	}

	return nil
}

// ValidateAnswerRequest validates an answer request.
func ValidateAnswerRequest(req *AnswerRequest) error {
	if req == nil {
		return fmt.Errorf("answer request cannot be nil")
	}

	if req.ParticipantID == "" {
		return fmt.Errorf("participant_id is required")
	}

	if req.TargetParticipantID == "" {
		return fmt.Errorf("target_participant_id is required")
	}

	if req.SDP == "" {
		return fmt.Errorf("sdp is required")
	}

	if len(req.SDP) > MaxSDPLength {
		return fmt.Errorf("sdp exceeds maximum length of %d bytes", MaxSDPLength)
	}

	return nil
}

// ValidateICECandidateRequest validates an ICE candidate request.
func ValidateICECandidateRequest(req *ICECandidateRequest) error {
	if req == nil {
		return fmt.Errorf("ice candidate request cannot be nil")
	}

	if req.ParticipantID == "" {
		return fmt.Errorf("participant_id is required")
	}

	if req.TargetParticipantID == "" {
		return fmt.Errorf("target_participant_id is required")
	}

	if req.Candidate == "" {
		return fmt.Errorf("candidate is required")
	}

	if len(req.Candidate) > MaxCandidateLength {
		return fmt.Errorf("candidate exceeds maximum length of %d bytes", MaxCandidateLength)
	}

	if req.SDPMid == "" {
		return fmt.Errorf("sdp_mid is required")
	}

	if req.SDPMLineIndex < 0 {
		return fmt.Errorf("sdp_mline_index must be non-negative")
	}

	return nil
}
