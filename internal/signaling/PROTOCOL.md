# Slive Signaling Protocol

## Overview

The Slive signaling protocol is a WebSocket-based protocol for real-time audio/video communication session negotiation. It handles room management, participant lifecycle, track management, and WebRTC signaling (SDP exchange, ICE candidates).

## Protocol Design

### Transport
- **Transport**: WebSocket (using `github.com/gorilla/websocket`)
- **Message Format**: JSON with `type` and `data` fields
- **Message Types**: String constants defining the operation

### Message Structure

All messages follow this structure:
```json
{
  "type": "message_type",
  "data": { ... }
}
```

## Message Types

### Room Management

| Message Type | Direction | Description |
|--------------|-----------|-------------|
| `create_room` | Client → Server | Request to create a new room |
| `room_created` | Server → Client | Response to room creation |
| `join_room` | Client → Server | Request to join an existing room |
| `room_joined` | Server → Client | Response to join request with room info |
| `leave_room` | Client → Server | Request to leave a room |
| `room_left` | Server → Client | Confirmation of leaving a room |

### Participant Management

| Message Type | Direction | Description |
|--------------|-----------|-------------|
| `participant_joined` | Server → Client | Notification that a new participant joined |
| `participant_left` | Server → Client | Notification that a participant left |

### Track Management

| Message Type | Direction | Description |
|--------------|-----------|-------------|
| `publish_track` | Client → Server | Request to publish a media track |
| `track_published` | Server → Client | Response to publish request |
| `unpublish_track` | Client → Server | Request to unpublish a track |
| `track_unpublished` | Server → Client | Response to unpublish request |
| `subscribe_track` | Client → Server | Request to subscribe to a track |
| `track_subscribed` | Server → Client | Response to subscribe request |
| `track_available` | Server → Client | Notification that a track is available |
| `track_unavailable` | Server → Client | Notification that a track is unavailable |

### WebRTC Signaling

| Message Type | Direction | Description |
|--------------|-----------|-------------|
| `webrtc:offer` | Client → Server | WebRTC offer (SDP) |
| `webrtc:answer` | Client → Server | WebRTC answer (SDP) |
| `webrtc:ice-candidate` | Client → Server | ICE candidate information |

### Error Handling

| Message Type | Direction | Description |
|--------------|-----------|-------------|
| `error` | Server → Client | Error response |

## Message Definitions

### Room Messages

#### CreateRoomRequest
```json
{
  "room_id": "string",
  "participant_id": "string",
  "participant_name": "string"
}
```

#### RoomCreatedResponse
```json
{
  "room_id": "string",
  "participant_id": "string",
  "status": "success"
}
```

#### JoinRoomRequest
```json
{
  "room_id": "string",
  "participant_id": "string",
  "participant_name": "string"
}
```

#### RoomJoinedResponse
```json
{
  "room_id": "string",
  "participant_id": "string",
  "participants": [
    {
      "id": "string",
      "name": "string"
    }
  ],
  "status": "success"
}
```

#### LeaveRoomRequest
```json
{
  "room_id": "string",
  "participant_id": "string"
}
```

#### RoomLeftResponse
```json
{
  "room_id": "string",
  "participant_id": "string",
  "status": "success"
}
```

### Participant Messages

#### ParticipantJoinedNotification
```json
{
  "participant": {
    "id": "string",
    "name": "string"
  }
}
```

#### ParticipantLeftNotification
```json
{
  "participant_id": "string"
}
```

### Track Messages

#### TrackInfo
```json
{
  "id": "string",
  "kind": "audio" | "video",
  "source": "microphone" | "camera" | "screen_share"
}
```

#### PublishTrackRequest
```json
{
  "room_id": "string",
  "participant_id": "string",
  "track": TrackInfo
}
```

#### TrackPublishedResponse
```json
{
  "track_id": "string",
  "participant_id": "string",
  "status": "success"
}
```

#### TrackAvailableNotification
```json
{
  "participant_id": "string",
  "track": TrackInfo
}
```

#### UnpublishTrackRequest
```json
{
  "room_id": "string",
  "participant_id": "string",
  "track_id": "string"
}
```

#### TrackUnpublishedResponse
```json
{
  "track_id": "string",
  "participant_id": "string",
  "status": "success"
}
```

#### TrackUnavailableNotification
```json
{
  "participant_id": "string",
  "track_id": "string"
}
```

#### SubscribeTrackRequest
```json
{
  "room_id": "string",
  "participant_id": "string",
  "track_id": "string"
}
```

#### TrackSubscribedResponse
```json
{
  "track_id": "string",
  "publisher_id": "string",
  "status": "success"
}
```

### WebRTC Signaling Messages

#### OfferRequest
```json
{
  "room_id": "string",
  "participant_id": "string",
  "target_participant_id": "string",
  "sdp": "string",
  "track_ids": ["string"]
}
```

#### OfferNotification
```json
{
  "source_participant_id": "string",
  "sdp": "string",
  "track_ids": ["string"]
}
```

#### AnswerRequest
```json
{
  "room_id": "string",
  "participant_id": "string",
  "target_participant_id": "string",
  "sdp": "string"
}
```

#### AnswerNotification
```json
{
  "source_participant_id": "string",
  "sdp": "string"
}
```

#### ICECandidateRequest
```json
{
  "room_id": "string",
  "participant_id": "string",
  "target_participant_id": "string",
  "candidate": "string",
  "sdp_mid": "string",
  "sdp_mline_index": 0
}
```

#### ICECandidateNotification
```json
{
  "source_participant_id": "string",
  "candidate": "string",
  "sdp_mid": "string",
  "sdp_mline_index": 0
}
```

### Error Messages

#### ErrorResponse
```json
{
  "error": "string",
  "code": "string",
  "request_type": "string"
}
```

#### Error Codes
- `room_not_found`
- `room_closed`
- `participant_not_found`
- `track_not_found`
- `invalid_request`
- `internal_error`

## Protocol Flow

### Room Creation
1. Client sends `create_room` request
2. Server creates room and participant
3. Server responds with `room_created`
4. Server broadcasts `participant_joined` to other participants (if any)

### Joining a Room
1. Client sends `join_room` request
2. Server adds participant to room
3. Server responds with `room_joined` (includes list of existing participants)
4. Server broadcasts `participant_joined` to other participants

### Leaving a Room
1. Client sends `leave_room` request or connection closes
2. Server removes participant from room
3. Server responds with `room_left` (if request was explicit)
4. Server broadcasts `participant_left` to other participants

### Publishing a Track
1. Client sends `publish_track` request with track info
2. Server creates track and associates it with participant
3. Server responds with `track_published`
4. Server broadcasts `track_available` to other participants

### Unpublishing a Track
1. Client sends `unpublish_track` request
2. Server removes track from participant
3. Server responds with `track_unpublished`
4. Server broadcasts `track_unavailable` to other participants

### Subscribing to a Track
1. Client sends `subscribe_track` request with track ID
2. Server finds the track and adds it to participant's subscriptions
3. Server responds with `track_subscribed`

### WebRTC Offer/Answer Exchange
1. Client A sends `offer` to Server targeting Client B
2. Server relays `offer` to Client B as `offer` notification
3. Client B sends `answer` to Server targeting Client A
4. Server relays `answer` to Client A as `answer` notification

### ICE Candidate Exchange
1. Client sends `ice_candidate` to Server targeting another client
2. Server relays `ice_candidate` to target client as `ice_candidate` notification

## Implementation Details

### Components
- **Handler**: HTTP handler for WebSocket connections, manages message routing
- **Connection**: WebSocket connection wrapper with send/receive channels
- **ConnectionManager**: Manages all active WebSocket connections
- **RoomManager**: Manages room lifecycle and participant membership

### Integration with Domain Layer
- Uses `domain.Room` for room state management
- Uses `domain.Participant` for participant state management
- Uses `domain.Track` for track state management

### Concurrency
- Each connection runs in its own goroutine
- Connection read/write operations use buffered channels
- Room and Connection managers use mutexes for thread-safe access

## Error Handling
- Domain errors are converted to signaling error codes
- Connection errors are handled gracefully
- Invalid messages result in error responses
- Connection closure is handled with cleanup of participant state
