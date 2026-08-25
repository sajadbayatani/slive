package signaling

import (
	"sync"

	"github.com/sajadbayatani/slive/internal/domain"
)

// RoomManager manages the lifecycle of rooms.
type RoomManager struct {
	mu    sync.RWMutex
	rooms map[string]*domain.Room
}

// NewRoomManager creates a new RoomManager.
func NewRoomManager() *RoomManager {
	return &RoomManager{
		rooms: make(map[string]*domain.Room),
	}
}

// CreateRoom creates a new room with the given ID.
// Returns an error if the room already exists.
func (rm *RoomManager) CreateRoom(roomID string) (*domain.Room, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.rooms[roomID]; exists {
		return nil, ErrRoomAlreadyExists
	}

	room := domain.NewRoom(roomID)
	if err := room.Create(); err != nil {
		return nil, err
	}

	rm.rooms[roomID] = room
	return room, nil
}

// GetRoom retrieves a room by ID.
// Returns nil if the room is not found.
func (rm *RoomManager) GetRoom(roomID string) *domain.Room {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	return rm.rooms[roomID]
}

// GetOrCreateRoom retrieves a room by ID or creates it if it doesn't exist.
func (rm *RoomManager) GetOrCreateRoom(roomID string) (*domain.Room, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	room, exists := rm.rooms[roomID]
	if exists {
		return room, nil
	}

	room = domain.NewRoom(roomID)
	if err := room.Create(); err != nil {
		return nil, err
	}

	rm.rooms[roomID] = room
	return room, nil
}

// CloseRoom closes a room and removes it from the manager.
func (rm *RoomManager) CloseRoom(roomID string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	room, exists := rm.rooms[roomID]
	if !exists {
		return ErrRoomNotFound
	}

	if err := room.Close(); err != nil {
		return err
	}

	delete(rm.rooms, roomID)
	return nil
}

// RoomIDs returns a slice of all room IDs.
func (rm *RoomManager) RoomIDs() []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	ids := make([]string, 0, len(rm.rooms))
	for id := range rm.rooms {
		ids = append(ids, id)
	}
	return ids
}

// RoomManager errors.
var (
	ErrRoomAlreadyExists = domain.ErrParticipantAlreadyExists // Reuse existing error
	ErrRoomNotFound      = domain.ErrParticipantNotFound      // Reuse existing error
)
