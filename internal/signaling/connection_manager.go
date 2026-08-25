package signaling

import (
	"sync"
)

// ConnectionManager manages WebSocket connections for participants.
type ConnectionManager struct {
	mu          sync.RWMutex
	connections map[string]*Connection
}

// NewConnectionManager creates a new ConnectionManager.
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		connections: make(map[string]*Connection),
	}
}

// Add adds a connection to the manager.
func (cm *ConnectionManager) Add(conn *Connection) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.connections[conn.ID()] = conn
}

// Remove removes a connection from the manager.
func (cm *ConnectionManager) Remove(connID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.connections, connID)
}

// Get retrieves a connection by participant ID.
// Returns nil if the connection is not found.
func (cm *ConnectionManager) Get(participantID string) *Connection {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return cm.connections[participantID]
}

// GetByRoom returns all connections for a given room ID.
func (cm *ConnectionManager) GetByRoom(roomID string) []*Connection {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var connections []*Connection
	for _, conn := range cm.connections {
		if conn.RoomID() == roomID {
			connections = append(connections, conn)
		}
	}
	return connections
}

// ConnectionIDs returns a slice of all connection IDs.
func (cm *ConnectionManager) ConnectionIDs() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	ids := make([]string, 0, len(cm.connections))
	for id := range cm.connections {
		ids = append(ids, id)
	}
	return ids
}

// CloseAll closes all connections in the manager.
func (cm *ConnectionManager) CloseAll() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for _, conn := range cm.connections {
		conn.Close()
	}
	cm.connections = make(map[string]*Connection)
}
