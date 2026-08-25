package signaling

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ConnectionState represents the state of a WebSocket connection.
type ConnectionState int

const (
	ConnectionStateConnected ConnectionState = iota
	ConnectionStateDisconnected
	ConnectionStateClosed
)

// Connection represents a WebSocket connection for a participant.
type Connection struct {
	mu            sync.RWMutex
	wsConn        *websocket.Conn
	participantID string
	roomID        string
	state         ConnectionState
	lastActive    time.Time
	sendChan      chan []byte
	receiveChan   chan []byte
	closeChan     chan struct{}
	closeOnce     sync.Once
}

// NewConnection creates a new Connection from an HTTP request.
func NewConnection(w http.ResponseWriter, r *http.Request, roomID, participantID string) (*Connection, error) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			// TODO: Add proper origin checking
			return true
		},
	}

	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}

	conn := &Connection{
		wsConn:        wsConn,
		participantID: participantID,
		roomID:        roomID,
		state:         ConnectionStateConnected,
		lastActive:    time.Now(),
		sendChan:      make(chan []byte, 256),
		receiveChan:   make(chan []byte, 256),
		closeChan:     make(chan struct{}),
	}

	// Start read and write goroutines
	go conn.readLoop()
	go conn.writeLoop()

	return conn, nil
}

// ID returns the participant ID associated with this connection.
func (c *Connection) ID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.participantID
}

// RoomID returns the room ID associated with this connection.
func (c *Connection) RoomID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.roomID
}

// State returns the current connection state.
func (c *Connection) State() ConnectionState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// LastActive returns the last time the connection was active.
func (c *Connection) LastActive() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastActive
}

// Send sends a message to the client.
func (c *Connection) Send(msgType MessageType, data interface{}) error {
	msg, err := NewMessage(msgType, data)
	if err != nil {
		return err
	}

	dataBytes, err := msg.Marshal()
	if err != nil {
		return err
	}

	select {
	case c.sendChan <- dataBytes:
		c.updateLastActive()
		return nil
	case <-c.closeChan:
		return ErrConnectionClosed
	}
}

// Receive receives a message from the client.
func (c *Connection) Receive() (*Message, error) {
	select {
	case data := <-c.receiveChan:
		c.updateLastActive()
		return ParseMessage(data)
	case <-c.closeChan:
		return nil, ErrConnectionClosed
	}
}

// Close closes the connection.
func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeChan)
		_ = c.wsConn.Close()

		c.mu.Lock()
		c.state = ConnectionStateClosed
		c.mu.Unlock()
	})

	return nil
}

// IsClosed returns true if the connection is closed.
func (c *Connection) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state == ConnectionStateClosed
}

// updateLastActive updates the last active timestamp.
func (c *Connection) updateLastActive() {
	c.mu.Lock()
	c.lastActive = time.Now()
	c.mu.Unlock()
}

// readLoop reads messages from the WebSocket connection.
func (c *Connection) readLoop() {
	defer close(c.receiveChan)

	for {
		select {
		case <-c.closeChan:
			return
		default:
			_, data, err := c.wsConn.ReadMessage()
			if err != nil {
				if !websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("WebSocket read error: %v", err)
				}
				c.Close()
				return
			}

			select {
			case c.receiveChan <- data:
			case <-c.closeChan:
				return
			}
		}
	}
}

// writeLoop writes messages to the WebSocket connection.
func (c *Connection) writeLoop() {
	defer c.Close()

	for {
		select {
		case data := <-c.sendChan:
			if err := c.wsConn.WriteMessage(websocket.TextMessage, data); err != nil {
				if !websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("WebSocket write error: %v", err)
				}
				return
			}
		case <-c.closeChan:
			return
		}
	}
}

// Connection-related errors.
var (
	ErrConnectionClosed = fmt.Errorf("connection closed")
)
