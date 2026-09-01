package signaling

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Default WebSocket timeouts.
const (
	DefaultWSReadTimeout  = 60 * time.Second
	DefaultWSPingInterval = 30 * time.Second
	DefaultWSWriteTimeout = 10 * time.Second
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
	// logger is resolved at construction (nil becomes slog.Default()) and
	// immutable afterwards; the read/write loops use it without locking.
	logger *slog.Logger

	readTimeout  time.Duration
	writeTimeout time.Duration
	pingInterval time.Duration
}

// isOriginAllowed implements D1: no-Origin → allow; Origin host equal to
// request Host → allow; exact match against allowlist → allow; else reject.
func isOriginAllowed(r *http.Request, allowedOrigins []string) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	for _, a := range allowedOrigins {
		if a == origin {
			return true
		}
	}
	if u, err := url.Parse(origin); err == nil {
		if u.Host == r.Host {
			return true
		}
	}
	return false
}

// NewConnection creates a new Connection from an HTTP request. A nil logger
// resolves to slog.Default(). It uses default timeouts and no origin allowlist.
func NewConnection(logger *slog.Logger, w http.ResponseWriter, r *http.Request, roomID, participantID string) (*Connection, error) {
	return NewConnectionWithConfig(logger, w, r, roomID, participantID, nil, DefaultWSReadTimeout, DefaultWSPingInterval, DefaultWSWriteTimeout)
}

// NewConnectionWithConfig creates a Connection with explicit origin and timeout config.
func NewConnectionWithConfig(logger *slog.Logger, w http.ResponseWriter, r *http.Request, roomID, participantID string, allowedOrigins []string, readTimeout, pingInterval, writeTimeout time.Duration) (*Connection, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if readTimeout <= 0 {
		readTimeout = DefaultWSReadTimeout
	}
	if writeTimeout <= 0 {
		writeTimeout = DefaultWSWriteTimeout
	}
	if pingInterval <= 0 {
		pingInterval = DefaultWSPingInterval
	}
	if pingInterval > readTimeout/2 {
		pingInterval = readTimeout / 2
	}

	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return isOriginAllowed(r, allowedOrigins)
		},
	}

	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}

	// Configure deadlines and handlers.
	_ = wsConn.SetReadDeadline(time.Now().Add(readTimeout))
	wsConn.SetPongHandler(func(string) error {
		_ = wsConn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})
	wsConn.SetPingHandler(func(appData string) error {
		_ = wsConn.SetReadDeadline(time.Now().Add(readTimeout))
		_ = wsConn.SetWriteDeadline(time.Now().Add(writeTimeout))
		return wsConn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(writeTimeout))
	})

	conn := &Connection{
		wsConn:        wsConn,
		participantID: participantID,
		roomID:        roomID,
		state:         ConnectionStateConnected,
		lastActive:    time.Now(),
		sendChan:      make(chan []byte, 256),
		receiveChan:   make(chan []byte, 256),
		closeChan:     make(chan struct{}),
		logger:        logger,
		readTimeout:   readTimeout,
		writeTimeout:  writeTimeout,
		pingInterval:  pingInterval,
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
					c.logger.Warn("websocket read failed",
						"event", "ws_read_failed",
						"participant_id", c.participantID,
						"room_id", c.roomID,
						"error", err,
					)
				}
				c.Close()
				return
			}
			// Refresh deadline after successful read so idle timeout only fires when no messages at all.
			_ = c.wsConn.SetReadDeadline(time.Now().Add(c.readTimeout))

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

	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case data := <-c.sendChan:
			_ = c.wsConn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
			if err := c.wsConn.WriteMessage(websocket.TextMessage, data); err != nil {
				if !websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					c.logger.Warn("websocket write failed",
						"event", "ws_write_failed",
						"participant_id", c.participantID,
						"room_id", c.roomID,
						"error", err,
					)
				}
				return
			}
		case <-ticker.C:
			_ = c.wsConn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
			if err := c.wsConn.WriteControl(websocket.PingMessage, nil, time.Now().Add(c.writeTimeout)); err != nil {
				c.logger.Warn("websocket ping failed",
					"event", "ws_ping_failed",
					"participant_id", c.participantID,
					"room_id", c.roomID,
					"error", err,
				)
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
