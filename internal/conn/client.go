package conn

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	writeTimeout = 10 * time.Second
	pongWait     = 60 * time.Second
	pingInterval = 50 * time.Second
	maxMsgSize   = 4096
)

// Client represents a single WebSocket connection.
type Client struct {
	ID     string
	UserID uuid.UUID
	Conn   *websocket.Conn
	Send   chan []byte
	mu     sync.Mutex
	logger *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

func NewClient(ctx context.Context, userID uuid.UUID, conn *websocket.Conn, logger *zap.Logger) *Client {
	clientCtx, cancel := context.WithCancel(ctx)
	return &Client{
		ID:     uuid.New().String(),
		UserID: userID,
		Conn:   conn,
		Send:   make(chan []byte, 256),
		logger: logger,
		ctx:    clientCtx,
		cancel: cancel,
	}
}

func (c *Client) Context() context.Context {
	return c.ctx
}

func (c *Client) Close() {
	c.cancel()
	close(c.Send)
	c.Conn.Close(websocket.StatusNormalClosure, "closing")
}

// WritePump sends messages from the Send channel to the WebSocket.
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingInterval)
	defer func() {
		ticker.Stop()
		c.Conn.Close(websocket.StatusNormalClosure, "write pump closed")
	}()

	for {
		select {
		case <-c.ctx.Done():
			return
		case msg, ok := <-c.Send:
			if !ok {
				return
			}
			ctx, cancel := context.WithTimeout(c.ctx, writeTimeout)
			err := c.Conn.Write(ctx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				c.logger.Debug("write failed, closing client",
					zap.String("client_id", c.ID),
					zap.Error(err),
				)
				return
			}
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(c.ctx, writeTimeout)
			err := c.Conn.Ping(ctx)
			cancel()
			if err != nil {
				c.logger.Debug("ping failed, closing client",
					zap.String("client_id", c.ID),
					zap.Error(err),
				)
				return
			}
		}
	}
}

// ReadPump reads messages from the WebSocket (client - server events).
func (c *Client) ReadPump(onMessage func(client *Client, msgType string, payload json.RawMessage)) {
	defer c.cancel()

	for {
		_, data, err := c.Conn.Read(c.ctx)
		if err != nil {
			c.logger.Debug("read failed, closing client",
				zap.String("client_id", c.ID),
				zap.Error(err),
			)
			return
		}

		if len(data) > maxMsgSize {
			c.logger.Warn("message too large, dropping",
				zap.String("client_id", c.ID),
				zap.Int("size", len(data)),
			)
			continue
		}

		var envelope struct {
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil {
			c.logger.Debug("invalid message format",
				zap.String("client_id", c.ID),
				zap.Error(err),
			)
			continue
		}

		if envelope.Type == "" {
			continue
		}

		onMessage(c, envelope.Type, envelope.Payload)
	}
}
