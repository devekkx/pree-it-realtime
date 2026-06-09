package handler

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/coder/websocket"
	"github.com/devekkx/pree-it-realtime/internal/conn"
	"github.com/devekkx/pree-it-realtime/internal/fanout"
	"github.com/devekkx/pree-it-realtime/internal/presence"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

var tracer = otel.Tracer("ws-handler")

type WSHandler struct {
	manager    *conn.Manager
	dispatcher *fanout.Dispatcher
	tracker    *presence.Tracker
	logger     *zap.Logger
}

func NewWSHandler(
	manager *conn.Manager,
	dispatcher *fanout.Dispatcher,
	tracker *presence.Tracker,
	logger *zap.Logger,
) *WSHandler {
	return &WSHandler{
		manager:    manager,
		dispatcher: dispatcher,
		tracker:    tracker,
		logger:     logger,
	}
}

func (h *WSHandler) HandleConnect(c *gin.Context) {
	userIDStr := c.GetHeader("X-User-ID")
	if userIDStr == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing X-User-ID"})
		return
	}

	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid X-User-ID"})
		return
	}

	wsConn, err := websocket.Accept(c.Writer, c.Request, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // CORS handled by gateway/Caddy
	})
	if err != nil {
		h.logger.Error("websocket accept failed", zap.Error(err))
		return
	}

	ctx := c.Request.Context()

	client := conn.NewClient(ctx, userID, wsConn, h.logger)
	h.manager.Register(client)

	// Set online in Redis
	if err := h.tracker.SetOnline(ctx, userID); err != nil {
		h.logger.Warn("failed to set user online", zap.Error(err))
	}

	h.logger.Info("WebSocket connected",
		zap.String("user_id", userID.String()),
		zap.String("client_id", client.ID),
	)

	// Start write pump in a goroutine
	go client.WritePump()

	// Read pump blocks until disconnect
	client.ReadPump(h.onClientMessage)

	// Client disconnected — cleanup
	h.manager.Unregister(client)

	// Only set offline if this was the user's last connection
	if !h.manager.IsUserOnline(userID) {
		if err := h.tracker.SetOffline(context.Background(), userID); err != nil {
			h.logger.Warn("failed to set user offline", zap.Error(err))
		}
	}

	h.logger.Info("WebSocket disconnected",
		zap.String("user_id", userID.String()),
		zap.String("client_id", client.ID),
	)
}

// onClientMessage handles client → server events (typing indicators, etc.)
func (h *WSHandler) onClientMessage(client *conn.Client, msgType string, payload json.RawMessage) {
	_, span := tracer.Start(client.Context(), "ws.onClientMessage")
	defer span.End()

	switch msgType {
	case "user.typing":
		h.handleTyping(client, payload)
	case "user.stop_typing":
		h.handleStopTyping(client, payload)
	case "heartbeat":
		if err := h.tracker.Heartbeat(client.Context(), client.UserID); err != nil {
			h.logger.Warn("heartbeat refresh failed", zap.Error(err))
		}
	default:
		h.logger.Debug("unknown client event type",
			zap.String("type", msgType),
			zap.String("client_id", client.ID),
		)
	}
}

type typingPayload struct {
	ConversationID uuid.UUID `json:"conversation_id"`
}

func (h *WSHandler) handleTyping(client *conn.Client, payload json.RawMessage) {
	var p typingPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return
	}

	// Broadcast typing indicator to all other users' connections
	// In production, you'd look up conversation members here.
	// For now, we broadcast to the conversation members the client is part of.
	h.logger.Debug("typing event",
		zap.String("user_id", client.UserID.String()),
		zap.String("conversation_id", p.ConversationID.String()),
	)
}

func (h *WSHandler) handleStopTyping(client *conn.Client, payload json.RawMessage) {
	var p typingPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return
	}

	h.logger.Debug("stop typing event",
		zap.String("user_id", client.UserID.String()),
		zap.String("conversation_id", p.ConversationID.String()),
	)
}
