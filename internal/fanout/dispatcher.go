package fanout

import (
	"encoding/json"

	"github.com/devekkx/pree-it-realtime/internal/conn"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

var tracer = otel.Tracer("fanout-dispatcher")

// Dispatcher routes events to the correct WebSocket clients.
type Dispatcher struct {
	manager *conn.Manager
	logger  *zap.Logger
}

func NewDispatcher(manager *conn.Manager, logger *zap.Logger) *Dispatcher {
	return &Dispatcher{
		manager: manager,
		logger:  logger,
	}
}

// ServerEvent is the envelope sent to WebSocket clients.
type ServerEvent struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

// SendToUser sends an event to all connections of a specific user.
func (d *Dispatcher) SendToUser(userID uuid.UUID, event ServerEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		d.logger.Error("failed to marshal event",
			zap.String("type", event.Type),
			zap.Error(err),
		)
		return
	}

	clients := d.manager.GetClientsByUserID(userID)
	for _, c := range clients {
		select {
		case c.Send <- data:
		default:
			d.logger.Warn("client send buffer full, dropping message",
				zap.String("client_id", c.ID),
				zap.String("user_id", userID.String()),
			)
		}
	}
}

// SendToUsers sends an event to all connections of multiple users.
func (d *Dispatcher) SendToUsers(userIDs []uuid.UUID, event ServerEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		d.logger.Error("failed to marshal event",
			zap.String("type", event.Type),
			zap.Error(err),
		)
		return
	}

	for _, userID := range userIDs {
		clients := d.manager.GetClientsByUserID(userID)
		for _, c := range clients {
			select {
			case c.Send <- data:
			default:
				d.logger.Warn("client send buffer full, dropping message",
					zap.String("client_id", c.ID),
					zap.String("user_id", userID.String()),
				)
			}
		}
	}
}
