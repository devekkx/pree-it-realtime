package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/devekkx/pree-it-realtime/internal/fanout"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

var tracer = otel.Tracer("nats-consumer")

// MessageEvent matches the structure published by chat-service.
type MessageEvent struct {
	MessageID      uuid.UUID `json:"message_id"`
	ConversationID uuid.UUID `json:"conversation_id"`
	SenderID       uuid.UUID `json:"sender_id"`
	Content        string    `json:"content,omitempty"`
	IsEdited       bool      `json:"is_edited"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// MemberLookup resolves conversation member IDs.
// The realtime service needs to know which users are in a conversation
// to fan out events. This is satisfied by an HTTP call to chat-service
// or a local cache. For now, we use a function type so the caller can inject it.
type MemberLookup func(ctx context.Context, conversationID uuid.UUID) ([]uuid.UUID, error)

type Consumer struct {
	nc           *nats.Conn
	dispatcher   *fanout.Dispatcher
	memberLookup MemberLookup
	logger       *zap.Logger
}

func New(nc *nats.Conn, dispatcher *fanout.Dispatcher, memberLookup MemberLookup, logger *zap.Logger) *Consumer {
	return &Consumer{
		nc:           nc,
		dispatcher:   dispatcher,
		memberLookup: memberLookup,
		logger:       logger,
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	js, err := jetstream.New(c.nc)
	if err != nil {
		return fmt.Errorf("create jetstream context: %w", err)
	}

	consumer, err := js.CreateOrUpdateConsumer(ctx, "CHAT_EVENTS", jetstream.ConsumerConfig{
		Durable:       "realtime-consumer",
		FilterSubject: "chat.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		MaxDeliver:    5,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	c.logger.Info("NATS consumer started", zap.String("stream", "CHAT_EVENTS"))

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msgs, err := consumer.Fetch(10, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.logger.Warn("fetch error", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			for msg := range msgs.Messages() {
				c.handleMessage(ctx, msg)
			}
		}
	}()

	return nil
}

func (c *Consumer) handleMessage(ctx context.Context, msg jetstream.Msg) {
	ctx, span := tracer.Start(ctx, "consumer.handleMessage")
	defer span.End()

	subject := msg.Subject()
	span.SetAttributes(attribute.String("nats.subject", subject))

	var event MessageEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		c.logger.Error("failed to unmarshal event",
			zap.String("subject", subject),
			zap.Error(err),
		)
		msg.Ack()
		return
	}

	// Look up conversation members to determine who should receive this event
	memberIDs, err := c.memberLookup(ctx, event.ConversationID)
	if err != nil {
		c.logger.Error("failed to lookup members",
			zap.String("conversation_id", event.ConversationID.String()),
			zap.Error(err),
		)
		msg.Nak()
		return
	}

	// Map NATS subject to client event type
	var eventType string
	switch subject {
	case "chat.message.created":
		eventType = "message.created"
	case "chat.message.updated":
		eventType = "message.updated"
	case "chat.message.deleted":
		eventType = "message.deleted"
	default:
		c.logger.Warn("unknown subject", zap.String("subject", subject))
		msg.Ack()
		return
	}

	c.dispatcher.SendToUsers(memberIDs, fanout.ServerEvent{
		Type:    eventType,
		Payload: event,
	})

	msg.Ack()

	c.logger.Debug("event dispatched",
		zap.String("type", eventType),
		zap.String("conversation_id", event.ConversationID.String()),
		zap.Int("recipients", len(memberIDs)),
	)
}
