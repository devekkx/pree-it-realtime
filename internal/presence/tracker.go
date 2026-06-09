package presence

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

var tracer = otel.Tracer("presence-tracker")

const (
	presencePrefix = "presence:"
	presenceTTL    = 90 * time.Second // must be > ping interval (50s)
)

// Tracker manages user online/offline state in Redis.
type Tracker struct {
	rdb    *redis.Client
	logger *zap.Logger
}

func NewTracker(rdb *redis.Client, logger *zap.Logger) *Tracker {
	return &Tracker{rdb: rdb, logger: logger}
}

func (t *Tracker) SetOnline(ctx context.Context, userID uuid.UUID) error {
	ctx, span := tracer.Start(ctx, "presence.SetOnline")
	defer span.End()
	span.SetAttributes(attribute.String("user.id", userID.String()))

	key := presencePrefix + userID.String()
	return t.rdb.Set(ctx, key, "online", presenceTTL).Err()
}

func (t *Tracker) SetOffline(ctx context.Context, userID uuid.UUID) error {
	ctx, span := tracer.Start(ctx, "presence.SetOffline")
	defer span.End()
	span.SetAttributes(attribute.String("user.id", userID.String()))

	key := presencePrefix + userID.String()
	return t.rdb.Del(ctx, key).Err()
}

// Heartbeat refreshes the TTL — called on every ping/pong cycle.
func (t *Tracker) Heartbeat(ctx context.Context, userID uuid.UUID) error {
	key := presencePrefix + userID.String()
	return t.rdb.Expire(ctx, key, presenceTTL).Err()
}

func (t *Tracker) IsOnline(ctx context.Context, userID uuid.UUID) (bool, error) {
	key := presencePrefix + userID.String()
	result, err := t.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("check presence: %w", err)
	}
	return result > 0, nil
}

// BulkIsOnline checks online status for multiple users.
func (t *Tracker) BulkIsOnline(ctx context.Context, userIDs []uuid.UUID) (map[uuid.UUID]bool, error) {
	ctx, span := tracer.Start(ctx, "presence.BulkIsOnline")
	defer span.End()

	if len(userIDs) == 0 {
		return map[uuid.UUID]bool{}, nil
	}

	pipe := t.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, len(userIDs))

	for i, id := range userIDs {
		cmds[i] = pipe.Exists(ctx, presencePrefix+id.String())
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("bulk presence check: %w", err)
	}

	result := make(map[uuid.UUID]bool, len(userIDs))
	for i, id := range userIDs {
		result[id] = cmds[i].Val() > 0
	}

	return result, nil
}
