package queue

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RedisPublisher publishes job messages to a Redis Stream using XADD.
type RedisPublisher struct {
	client *redis.Client
	stream string
}

// NewRedisPublisher creates a new RedisPublisher.
func NewRedisPublisher(client *redis.Client, stream string) *RedisPublisher {
	return &RedisPublisher{
		client: client,
		stream: stream,
	}
}

// Publish writes a job_id and type to the Redis Stream.
func (p *RedisPublisher) Publish(ctx context.Context, jobID uuid.UUID, jobType string) error {
	args := &redis.XAddArgs{
		Stream: p.stream,
		Values: map[string]interface{}{
			"job_id": jobID.String(),
			"type":   jobType,
		},
	}

	_, err := p.client.XAdd(ctx, args).Result()
	if err != nil {
		return fmt.Errorf("publishing to stream %q: %w", p.stream, err)
	}

	return nil
}

// Ensure interface compliance.
var _ Publisher = (*RedisPublisher)(nil)
