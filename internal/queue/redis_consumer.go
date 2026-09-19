package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RedisConsumer consumes job messages from a Redis Stream using consumer groups (XREADGROUP).
type RedisConsumer struct {
	client       *redis.Client
	stream       string
	group        string
	consumerName string
	logger       *slog.Logger
	stopOnce     sync.Once
	doneChan     chan struct{}
}

// NewRedisConsumer creates a new RedisConsumer.
func NewRedisConsumer(client *redis.Client, stream, group, consumerName string, logger *slog.Logger) *RedisConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	if consumerName == "" {
		consumerName = fmt.Sprintf("worker-%s", uuid.New().String()[:8])
	}
	return &RedisConsumer{
		client:       client,
		stream:       stream,
		group:        group,
		consumerName: consumerName,
		logger:       logger,
		doneChan:     make(chan struct{}),
	}
}

// ensureGroup creates the consumer group and stream if they don't already exist.
func (c *RedisConsumer) ensureGroup(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(ctx, c.stream, c.group, "0").Err()
	if err != nil {
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}
		return fmt.Errorf("creating consumer group %q on stream %q: %w", c.group, c.stream, err)
	}
	c.logger.Info("created consumer group", "stream", c.stream, "group", c.group)
	return nil
}

// Consume starts listening for new messages in a background goroutine and sends them to the returned channel.
func (c *RedisConsumer) Consume(ctx context.Context) (<-chan Message, error) {
	if err := c.ensureGroup(ctx); err != nil {
		return nil, err
	}

	msgChan := make(chan Message, 100)

	go func() {
		defer close(msgChan)

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.doneChan:
				return
			default:
			}

			// Block for 2 seconds waiting for new messages (ID ">" means unread by any consumer in group)
			streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    c.group,
				Consumer: c.consumerName,
				Streams:  []string{c.stream, ">"},
				Count:    10,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				// If closing down
				select {
				case <-ctx.Done():
					return
				case <-c.doneChan:
					return
				default:
					c.logger.Error("error reading from redis stream", "error", err)
					time.Sleep(500 * time.Millisecond)
					continue
				}
			}

			for _, stream := range streams {
				for _, xmsg := range stream.Messages {
					jobIDStr, _ := xmsg.Values["job_id"].(string)
					jobType, _ := xmsg.Values["type"].(string)

					jobID, err := uuid.Parse(jobIDStr)
					if err != nil {
						c.logger.Error("invalid job_id in queue message",
							"message_id", xmsg.ID,
							"raw_job_id", jobIDStr,
							"error", err,
						)
						// Ack invalid message so it does not block the queue
						_ = c.Ack(ctx, xmsg.ID)
						continue
					}

					msg := Message{
						ID:    xmsg.ID,
						JobID: jobID,
						Type:  jobType,
					}

					select {
					case msgChan <- msg:
					case <-ctx.Done():
						return
					case <-c.doneChan:
						return
					}
				}
			}
		}
	}()

	return msgChan, nil
}

// Ack acknowledges a processed message.
func (c *RedisConsumer) Ack(ctx context.Context, messageID string) error {
	err := c.client.XAck(ctx, c.stream, c.group, messageID).Err()
	if err != nil {
		return fmt.Errorf("acknowledging message %s: %w", messageID, err)
	}
	return nil
}

// Close signals consumer goroutines to terminate.
func (c *RedisConsumer) Close() error {
	c.stopOnce.Do(func() {
		close(c.doneChan)
	})
	return nil
}

// Ensure interface compliance.
var _ Consumer = (*RedisConsumer)(nil)
