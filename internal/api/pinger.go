package api

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

// Pinger defines the interface for dependency connectivity and readiness checks.
type Pinger interface {
	Ping(ctx context.Context) error
}

// RedisPinger adapts a go-redis Client to satisfy the Pinger interface.
type RedisPinger struct {
	client *redis.Client
}

// NewRedisPinger creates a new RedisPinger.
func NewRedisPinger(client *redis.Client) *RedisPinger {
	return &RedisPinger{client: client}
}

// Ping performs a ping on the underlying Redis client.
func (p *RedisPinger) Ping(ctx context.Context) error {
	if p == nil || p.client == nil {
		return errors.New("redis client is nil")
	}
	return p.client.Ping(ctx).Err()
}
