package queue

import (
	"context"

	"github.com/google/uuid"
)

// Message represents a task message received from the queue.
type Message struct {
	ID    string    `json:"id"`     // Queue-specific message identifier (e.g., Redis stream ID)
	JobID uuid.UUID `json:"job_id"` // FlowForge Job UUID
	Type  string    `json:"type"`   // Job type / task handler name
}

// Publisher publishes job execution requests to the queue.
type Publisher interface {
	Publish(ctx context.Context, jobID uuid.UUID, jobType string) error
}

// Consumer consumes job execution requests from the queue.
type Consumer interface {
	Consume(ctx context.Context) (<-chan Message, error)
	Ack(ctx context.Context, messageID string) error
	Close() error
}
