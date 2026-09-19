package tasks

import (
	"context"
	"encoding/json"
)

// Handler represents the execution logic for a specific type of background task.
type Handler interface {
	// Handle executes the task logic with the provided payload.
	// It returns an optional JSON result on success, or an error on failure.
	Handle(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)
}
