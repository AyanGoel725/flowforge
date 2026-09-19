package tasks

import (
	"context"

	"github.com/google/uuid"
)

type contextKey string

const (
	jobIDKey         contextKey = "flowforge:job_id"
	attemptNumberKey contextKey = "flowforge:attempt_number"
)

// WithJobContext enriches a context with JobID and AttemptNumber for task execution.
func WithJobContext(ctx context.Context, jobID uuid.UUID, attemptNumber int) context.Context {
	ctx = context.WithValue(ctx, jobIDKey, jobID)
	return context.WithValue(ctx, attemptNumberKey, attemptNumber)
}

// GetJobID extracts the JobID from context, or uuid.Nil if not present.
func GetJobID(ctx context.Context) uuid.UUID {
	if val, ok := ctx.Value(jobIDKey).(uuid.UUID); ok {
		return val
	}
	return uuid.Nil
}

// GetAttemptNumber extracts the current execution attempt number from context (1-indexed), or 1 if unset.
func GetAttemptNumber(ctx context.Context) int {
	if val, ok := ctx.Value(attemptNumberKey).(int); ok && val > 0 {
		return val
	}
	return 1
}
