package jobs

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// OutboxProcessor is a callback used to process a claimed outbox event.
type OutboxProcessor func(ctx context.Context, event *OutboxEvent) error

// RetryProcessor is a callback used to process a claimed retryable job.
type RetryProcessor func(ctx context.Context, job *Job) error

// Repository defines data access methods for Jobs, Outbox Events, and Job Attempts.
type Repository interface {
	// Job operations
	Create(ctx context.Context, job *Job) error
	CreateWithOutbox(ctx context.Context, job *Job, event *OutboxEvent) error
	GetByID(ctx context.Context, id uuid.UUID) (*Job, error)
	List(ctx context.Context, limit, offset int) ([]*Job, int, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, from, to Status) error

	// Worker state transitions
	SetRunning(ctx context.Context, id uuid.UUID) error
	SetCompleted(ctx context.Context, id uuid.UUID, result json.RawMessage) error
	SetFailed(ctx context.Context, id uuid.UUID, errMsg string) error
	SetRetryWait(ctx context.Context, id uuid.UUID, errMsg string, nextAttemptAt time.Time) error

	// Dispatcher / Concurrency operations
	ProcessPendingOutboxEvents(ctx context.Context, limit int, processor OutboxProcessor) (int, error)
	ProcessRetryEligibleJobs(ctx context.Context, limit int, processor RetryProcessor) (int, error)

	// Attempt tracking
	RecordAttempt(ctx context.Context, attempt *JobAttempt) error
	GetAttemptsByJobID(ctx context.Context, jobID uuid.UUID) ([]*JobAttempt, error)
}
