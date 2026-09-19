package jobs

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Status represents the state of a job in the system.
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusQueued    Status = "QUEUED"
	StatusRunning   Status = "RUNNING"
	StatusRetryWait Status = "RETRY_WAIT"
	StatusCompleted Status = "COMPLETED"
	StatusFailed    Status = "FAILED"
)

// DefaultMaxAttempts is the default number of execution attempts for a job.
const DefaultMaxAttempts = 3

// Job represents a single background task and its execution state.
type Job struct {
	ID            uuid.UUID       `json:"id"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
	Status        Status          `json:"status"`
	Result        json.RawMessage `json:"result,omitempty"`
	Error         *string         `json:"error,omitempty"`
	AttemptCount  int             `json:"attempt_count"`
	MaxAttempts   int             `json:"max_attempts"`
	LastError     *string         `json:"last_error,omitempty"`
	NextAttemptAt *time.Time      `json:"next_attempt_at,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	StartedAt     *time.Time      `json:"started_at,omitempty"`
	CompletedAt   *time.Time      `json:"completed_at,omitempty"`
}

// OutboxStatus represents the publication status of an outbox event.
type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "PENDING"
	OutboxStatusPublished OutboxStatus = "PUBLISHED"
	OutboxStatusFailed    OutboxStatus = "FAILED"
)

// OutboxEvent represents a pending or published event for the Transactional Outbox pattern.
type OutboxEvent struct {
	ID          uuid.UUID       `json:"id"`
	AggregateID uuid.UUID       `json:"aggregate_id"`
	EventType   string          `json:"event_type"`
	Payload     json.RawMessage `json:"payload"`
	Status      OutboxStatus    `json:"status"`
	Attempts    int             `json:"attempts"`
	LastError   *string         `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	PublishedAt *time.Time      `json:"published_at,omitempty"`
}

// JobCreatedPayload defines the outbox event payload for JOB_CREATED events.
type JobCreatedPayload struct {
	JobID uuid.UUID `json:"job_id"`
	Type  string    `json:"type"`
}

// JobAttempt records a single execution attempt of a job.
type JobAttempt struct {
	ID            uuid.UUID  `json:"id"`
	JobID         uuid.UUID  `json:"job_id"`
	AttemptNumber int        `json:"attempt_number"`
	WorkerID      *string    `json:"worker_id,omitempty"`
	Status        string     `json:"status"`
	Error         *string    `json:"error,omitempty"`
	StartedAt     time.Time  `json:"started_at"`
	FinishedAt    time.Time  `json:"finished_at"`
}
