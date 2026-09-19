package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
)

// Service coordinates job operations across the database.
// In Stage 2, it publishes jobs exclusively via the Transactional Outbox pattern.
type Service struct {
	repo   Repository
	logger *slog.Logger
}

// NewService creates a new job service.
func NewService(repo Repository, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		repo:   repo,
		logger: logger,
	}
}

// CreateJob validates the input, and persists a PENDING job and a JOB_CREATED
// outbox event in a single database transaction.
// It eliminates the dual-write race from Stage 1: the system guarantees
// that either both the job and the publication intent exist durably, or neither does.
	func (s *Service) CreateJob(ctx context.Context, req *CreateJobRequest) (*Job, error) {
	if err := ValidateCreateRequest(req); err != nil {
		return nil, err
	}

	jobID := uuid.New()
	maxAttempts := DefaultMaxAttempts
	if req.MaxAttempts != nil && *req.MaxAttempts > 0 {
		maxAttempts = *req.MaxAttempts
	}
	job := &Job{
		ID:           jobID,
		Type:         strings.TrimSpace(req.Type),
		Payload:      req.Payload,
		Status:       StatusPending,
		AttemptCount: 0,
		MaxAttempts:  maxAttempts,
	}

	payloadBytes, err := json.Marshal(&JobCreatedPayload{
		JobID: job.ID,
		Type:  job.Type,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling job created payload: %w", err)
	}

	outboxEvent := &OutboxEvent{
		ID:          uuid.New(),
		AggregateID: job.ID,
		EventType:   "JOB_CREATED",
		Payload:     payloadBytes,
		Status:      OutboxStatusPending,
	}

	if err := s.repo.CreateWithOutbox(ctx, job, outboxEvent); err != nil {
		return nil, fmt.Errorf("persisting job with outbox event: %w", err)
	}

	s.logger.Info("job and outbox event created durably", "job_id", job.ID.String(), "status", job.Status)
	return job, nil
}

// GetJob fetches a job by ID.
func (s *Service) GetJob(ctx context.Context, id uuid.UUID) (*Job, error) {
	return s.repo.GetByID(ctx, id)
}

// ListJobs returns a paginated list of jobs.
func (s *Service) ListJobs(ctx context.Context, limit, offset int) ([]*Job, int, error) {
	return s.repo.List(ctx, limit, offset)
}
