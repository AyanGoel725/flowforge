package jobs

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/flowforge/flowforge/internal/queue"
)

// Service coordinates job operations across the database and message queue.
type Service struct {
	repo      Repository
	publisher queue.Publisher
	logger    *slog.Logger
}

// NewService creates a new job service.
func NewService(repo Repository, publisher queue.Publisher, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		repo:      repo,
		publisher: publisher,
		logger:    logger,
	}
}

// CreateJob validates the input, inserts a PENDING job into the database,
// attempts to publish it to the message queue, and transitions it to QUEUED.
// If queue publishing fails, the job remains in PENDING state (documented dual-write limitation).
func (s *Service) CreateJob(ctx context.Context, req *CreateJobRequest) (*Job, error) {
	if err := ValidateCreateRequest(req); err != nil {
		return nil, err
	}

	job := &Job{
		ID:      uuid.New(),
		Type:    req.Type,
		Payload: req.Payload,
		Status:  StatusPending,
	}

	if err := s.repo.Create(ctx, job); err != nil {
		return nil, fmt.Errorf("persisting job: %w", err)
	}

	s.logger.Info("job created in database", "job_id", job.ID.String(), "status", job.Status)

	// Attempt publishing to queue
	if s.publisher != nil {
		// Transition to QUEUED before publishing to ensure that the worker
		// sees the job as QUEUED when it picks up the message.
		if err := s.repo.UpdateStatus(ctx, job.ID, StatusPending, StatusQueued); err != nil {
			s.logger.Error("failed to update job status to QUEUED",
				"job_id", job.ID.String(),
				"error", err,
			)
			return nil, fmt.Errorf("updating job status to QUEUED: %w", err)
		}
		job.Status = StatusQueued

		if err := s.publisher.Publish(ctx, job.ID, job.Type); err != nil {
			s.logger.Error("failed to publish job to queue; reverting to PENDING",
				"job_id", job.ID.String(),
				"error", err,
			)
			// Revert to PENDING per documented dual-write limitation
			_ = s.repo.UpdateStatus(ctx, job.ID, StatusQueued, StatusPending)
			job.Status = StatusPending
			return job, nil
		}

		s.logger.Info("job queued successfully", "job_id", job.ID.String(), "status", job.Status)
	}

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
