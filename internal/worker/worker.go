package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/flowforge/flowforge/internal/jobs"
	"github.com/flowforge/flowforge/internal/queue"
	"github.com/flowforge/flowforge/internal/tasks"
)

// Worker consumes and executes jobs sequentially from the queue.
type Worker struct {
	consumer queue.Consumer
	repo     jobs.Repository
	registry *tasks.Registry
	logger   *slog.Logger
}

// New creates a new Worker instance.
func New(consumer queue.Consumer, repo jobs.Repository, registry *tasks.Registry, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		consumer: consumer,
		repo:     repo,
		registry: registry,
		logger:   logger,
	}
}

// Run starts the worker loop until the context is canceled.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("starting worker loop")

	msgChan, err := w.consumer.Consume(ctx)
	if err != nil {
		return fmt.Errorf("starting consumer: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("worker shutting down")
			return nil
		case msg, ok := <-msgChan:
			if !ok {
				w.logger.Info("message channel closed, exiting worker")
				return nil
			}
			if err := w.processJob(ctx, msg); err != nil {
				w.logger.Error("error processing job",
					"job_id", msg.JobID.String(),
					"type", msg.Type,
					"error", err,
				)
			}
		}
	}
}

// processJob executes a single job through its lifecycle.
func (w *Worker) processJob(ctx context.Context, msg queue.Message) error {
	w.logger.Info("received job message", "job_id", msg.JobID.String(), "type", msg.Type, "msg_id", msg.ID)

	// Step 1: Load job from PostgreSQL
	job, err := w.repo.GetByID(ctx, msg.JobID)
	if err != nil {
		if errors.Is(err, jobs.ErrJobNotFound) {
			w.logger.Warn("job not found in database; acknowledging message", "job_id", msg.JobID.String())
			_ = w.consumer.Ack(ctx, msg.ID)
			return nil
		}
		return fmt.Errorf("loading job %s: %w", msg.JobID, err)
	}

	// Step 2: Guard against replays - verify job is QUEUED
	if job.Status != jobs.StatusQueued {
		w.logger.Warn("job is not in QUEUED status; skipping execution",
			"job_id", job.ID.String(),
			"status", job.Status,
		)
		_ = w.consumer.Ack(ctx, msg.ID)
		return nil
	}

	// Step 3: Transition QUEUED -> RUNNING
	if err := w.repo.SetRunning(ctx, job.ID); err != nil {
		w.logger.Error("failed to transition job to RUNNING", "job_id", job.ID.String(), "error", err)
		return fmt.Errorf("setting job running: %w", err)
	}
	w.logger.Info("job status changed to RUNNING", "job_id", job.ID.String())

	// Step 4: Lookup task handler
	handler, err := w.registry.Get(job.Type)
	if err != nil {
		// Handler not registered -> mark job as FAILED
		errMsg := fmt.Sprintf("unsupported job type: %s", job.Type)
		w.logger.Error(errMsg, "job_id", job.ID.String())
		if failErr := w.repo.SetFailed(ctx, job.ID, errMsg); failErr != nil {
			w.logger.Error("failed to set job state to FAILED", "job_id", job.ID.String(), "error", failErr)
		}
		_ = w.consumer.Ack(ctx, msg.ID)
		return nil
	}

	// Step 5: Execute handler
	result, execErr := handler.Handle(ctx, job.Payload)
	if execErr != nil {
		// Step 6b: On failure -> FAILED
		w.logger.Error("task handler execution failed",
			"job_id", job.ID.String(),
			"type", job.Type,
			"error", execErr,
		)
		if setErr := w.repo.SetFailed(ctx, job.ID, execErr.Error()); setErr != nil {
			w.logger.Error("failed to record job failure in db", "job_id", job.ID.String(), "error", setErr)
			return fmt.Errorf("recording job failure: %w", setErr)
		}
	} else {
		// Step 6a: On success -> COMPLETED
		if setErr := w.repo.SetCompleted(ctx, job.ID, result); setErr != nil {
			w.logger.Error("failed to record job completion in db", "job_id", job.ID.String(), "error", setErr)
			return fmt.Errorf("recording job completion: %w", setErr)
		}
		w.logger.Info("job completed successfully", "job_id", job.ID.String())
	}

	// Step 7: Acknowledge Redis message
	if ackErr := w.consumer.Ack(ctx, msg.ID); ackErr != nil {
		w.logger.Error("failed to ack message in redis", "msg_id", msg.ID, "error", ackErr)
		return fmt.Errorf("acknowledging message: %w", ackErr)
	}

	return nil
}
