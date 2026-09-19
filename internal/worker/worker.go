package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"

	"github.com/flowforge/flowforge/internal/jobs"
	"github.com/flowforge/flowforge/internal/queue"
	"github.com/flowforge/flowforge/internal/retry"
	"github.com/flowforge/flowforge/internal/tasks"
)

// Worker consumes and executes jobs sequentially from the queue.
type Worker struct {
	workerID    string
	consumer    queue.Consumer
	repo        jobs.Repository
	registry    *tasks.Registry
	retryPolicy retry.Policy
	logger      *slog.Logger
}

// New creates a new Worker instance.
func New(consumer queue.Consumer, repo jobs.Repository, registry *tasks.Registry, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	workerID := fmt.Sprintf("worker-%s", uuid.New().String()[:8])
	return &Worker{
		workerID:    workerID,
		consumer:    consumer,
		repo:        repo,
		registry:    registry,
		retryPolicy: retry.DefaultPolicy(),
		logger:      logger,
	}
}

// WithWorkerID sets a custom worker ID.
func (w *Worker) WithWorkerID(id string) *Worker {
	if id != "" {
		w.workerID = id
	}
	return w
}

// WithRetryPolicy sets a custom retry backoff policy.
func (w *Worker) WithRetryPolicy(p retry.Policy) *Worker {
	w.retryPolicy = p
	return w
}

// Run starts the worker loop until the context is canceled.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("starting worker loop", "worker_id", w.workerID)

	msgChan, err := w.consumer.Consume(ctx)
	if err != nil {
		return fmt.Errorf("starting consumer: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("worker shutting down", "worker_id", w.workerID)
			return nil
		case msg, ok := <-msgChan:
			if !ok {
				w.logger.Info("message channel closed, exiting worker", "worker_id", w.workerID)
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

	// Step 2: Guard against replays - verify job is QUEUED (or PENDING/RETRY_WAIT during dispatcher delivery)
	// If a duplicate delivery arrives when job is already COMPLETED, FAILED, or RUNNING,
	// we acknowledge and discard it safely without re-executing.
	if job.Status != jobs.StatusQueued && job.Status != jobs.StatusPending && job.Status != jobs.StatusRetryWait {
		w.logger.Warn("job is not in QUEUED status; skipping duplicate or stale execution",
			"job_id", job.ID.String(),
			"status", job.Status,
		)
		_ = w.consumer.Ack(ctx, msg.ID)
		return nil
	}

	attemptNumber := job.AttemptCount + 1
	// Step 3: Transition QUEUED -> RUNNING (increments attempt_count in DB atomically)
	if err := w.repo.SetRunning(ctx, job.ID); err != nil {
		w.logger.Error("failed to transition job to RUNNING", "job_id", job.ID.String(), "error", err)
		return fmt.Errorf("setting job running: %w", err)
	}
	w.logger.Info("job status changed to RUNNING", "job_id", job.ID.String(), "attempt", attemptNumber)

	startedAt := time.Now().UTC()
	execCtx := tasks.WithJobContext(ctx, job.ID, attemptNumber)
	workerIDStr := w.workerID

	// Step 4: Lookup task handler
	handler, err := w.registry.Get(job.Type)
	if err != nil {
		errMsg := fmt.Sprintf("unsupported job type: %s", job.Type)
		w.logger.Error(errMsg, "job_id", job.ID.String())

		finishedAt := time.Now().UTC()
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()

		attempt := &jobs.JobAttempt{
			ID:            uuid.New(),
			JobID:         job.ID,
			AttemptNumber: attemptNumber,
			WorkerID:      &workerIDStr,
			Status:        "FAILED",
			Error:         &errMsg,
			StartedAt:     startedAt,
			FinishedAt:    finishedAt,
		}
		_ = w.repo.RecordAttempt(cleanupCtx, attempt)

		if failErr := w.repo.SetFailed(cleanupCtx, job.ID, errMsg); failErr != nil {
			w.logger.Error("failed to set job state to FAILED", "job_id", job.ID.String(), "error", failErr)
		}
		_ = w.consumer.Ack(cleanupCtx, msg.ID)
		return nil
	}

	// Step 5: Execute handler with panic recovery
	var (
		result   json.RawMessage
		execErr  error
		panicked bool
		panicVal interface{}
	)

	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				panicVal = r
				w.logger.Error("task handler panicked",
					"job_id", job.ID.String(),
					"attempt", attemptNumber,
					"type", job.Type,
					"panic", fmt.Sprintf("%v", r),
					"stack", string(debug.Stack()),
				)
			}
		}()
		result, execErr = handler.Handle(execCtx, job.Payload)
	}()

	finishedAt := time.Now().UTC()

	// Bounded cleanup context for terminal/retry DB persistence and ACK.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if panicked {
		sanitizedErr := fmt.Sprintf("task execution panicked: %v", panicVal)
		attempt := &jobs.JobAttempt{
			ID:            uuid.New(),
			JobID:         job.ID,
			AttemptNumber: attemptNumber,
			WorkerID:      &workerIDStr,
			Status:        "FAILED",
			Error:         &sanitizedErr,
			StartedAt:     startedAt,
			FinishedAt:    finishedAt,
		}
		_ = w.repo.RecordAttempt(cleanupCtx, attempt)

		if setErr := w.repo.SetFailed(cleanupCtx, job.ID, sanitizedErr); setErr != nil {
			w.logger.Error("failed to record job panic failure in db", "job_id", job.ID.String(), "error", setErr)
			return fmt.Errorf("recording job panic failure: %w", setErr)
		}
		w.logger.Warn("job marked as FAILED due to panic", "job_id", job.ID.String(), "attempt", attemptNumber)
	} else if execErr != nil {
		retryable := tasks.IsRetryable(execErr)
		maxAttempts := job.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 3
		}
		canRetry := retryable && attemptNumber < maxAttempts

		errStr := execErr.Error()
		attempt := &jobs.JobAttempt{
			ID:            uuid.New(),
			JobID:         job.ID,
			AttemptNumber: attemptNumber,
			WorkerID:      &workerIDStr,
			Status:        "FAILED",
			Error:         &errStr,
			StartedAt:     startedAt,
			FinishedAt:    finishedAt,
		}
		if recErr := w.repo.RecordAttempt(cleanupCtx, attempt); recErr != nil {
			w.logger.Error("failed to record attempt in db", "job_id", job.ID.String(), "error", recErr)
		}

		if canRetry {
			delay := w.retryPolicy.NextDelay(attemptNumber)
			nextAttemptAt := time.Now().UTC().Add(delay)
			if err := w.repo.SetRetryWait(cleanupCtx, job.ID, execErr.Error(), nextAttemptAt); err != nil {
				w.logger.Error("failed to transition job to RETRY_WAIT", "job_id", job.ID.String(), "error", err)
				return fmt.Errorf("setting job retry wait: %w", err)
			}
			w.logger.Info("job scheduled for retry",
				"job_id", job.ID.String(),
				"attempt", attemptNumber,
				"max_attempts", maxAttempts,
				"delay", delay.String(),
				"next_attempt_at", nextAttemptAt.Format(time.RFC3339),
			)
		} else {
			reason := "non-retryable permanent error"
			if retryable {
				reason = "retries exhausted"
			}
			if err := w.repo.SetFailed(cleanupCtx, job.ID, execErr.Error()); err != nil {
				w.logger.Error("failed to record job failure in db", "job_id", job.ID.String(), "error", err)
				return fmt.Errorf("recording job failure: %w", err)
			}
			w.logger.Warn("job marked as FAILED",
				"job_id", job.ID.String(),
				"attempt", attemptNumber,
				"max_attempts", maxAttempts,
				"reason", reason,
				"error", execErr.Error(),
			)
		}
	} else {
		attempt := &jobs.JobAttempt{
			ID:            uuid.New(),
			JobID:         job.ID,
			AttemptNumber: attemptNumber,
			WorkerID:      &workerIDStr,
			Status:        "COMPLETED",
			Error:         nil,
			StartedAt:     startedAt,
			FinishedAt:    finishedAt,
		}
		if recErr := w.repo.RecordAttempt(cleanupCtx, attempt); recErr != nil {
			w.logger.Error("failed to record attempt in db", "job_id", job.ID.String(), "error", recErr)
		}

		if setErr := w.repo.SetCompleted(cleanupCtx, job.ID, result); setErr != nil {
			w.logger.Error("failed to record job completion in db", "job_id", job.ID.String(), "error", setErr)
			return fmt.Errorf("recording job completion: %w", setErr)
		}
		w.logger.Info("job completed successfully", "job_id", job.ID.String(), "attempt", attemptNumber)
	}

	// Step 6: Acknowledge Redis message
	if ackErr := w.consumer.Ack(cleanupCtx, msg.ID); ackErr != nil {
		w.logger.Error("failed to ack message in redis", "msg_id", msg.ID, "error", ackErr)
		return fmt.Errorf("acknowledging message: %w", ackErr)
	}

	return nil
}
