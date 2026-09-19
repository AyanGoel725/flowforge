package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/flowforge/flowforge/internal/jobs"
	"github.com/flowforge/flowforge/internal/queue"
)

// Dispatcher polls for PENDING outbox events and RETRY_WAIT jobs and publishes them to the message queue.
type Dispatcher struct {
	repo      jobs.Repository
	publisher queue.Publisher
	logger    *slog.Logger
}

type Options struct {
	PollInterval time.Duration
	BatchSize    int
}

// New creates a new Transactional Outbox Dispatcher.
func New(repo jobs.Repository, publisher queue.Publisher, logger *slog.Logger) *Dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Dispatcher{
		repo:      repo,
		publisher: publisher,
		logger:    logger,
	}
}

// Run starts the dispatcher loop until the context is canceled.
func (d *Dispatcher) Run(ctx context.Context, opts Options) error {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 500 * time.Millisecond
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}

	d.logger.Info("dispatcher started", "poll_interval", opts.PollInterval, "batch_size", opts.BatchSize)

	ticker := time.NewTicker(opts.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("dispatcher shutting down")
			return nil
		case <-ticker.C:
			d.processOutboxBatch(ctx, opts.BatchSize)
			d.processRetryBatch(ctx, opts.BatchSize)
		}
	}
}

// processOutboxBatch claims and processes pending outbox events using SELECT FOR UPDATE SKIP LOCKED.
func (d *Dispatcher) processOutboxBatch(ctx context.Context, batchSize int) {
	processor := func(ctx context.Context, event *jobs.OutboxEvent) error {
		if event.EventType != "JOB_CREATED" {
			d.logger.Warn("unknown event type in outbox", "event_id", event.ID.String(), "event_type", event.EventType)
			return fmt.Errorf("unknown event type: %s", event.EventType)
		}

		var payload jobs.JobCreatedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("unmarshaling job created payload: %w", err)
		}

		if err := d.publisher.Publish(ctx, payload.JobID, payload.Type); err != nil {
			return fmt.Errorf("publishing to queue: %w", err)
		}

		return nil
	}

	count, err := d.repo.ProcessPendingOutboxEvents(ctx, batchSize, processor)
	if err != nil {
		d.logger.Error("failed to process outbox events", "error", err)
		return
	}

	if count > 0 {
		d.logger.Info("processed outbox batch", "published_count", count)
	}
}

// processRetryBatch claims and requeues eligible RETRY_WAIT jobs whose next_attempt_at <= now().
func (d *Dispatcher) processRetryBatch(ctx context.Context, batchSize int) {
	processor := func(ctx context.Context, job *jobs.Job) error {
		if err := d.publisher.Publish(ctx, job.ID, job.Type); err != nil {
			return fmt.Errorf("publishing retry job %s to queue: %w", job.ID, err)
		}
		return nil
	}

	count, err := d.repo.ProcessRetryEligibleJobs(ctx, batchSize, processor)
	if err != nil {
		d.logger.Error("failed to process retry eligible jobs", "error", err)
		return
	}

	if count > 0 {
		d.logger.Info("processed retry eligible jobs", "requeued_count", count)
	}
}
