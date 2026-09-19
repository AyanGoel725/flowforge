package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepository implements Repository using a pgxpool.Pool.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresRepository creates a new PostgresRepository.
func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// Create inserts a new job into the database.
func (r *PostgresRepository) Create(ctx context.Context, job *Job) error {
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	if job.Status == "" {
		job.Status = StatusPending
	}
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = DefaultMaxAttempts
	}

	query := `
		INSERT INTO jobs (id, type, payload, status, attempt_count, max_attempts, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		RETURNING created_at
	`

	err := r.pool.QueryRow(ctx, query, job.ID, job.Type, job.Payload, job.Status, job.AttemptCount, job.MaxAttempts).Scan(&job.CreatedAt)
	if err != nil {
		return fmt.Errorf("creating job: %w", err)
	}

	return nil
}

// CreateWithOutbox inserts a new job and an outbox event within a single database transaction.
func (r *PostgresRepository) CreateWithOutbox(ctx context.Context, job *Job, event *OutboxEvent) error {
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	if job.Status == "" {
		job.Status = StatusPending
	}
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = DefaultMaxAttempts
	}
	if event.ID == uuid.Nil {
		event.ID = uuid.New()
	}
	if event.Status == "" {
		event.Status = OutboxStatusPending
	}
	event.AggregateID = job.ID

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("starting tx for create with outbox: %w", err)
	}
	defer tx.Rollback(ctx)

	jobQuery := `
		INSERT INTO jobs (id, type, payload, status, attempt_count, max_attempts, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		RETURNING created_at
	`
	err = tx.QueryRow(ctx, jobQuery, job.ID, job.Type, job.Payload, job.Status, job.AttemptCount, job.MaxAttempts).Scan(&job.CreatedAt)
	if err != nil {
		return fmt.Errorf("inserting job: %w", err)
	}

	outboxQuery := `
		INSERT INTO outbox_events (id, aggregate_id, event_type, payload, status, attempts, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		RETURNING created_at
	`
	err = tx.QueryRow(ctx, outboxQuery, event.ID, event.AggregateID, event.EventType, event.Payload, event.Status, event.Attempts).Scan(&event.CreatedAt)
	if err != nil {
		return fmt.Errorf("inserting outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing create with outbox: %w", err)
	}

	return nil
}

// GetByID retrieves a job by its unique identifier.
func (r *PostgresRepository) GetByID(ctx context.Context, id uuid.UUID) (*Job, error) {
	query := `
		SELECT id, type, payload, status, result, error, attempt_count, max_attempts, last_error, next_attempt_at, created_at, started_at, completed_at
		FROM jobs
		WHERE id = $1
	`

	var job Job
	var statusStr string
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&job.ID,
		&job.Type,
		&job.Payload,
		&statusStr,
		&job.Result,
		&job.Error,
		&job.AttemptCount,
		&job.MaxAttempts,
		&job.LastError,
		&job.NextAttemptAt,
		&job.CreatedAt,
		&job.StartedAt,
		&job.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("getting job by id: %w", err)
	}

	job.Status = Status(statusStr)
	return &job, nil
}

// List returns a paginated list of jobs sorted by created_at DESC, along with total count.
func (r *PostgresRepository) List(ctx context.Context, limit, offset int) ([]*Job, int, error) {
	var total int
	countQuery := `SELECT count(*) FROM jobs`
	if err := r.pool.QueryRow(ctx, countQuery).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("counting jobs: %w", err)
	}

	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT id, type, payload, status, result, error, attempt_count, max_attempts, last_error, next_attempt_at, created_at, started_at, completed_at
		FROM jobs
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := r.pool.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("listing jobs: %w", err)
	}
	defer rows.Close()

	var jobsList []*Job
	for rows.Next() {
		var job Job
		var statusStr string
		if err := rows.Scan(
			&job.ID,
			&job.Type,
			&job.Payload,
			&statusStr,
			&job.Result,
			&job.Error,
			&job.AttemptCount,
			&job.MaxAttempts,
			&job.LastError,
			&job.NextAttemptAt,
			&job.CreatedAt,
			&job.StartedAt,
			&job.CompletedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("scanning job row: %w", err)
		}
		job.Status = Status(statusStr)
		jobsList = append(jobsList, &job)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterating job rows: %w", err)
	}

	return jobsList, total, nil
}

// checkNotFoundOrInvalidState disambiguates zero-row status updates.
func (r *PostgresRepository) checkNotFoundOrInvalidState(ctx context.Context, id uuid.UUID) error {
	var exists bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)`, id).Scan(&exists)
	if err != nil {
		return fmt.Errorf("checking job existence: %w", err)
	}
	if !exists {
		return ErrJobNotFound
	}
	return ErrInvalidState
}

// UpdateStatus performs an optimistic concurrency status update.
func (r *PostgresRepository) UpdateStatus(ctx context.Context, id uuid.UUID, from, to Status) error {
	query := `
		UPDATE jobs
		SET status = $1
		WHERE id = $2 AND status = $3
	`

	cmdTag, err := r.pool.Exec(ctx, query, to, id, from)
	if err != nil {
		return fmt.Errorf("updating job status from %s to %s: %w", from, to, err)
	}

	if cmdTag.RowsAffected() == 0 {
		return r.checkNotFoundOrInvalidState(ctx, id)
	}

	return nil
}

// SetRunning transitions a job from QUEUED (or PENDING/RETRY_WAIT) to RUNNING, increments attempt_count, and sets started_at.
func (r *PostgresRepository) SetRunning(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE jobs
		SET status = $1, started_at = COALESCE(started_at, now()), attempt_count = attempt_count + 1
		WHERE id = $2 AND (status = $3 OR status = 'PENDING' OR status = 'RETRY_WAIT')
	`

	cmdTag, err := r.pool.Exec(ctx, query, StatusRunning, id, StatusQueued)
	if err != nil {
		return fmt.Errorf("setting job running: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return r.checkNotFoundOrInvalidState(ctx, id)
	}

	return nil
}

// SetCompleted transitions a job from RUNNING to COMPLETED and records result and completed_at.
func (r *PostgresRepository) SetCompleted(ctx context.Context, id uuid.UUID, result json.RawMessage) error {
	query := `
		UPDATE jobs
		SET status = $1, result = $2, completed_at = now()
		WHERE id = $3 AND status = $4
	`

	cmdTag, err := r.pool.Exec(ctx, query, StatusCompleted, result, id, StatusRunning)
	if err != nil {
		return fmt.Errorf("setting job completed: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return r.checkNotFoundOrInvalidState(ctx, id)
	}

	return nil
}

// SetFailed transitions a job from RUNNING to FAILED and records error message and completed_at.
func (r *PostgresRepository) SetFailed(ctx context.Context, id uuid.UUID, errMsg string) error {
	query := `
		UPDATE jobs
		SET status = $1, error = $2, last_error = $2, completed_at = now()
		WHERE id = $3 AND status = $4
	`

	cmdTag, err := r.pool.Exec(ctx, query, StatusFailed, errMsg, id, StatusRunning)
	if err != nil {
		return fmt.Errorf("setting job failed: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return r.checkNotFoundOrInvalidState(ctx, id)
	}

	return nil
}

// SetRetryWait transitions a job from RUNNING to RETRY_WAIT and records last_error and next_attempt_at.
func (r *PostgresRepository) SetRetryWait(ctx context.Context, id uuid.UUID, errMsg string, nextAttemptAt time.Time) error {
	query := `
		UPDATE jobs
		SET status = $1, last_error = $2, next_attempt_at = $3
		WHERE id = $4 AND status = $5
	`

	cmdTag, err := r.pool.Exec(ctx, query, StatusRetryWait, errMsg, nextAttemptAt, id, StatusRunning)
	if err != nil {
		return fmt.Errorf("setting job retry wait: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return r.checkNotFoundOrInvalidState(ctx, id)
	}

	return nil
}

// ProcessPendingOutboxEvents claims pending outbox events using SELECT FOR UPDATE SKIP LOCKED,
// runs the processor for each, and updates outbox and job status atomically in PostgreSQL.
func (r *PostgresRepository) ProcessPendingOutboxEvents(ctx context.Context, limit int, processor OutboxProcessor) (int, error) {
	if limit <= 0 {
		limit = 100
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("beginning tx for outbox processing: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `
		SELECT id, aggregate_id, event_type, payload, status, attempts, last_error, created_at, published_at
		FROM outbox_events
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`
	rows, err := tx.Query(ctx, query, limit)
	if err != nil {
		return 0, fmt.Errorf("querying pending outbox events: %w", err)
	}
	defer rows.Close()

	var events []*OutboxEvent
	for rows.Next() {
		var evt OutboxEvent
		var statusStr string
		if err := rows.Scan(
			&evt.ID,
			&evt.AggregateID,
			&evt.EventType,
			&evt.Payload,
			&statusStr,
			&evt.Attempts,
			&evt.LastError,
			&evt.CreatedAt,
			&evt.PublishedAt,
		); err != nil {
			return 0, fmt.Errorf("scanning outbox event: %w", err)
		}
		evt.Status = OutboxStatus(statusStr)
		events = append(events, &evt)
	}
	rows.Close()

	if len(events) == 0 {
		return 0, nil
	}

	processedCount := 0
	for _, evt := range events {
		pubErr := processor(ctx, evt)
		if pubErr == nil {
			updateOutbox := `
				UPDATE outbox_events
				SET status = 'PUBLISHED', published_at = now(), attempts = attempts + 1
				WHERE id = $1
			`
			if _, err := tx.Exec(ctx, updateOutbox, evt.ID); err != nil {
				return processedCount, fmt.Errorf("marking outbox event published: %w", err)
			}

			updateJob := `
				UPDATE jobs
				SET status = 'QUEUED'
				WHERE id = $1 AND status = 'PENDING'
			`
			if _, err := tx.Exec(ctx, updateJob, evt.AggregateID); err != nil {
				return processedCount, fmt.Errorf("updating job status to queued: %w", err)
			}
			processedCount++
		} else {
			errMsg := pubErr.Error()
			updateOutbox := `
				UPDATE outbox_events
				SET attempts = attempts + 1, last_error = $1
				WHERE id = $2
			`
			if _, err := tx.Exec(ctx, updateOutbox, errMsg, evt.ID); err != nil {
				return processedCount, fmt.Errorf("updating outbox event failure: %w", err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("committing outbox processing tx: %w", err)
	}

	return processedCount, nil
}

// ProcessRetryEligibleJobs claims retryable jobs that are ready to run,
// runs the processor for each, and transitions them to QUEUED.
func (r *PostgresRepository) ProcessRetryEligibleJobs(ctx context.Context, limit int, processor RetryProcessor) (int, error) {
	if limit <= 0 {
		limit = 100
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("beginning tx for retry processing: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `
		SELECT id, type, payload, status, result, error, attempt_count, max_attempts, last_error, next_attempt_at, created_at, started_at, completed_at
		FROM jobs
		WHERE status = 'RETRY_WAIT' AND next_attempt_at <= now()
		ORDER BY next_attempt_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`
	rows, err := tx.Query(ctx, query, limit)
	if err != nil {
		return 0, fmt.Errorf("querying retry eligible jobs: %w", err)
	}
	defer rows.Close()

	var jobsList []*Job
	for rows.Next() {
		var job Job
		var statusStr string
		if err := rows.Scan(
			&job.ID,
			&job.Type,
			&job.Payload,
			&statusStr,
			&job.Result,
			&job.Error,
			&job.AttemptCount,
			&job.MaxAttempts,
			&job.LastError,
			&job.NextAttemptAt,
			&job.CreatedAt,
			&job.StartedAt,
			&job.CompletedAt,
		); err != nil {
			return 0, fmt.Errorf("scanning retry job: %w", err)
		}
		job.Status = Status(statusStr)
		jobsList = append(jobsList, &job)
	}
	rows.Close()

	if len(jobsList) == 0 {
		return 0, nil
	}

	processedCount := 0
	for _, job := range jobsList {
		if err := processor(ctx, job); err == nil {
			updateJob := `
				UPDATE jobs
				SET status = 'QUEUED', next_attempt_at = NULL
				WHERE id = $1 AND status = 'RETRY_WAIT'
			`
			if _, err := tx.Exec(ctx, updateJob, job.ID); err != nil {
				return processedCount, fmt.Errorf("updating retry job to queued: %w", err)
			}
			processedCount++
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("committing retry processing tx: %w", err)
	}

	return processedCount, nil
}

// RecordAttempt inserts a new execution attempt record.
func (r *PostgresRepository) RecordAttempt(ctx context.Context, attempt *JobAttempt) error {
	if attempt.ID == uuid.Nil {
		attempt.ID = uuid.New()
	}
	query := `
		INSERT INTO job_attempts (id, job_id, attempt_number, worker_id, status, error, started_at, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := r.pool.Exec(ctx, query,
		attempt.ID,
		attempt.JobID,
		attempt.AttemptNumber,
		attempt.WorkerID,
		attempt.Status,
		attempt.Error,
		attempt.StartedAt,
		attempt.FinishedAt,
	)
	if err != nil {
		return fmt.Errorf("recording job attempt: %w", err)
	}
	return nil
}

// GetAttemptsByJobID retrieves all execution attempts for a given job ordered by attempt number.
func (r *PostgresRepository) GetAttemptsByJobID(ctx context.Context, jobID uuid.UUID) ([]*JobAttempt, error) {
	query := `
		SELECT id, job_id, attempt_number, worker_id, status, error, started_at, finished_at
		FROM job_attempts
		WHERE job_id = $1
		ORDER BY attempt_number ASC
	`
	rows, err := r.pool.Query(ctx, query, jobID)
	if err != nil {
		return nil, fmt.Errorf("querying job attempts: %w", err)
	}
	defer rows.Close()

	var attempts []*JobAttempt
	for rows.Next() {
		var a JobAttempt
		if err := rows.Scan(
			&a.ID,
			&a.JobID,
			&a.AttemptNumber,
			&a.WorkerID,
			&a.Status,
			&a.Error,
			&a.StartedAt,
			&a.FinishedAt,
		); err != nil {
			return nil, fmt.Errorf("scanning job attempt: %w", err)
		}
		attempts = append(attempts, &a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating job attempts: %w", err)
	}
	return attempts, nil
}

// Ensure interface compliance.
var _ Repository = (*PostgresRepository)(nil)
