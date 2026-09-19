package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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

	query := `
		INSERT INTO jobs (id, type, payload, status, created_at)
		VALUES ($1, $2, $3, $4, now())
		RETURNING created_at
	`

	err := r.pool.QueryRow(ctx, query, job.ID, job.Type, job.Payload, job.Status).Scan(&job.CreatedAt)
	if err != nil {
		return fmt.Errorf("creating job: %w", err)
	}

	return nil
}

// GetByID retrieves a job by its unique identifier.
func (r *PostgresRepository) GetByID(ctx context.Context, id uuid.UUID) (*Job, error) {
	query := `
		SELECT id, type, payload, status, result, error, created_at, started_at, completed_at
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
		SELECT id, type, payload, status, result, error, created_at, started_at, completed_at
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
		return ErrInvalidState
	}

	return nil
}

// SetRunning transitions a job from QUEUED to RUNNING and sets started_at.
func (r *PostgresRepository) SetRunning(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE jobs
		SET status = $1, started_at = now()
		WHERE id = $2 AND status = $3
	`

	cmdTag, err := r.pool.Exec(ctx, query, StatusRunning, id, StatusQueued)
	if err != nil {
		return fmt.Errorf("setting job running: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return ErrInvalidState
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
		return ErrInvalidState
	}

	return nil
}

// SetFailed transitions a job from RUNNING to FAILED and records error message and completed_at.
func (r *PostgresRepository) SetFailed(ctx context.Context, id uuid.UUID, errMsg string) error {
	query := `
		UPDATE jobs
		SET status = $1, error = $2, completed_at = now()
		WHERE id = $3 AND status = $4
	`

	cmdTag, err := r.pool.Exec(ctx, query, StatusFailed, errMsg, id, StatusRunning)
	if err != nil {
		return fmt.Errorf("setting job failed: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return ErrInvalidState
	}

	return nil
}

// Ensure interface compliance.
var _ Repository = (*PostgresRepository)(nil)
