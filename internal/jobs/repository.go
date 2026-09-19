package jobs

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
)

// Repository defines data access methods for Jobs.
type Repository interface {
	Create(ctx context.Context, job *Job) error
	GetByID(ctx context.Context, id uuid.UUID) (*Job, error)
	List(ctx context.Context, limit, offset int) ([]*Job, int, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, from, to Status) error
	SetRunning(ctx context.Context, id uuid.UUID) error
	SetCompleted(ctx context.Context, id uuid.UUID, result json.RawMessage) error
	SetFailed(ctx context.Context, id uuid.UUID, errMsg string) error
}
