package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
)

// MockRepository implements Repository in-memory for testing.
type MockRepository struct {
	jobs      map[uuid.UUID]*Job
	createErr error
	getErr    error
	updateErr error
	listErr   error
}

func NewMockRepository() *MockRepository {
	return &MockRepository{
		jobs: make(map[uuid.UUID]*Job),
	}
}

func (m *MockRepository) Create(ctx context.Context, job *Job) error {
	if m.createErr != nil {
		return m.createErr
	}
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	m.jobs[job.ID] = job
	return nil
}

func (m *MockRepository) GetByID(ctx context.Context, id uuid.UUID) (*Job, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	job, ok := m.jobs[id]
	if !ok {
		return nil, ErrJobNotFound
	}
	return job, nil
}

func (m *MockRepository) List(ctx context.Context, limit, offset int) ([]*Job, int, error) {
	if m.listErr != nil {
		return nil, 0, m.listErr
	}
	var res []*Job
	for _, j := range m.jobs {
		res = append(res, j)
	}
	return res, len(res), nil
}

func (m *MockRepository) UpdateStatus(ctx context.Context, id uuid.UUID, from, to Status) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	job, ok := m.jobs[id]
	if !ok || job.Status != from {
		return ErrInvalidState
	}
	job.Status = to
	return nil
}

func (m *MockRepository) SetRunning(ctx context.Context, id uuid.UUID) error {
	return m.UpdateStatus(ctx, id, StatusQueued, StatusRunning)
}

func (m *MockRepository) SetCompleted(ctx context.Context, id uuid.UUID, result json.RawMessage) error {
	if err := m.UpdateStatus(ctx, id, StatusRunning, StatusCompleted); err != nil {
		return err
	}
	m.jobs[id].Result = result
	return nil
}

func (m *MockRepository) SetFailed(ctx context.Context, id uuid.UUID, errMsg string) error {
	if err := m.UpdateStatus(ctx, id, StatusRunning, StatusFailed); err != nil {
		return err
	}
	m.jobs[id].Error = &errMsg
	return nil
}

// MockPublisher implements queue.Publisher.
type MockPublisher struct {
	published []uuid.UUID
	err       error
}

func (p *MockPublisher) Publish(ctx context.Context, jobID uuid.UUID, jobType string) error {
	if p.err != nil {
		return p.err
	}
	p.published = append(p.published, jobID)
	return nil
}

func TestService_CreateJob_Success(t *testing.T) {
	repo := NewMockRepository()
	pub := &MockPublisher{}
	svc := NewService(repo, pub, nil)

	req := &CreateJobRequest{
		Type:    "echo",
		Payload: json.RawMessage(`{"message": "hello"}`),
	}

	job, err := svc.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if job.Status != StatusQueued {
		t.Errorf("expected job status %s, got %s", StatusQueued, job.Status)
	}

	if len(pub.published) != 1 || pub.published[0] != job.ID {
		t.Errorf("expected job ID %s to be published", job.ID)
	}
}

func TestService_CreateJob_PublishFailure(t *testing.T) {
	repo := NewMockRepository()
	pub := &MockPublisher{err: errors.New("redis unavailable")}
	svc := NewService(repo, pub, nil)

	req := &CreateJobRequest{
		Type:    "echo",
		Payload: json.RawMessage(`{"message": "hello"}`),
	}

	job, err := svc.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("expected graceful return of PENDING job, got error: %v", err)
	}

	if job.Status != StatusPending {
		t.Errorf("expected job status %s on publish failure, got %s", StatusPending, job.Status)
	}
}

func TestService_CreateJob_ValidationError(t *testing.T) {
	repo := NewMockRepository()
	pub := &MockPublisher{}
	svc := NewService(repo, pub, nil)

	req := &CreateJobRequest{
		Type:    "",
		Payload: json.RawMessage(`{}`),
	}

	_, err := svc.CreateJob(context.Background(), req)
	if !errors.Is(err, ErrEmptyType) {
		t.Fatalf("expected ErrEmptyType, got: %v", err)
	}
}

func TestService_GetJob(t *testing.T) {
	repo := NewMockRepository()
	svc := NewService(repo, nil, nil)

	jobID := uuid.New()
	repo.jobs[jobID] = &Job{
		ID:     jobID,
		Type:   "echo",
		Status: StatusCompleted,
	}

	job, err := svc.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if job.ID != jobID {
		t.Errorf("expected job ID %s, got %s", jobID, job.ID)
	}

	_, err = svc.GetJob(context.Background(), uuid.New())
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got: %v", err)
	}
}

func TestService_ListJobs(t *testing.T) {
	repo := NewMockRepository()
	svc := NewService(repo, nil, nil)

	id1 := uuid.New()
	id2 := uuid.New()
	repo.jobs[id1] = &Job{ID: id1, Type: "echo"}
	repo.jobs[id2] = &Job{ID: id2, Type: "sleep"}

	jobsList, total, err := svc.ListJobs(context.Background(), 10, 0)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if total != 2 || len(jobsList) != 2 {
		t.Errorf("expected 2 jobs, got total=%d, len=%d", total, len(jobsList))
	}
}
