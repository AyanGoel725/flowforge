package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// MockRepository implements Repository in-memory for testing.
type MockRepository struct {
	jobs             map[uuid.UUID]*Job
	outboxEvents     map[uuid.UUID]*OutboxEvent
	attempts         map[uuid.UUID][]*JobAttempt
	createErr        error
	createOutboxErr  error
	getErr           error
	updateErr        error
	listErr          error
}

func NewMockRepository() *MockRepository {
	return &MockRepository{
		jobs:         make(map[uuid.UUID]*Job),
		outboxEvents: make(map[uuid.UUID]*OutboxEvent),
		attempts:     make(map[uuid.UUID][]*JobAttempt),
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

func (m *MockRepository) CreateWithOutbox(ctx context.Context, job *Job, event *OutboxEvent) error {
	if m.createOutboxErr != nil {
		return m.createOutboxErr
	}
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	if event.ID == uuid.Nil {
		event.ID = uuid.New()
	}
	event.AggregateID = job.ID
	m.jobs[job.ID] = job
	m.outboxEvents[event.ID] = event
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
	job, ok := m.jobs[id]
	if !ok || job.Status != StatusQueued {
		return ErrInvalidState
	}
	job.Status = StatusRunning
	job.AttemptCount++
	now := time.Now()
	if job.StartedAt == nil {
		job.StartedAt = &now
	}
	return nil
}

func (m *MockRepository) SetCompleted(ctx context.Context, id uuid.UUID, result json.RawMessage) error {
	if err := m.UpdateStatus(ctx, id, StatusRunning, StatusCompleted); err != nil {
		return err
	}
	m.jobs[id].Result = result
	now := time.Now()
	m.jobs[id].CompletedAt = &now
	return nil
}

func (m *MockRepository) SetFailed(ctx context.Context, id uuid.UUID, errMsg string) error {
	if err := m.UpdateStatus(ctx, id, StatusRunning, StatusFailed); err != nil {
		return err
	}
	m.jobs[id].Error = &errMsg
	m.jobs[id].LastError = &errMsg
	now := time.Now()
	m.jobs[id].CompletedAt = &now
	return nil
}

func (m *MockRepository) SetRetryWait(ctx context.Context, id uuid.UUID, errMsg string, nextAttemptAt time.Time) error {
	if err := m.UpdateStatus(ctx, id, StatusRunning, StatusRetryWait); err != nil {
		return err
	}
	m.jobs[id].LastError = &errMsg
	m.jobs[id].NextAttemptAt = &nextAttemptAt
	return nil
}

func (m *MockRepository) ProcessPendingOutboxEvents(ctx context.Context, limit int, processor OutboxProcessor) (int, error) {
	count := 0
	for _, event := range m.outboxEvents {
		if event.Status == OutboxStatusPending {
			if err := processor(ctx, event); err == nil {
				event.Status = OutboxStatusPublished
				if job, ok := m.jobs[event.AggregateID]; ok && job.Status == StatusPending {
					job.Status = StatusQueued
				}
				count++
			}
		}
	}
	return count, nil
}

func (m *MockRepository) ProcessRetryEligibleJobs(ctx context.Context, limit int, processor RetryProcessor) (int, error) {
	count := 0
	now := time.Now()
	for _, job := range m.jobs {
		if job.Status == StatusRetryWait && job.NextAttemptAt != nil && !job.NextAttemptAt.After(now) {
			if err := processor(ctx, job); err == nil {
				job.Status = StatusQueued
				job.NextAttemptAt = nil
				count++
			}
		}
	}
	return count, nil
}

func (m *MockRepository) RecordAttempt(ctx context.Context, attempt *JobAttempt) error {
	m.attempts[attempt.JobID] = append(m.attempts[attempt.JobID], attempt)
	return nil
}

func (m *MockRepository) GetAttemptsByJobID(ctx context.Context, jobID uuid.UUID) ([]*JobAttempt, error) {
	return m.attempts[jobID], nil
}

func TestService_CreateJob_Success(t *testing.T) {
	repo := NewMockRepository()
	svc := NewService(repo, nil)

	req := &CreateJobRequest{
		Type:    "echo",
		Payload: json.RawMessage(`{"message": "hello"}`),
	}

	job, err := svc.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if job.Status != StatusPending {
		t.Errorf("expected job status %s, got %s", StatusPending, job.Status)
	}

	if len(repo.jobs) != 1 {
		t.Fatalf("expected 1 job in repo, got %d", len(repo.jobs))
	}

	if len(repo.outboxEvents) != 1 {
		t.Fatalf("expected 1 outbox event in repo, got %d", len(repo.outboxEvents))
	}

	for _, event := range repo.outboxEvents {
		if event.AggregateID != job.ID {
			t.Errorf("expected outbox event aggregate ID %s, got %s", job.ID, event.AggregateID)
		}
		if event.EventType != "JOB_CREATED" {
			t.Errorf("expected event type JOB_CREATED, got %s", event.EventType)
		}
	}
}

func TestService_CreateJob_ValidationError(t *testing.T) {
	repo := NewMockRepository()
	svc := NewService(repo, nil)

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
	svc := NewService(repo, nil)

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
	svc := NewService(repo, nil)

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
