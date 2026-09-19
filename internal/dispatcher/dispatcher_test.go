package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/flowforge/flowforge/internal/jobs"
)

type mockRepository struct {
	events []*jobs.OutboxEvent
	jobs   map[uuid.UUID]*jobs.Job
}

func (m *mockRepository) Create(ctx context.Context, job *jobs.Job) error { return nil }
func (m *mockRepository) CreateWithOutbox(ctx context.Context, job *jobs.Job, event *jobs.OutboxEvent) error {
	m.jobs[job.ID] = job
	m.events = append(m.events, event)
	return nil
}
func (m *mockRepository) GetByID(ctx context.Context, id uuid.UUID) (*jobs.Job, error) {
	if j, ok := m.jobs[id]; ok {
		return j, nil
	}
	return nil, jobs.ErrJobNotFound
}
func (m *mockRepository) List(ctx context.Context, limit, offset int) ([]*jobs.Job, int, error) {
	return nil, 0, nil
}
func (m *mockRepository) UpdateStatus(ctx context.Context, id uuid.UUID, from, to jobs.Status) error {
	return nil
}
func (m *mockRepository) SetRunning(ctx context.Context, id uuid.UUID) error { return nil }
func (m *mockRepository) SetCompleted(ctx context.Context, id uuid.UUID, result json.RawMessage) error {
	return nil
}
func (m *mockRepository) SetFailed(ctx context.Context, id uuid.UUID, errMsg string) error {
	return nil
}
func (m *mockRepository) SetRetryWait(ctx context.Context, id uuid.UUID, errMsg string, nextAttemptAt time.Time) error {
	return nil
}
func (m *mockRepository) ProcessPendingOutboxEvents(ctx context.Context, limit int, processor jobs.OutboxProcessor) (int, error) {
	processed := 0
	for _, evt := range m.events {
		if evt.Status == jobs.OutboxStatusPending {
			if err := processor(ctx, evt); err == nil {
				evt.Status = jobs.OutboxStatusPublished
				if j, ok := m.jobs[evt.AggregateID]; ok {
					j.Status = jobs.StatusQueued
				}
				processed++
			}
		}
	}
	return processed, nil
}
func (m *mockRepository) ProcessRetryEligibleJobs(ctx context.Context, limit int, processor jobs.RetryProcessor) (int, error) {
	processed := 0
	for _, j := range m.jobs {
		if j.Status == jobs.StatusRetryWait {
			if err := processor(ctx, j); err == nil {
				j.Status = jobs.StatusQueued
				j.NextAttemptAt = nil
				processed++
			}
		}
	}
	return processed, nil
}
func (m *mockRepository) RecordAttempt(ctx context.Context, attempt *jobs.JobAttempt) error {
	return nil
}
func (m *mockRepository) GetAttemptsByJobID(ctx context.Context, jobID uuid.UUID) ([]*jobs.JobAttempt, error) {
	return nil, nil
}

type mockPublisher struct {
	published []uuid.UUID
	err       error
}

func (p *mockPublisher) Publish(ctx context.Context, jobID uuid.UUID, jobType string) error {
	if p.err != nil {
		return p.err
	}
	p.published = append(p.published, jobID)
	return nil
}

func TestDispatcher_ProcessOutboxBatch_Success(t *testing.T) {
	jobID := uuid.New()
	payload, _ := json.Marshal(&jobs.JobCreatedPayload{
		JobID: jobID,
		Type:  "echo",
	})

	repo := &mockRepository{
		jobs: map[uuid.UUID]*jobs.Job{
			jobID: {ID: jobID, Status: jobs.StatusPending},
		},
		events: []*jobs.OutboxEvent{
			{
				ID:          uuid.New(),
				AggregateID: jobID,
				EventType:   "JOB_CREATED",
				Payload:     payload,
				Status:      jobs.OutboxStatusPending,
			},
		},
	}
	pub := &mockPublisher{}

	d := New(repo, pub, nil)
	d.processOutboxBatch(context.Background(), 10)

	if len(pub.published) != 1 || pub.published[0] != jobID {
		t.Fatalf("expected job %s to be published to queue, got: %v", jobID, pub.published)
	}

	if repo.events[0].Status != jobs.OutboxStatusPublished {
		t.Errorf("expected outbox event status PUBLISHED, got %s", repo.events[0].Status)
	}

	if repo.jobs[jobID].Status != jobs.StatusQueued {
		t.Errorf("expected job status QUEUED, got %s", repo.jobs[jobID].Status)
	}
}

func TestDispatcher_ProcessOutboxBatch_PublishFailure(t *testing.T) {
	jobID := uuid.New()
	payload, _ := json.Marshal(&jobs.JobCreatedPayload{
		JobID: jobID,
		Type:  "echo",
	})

	repo := &mockRepository{
		jobs: map[uuid.UUID]*jobs.Job{
			jobID: {ID: jobID, Status: jobs.StatusPending},
		},
		events: []*jobs.OutboxEvent{
			{
				ID:          uuid.New(),
				AggregateID: jobID,
				EventType:   "JOB_CREATED",
				Payload:     payload,
				Status:      jobs.OutboxStatusPending,
			},
		},
	}
	pub := &mockPublisher{err: errors.New("redis down")}

	d := New(repo, pub, nil)
	d.processOutboxBatch(context.Background(), 10)

	if len(pub.published) != 0 {
		t.Errorf("expected 0 published jobs on failure, got %d", len(pub.published))
	}

	if repo.events[0].Status != jobs.OutboxStatusPending {
		t.Errorf("expected outbox event status PENDING, got %s", repo.events[0].Status)
	}

	if repo.jobs[jobID].Status != jobs.StatusPending {
		t.Errorf("expected job status PENDING, got %s", repo.jobs[jobID].Status)
	}
}

func TestDispatcher_ProcessRetryBatch_Success(t *testing.T) {
	jobID := uuid.New()
	past := time.Now().Add(-1 * time.Minute)

	repo := &mockRepository{
		jobs: map[uuid.UUID]*jobs.Job{
			jobID: {
				ID:            jobID,
				Type:          "flaky",
				Status:        jobs.StatusRetryWait,
				NextAttemptAt: &past,
			},
		},
	}
	pub := &mockPublisher{}

	d := New(repo, pub, nil)
	d.processRetryBatch(context.Background(), 10)

	if len(pub.published) != 1 || pub.published[0] != jobID {
		t.Fatalf("expected retry job %s to be published to queue, got: %v", jobID, pub.published)
	}

	if repo.jobs[jobID].Status != jobs.StatusQueued {
		t.Errorf("expected job status QUEUED, got %s", repo.jobs[jobID].Status)
	}

	if repo.jobs[jobID].NextAttemptAt != nil {
		t.Errorf("expected NextAttemptAt to be cleared, got: %v", repo.jobs[jobID].NextAttemptAt)
	}
}
