package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/flowforge/flowforge/internal/jobs"
	"github.com/flowforge/flowforge/internal/queue"
	"github.com/flowforge/flowforge/internal/retry"
	"github.com/flowforge/flowforge/internal/tasks"
)

// MockConsumer implements queue.Consumer for worker testing.
type MockConsumer struct {
	mu       sync.Mutex
	messages []queue.Message
	acked    []string
	closed   bool
}

func NewMockConsumer(msgs ...queue.Message) *MockConsumer {
	return &MockConsumer{
		messages: msgs,
		acked:    make([]string, 0),
	}
}

func (m *MockConsumer) Consume(ctx context.Context) (<-chan queue.Message, error) {
	ch := make(chan queue.Message, len(m.messages))
	for _, msg := range m.messages {
		ch <- msg
	}
	close(ch)
	return ch, nil
}

func (m *MockConsumer) Ack(ctx context.Context, messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = append(m.acked, messageID)
	return nil
}

func (m *MockConsumer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// MockRepo implements jobs.Repository in-memory for worker tests.
type MockRepo struct {
	mu       sync.Mutex
	jobs     map[uuid.UUID]*jobs.Job
	attempts map[uuid.UUID][]*jobs.JobAttempt
	events   []*jobs.OutboxEvent
	getErr   error
	runErr   error
	compErr  error
	failErr  error
	retryErr error
}

func NewMockRepo() *MockRepo {
	return &MockRepo{
		jobs:     make(map[uuid.UUID]*jobs.Job),
		attempts: make(map[uuid.UUID][]*jobs.JobAttempt),
	}
}

func (r *MockRepo) Create(ctx context.Context, job *jobs.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[job.ID] = job
	return nil
}

func (r *MockRepo) CreateWithOutbox(ctx context.Context, job *jobs.Job, event *jobs.OutboxEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[job.ID] = job
	r.events = append(r.events, event)
	return nil
}

func (r *MockRepo) GetByID(ctx context.Context, id uuid.UUID) (*jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.getErr != nil {
		return nil, r.getErr
	}
	job, ok := r.jobs[id]
	if !ok {
		return nil, jobs.ErrJobNotFound
	}
	return job, nil
}

func (r *MockRepo) List(ctx context.Context, limit, offset int) ([]*jobs.Job, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	list := make([]*jobs.Job, 0, len(r.jobs))
	for _, j := range r.jobs {
		list = append(list, j)
	}
	return list, len(list), nil
}

func (r *MockRepo) UpdateStatus(ctx context.Context, id uuid.UUID, from, to jobs.Status) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok || job.Status != from {
		return jobs.ErrInvalidState
	}
	job.Status = to
	return nil
}

func (r *MockRepo) SetRunning(ctx context.Context, id uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.runErr != nil {
		return r.runErr
	}
	job, ok := r.jobs[id]
	if !ok || (job.Status != jobs.StatusQueued && job.Status != jobs.StatusPending && job.Status != jobs.StatusRetryWait) {
		return jobs.ErrInvalidState
	}
	job.Status = jobs.StatusRunning
	job.AttemptCount++
	return nil
}

func (r *MockRepo) SetCompleted(ctx context.Context, id uuid.UUID, result json.RawMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.compErr != nil {
		return r.compErr
	}
	job, ok := r.jobs[id]
	if !ok || job.Status != jobs.StatusRunning {
		return jobs.ErrInvalidState
	}
	job.Status = jobs.StatusCompleted
	job.Result = result
	return nil
}

func (r *MockRepo) SetFailed(ctx context.Context, id uuid.UUID, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failErr != nil {
		return r.failErr
	}
	job, ok := r.jobs[id]
	if !ok || job.Status != jobs.StatusRunning {
		return jobs.ErrInvalidState
	}
	job.Status = jobs.StatusFailed
	job.Error = &errMsg
	job.LastError = &errMsg
	return nil
}

func (r *MockRepo) SetRetryWait(ctx context.Context, id uuid.UUID, errMsg string, nextAttemptAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retryErr != nil {
		return r.retryErr
	}
	job, ok := r.jobs[id]
	if !ok || job.Status != jobs.StatusRunning {
		return jobs.ErrInvalidState
	}
	job.Status = jobs.StatusRetryWait
	job.LastError = &errMsg
	job.NextAttemptAt = &nextAttemptAt
	return nil
}

func (r *MockRepo) ProcessPendingOutboxEvents(ctx context.Context, limit int, processor jobs.OutboxProcessor) (int, error) {
	return 0, nil
}

func (r *MockRepo) ProcessRetryEligibleJobs(ctx context.Context, limit int, processor jobs.RetryProcessor) (int, error) {
	return 0, nil
}

func (r *MockRepo) RecordAttempt(ctx context.Context, attempt *jobs.JobAttempt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts[attempt.JobID] = append(r.attempts[attempt.JobID], attempt)
	return nil
}

func (r *MockRepo) GetAttemptsByJobID(ctx context.Context, jobID uuid.UUID) ([]*jobs.JobAttempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts[jobID], nil
}

// PanicTaskHandler simulates a handler that panics during execution.
type PanicTaskHandler struct{}

func (h *PanicTaskHandler) Handle(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	panic("unexpected nil pointer dereference in task execution")
}

// FailingTaskHandler simulates a task returning an error.
type FailingTaskHandler struct {
	err error
}

func (h *FailingTaskHandler) Handle(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	return nil, h.err
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestWorker_SuccessfulExecution(t *testing.T) {
	jobID := uuid.New()
	job := &jobs.Job{
		ID:          jobID,
		Type:        "echo",
		Payload:     json.RawMessage(`{"message": "hello"}`),
		Status:      jobs.StatusQueued,
		MaxAttempts: 3,
	}

	repo := NewMockRepo()
	repo.jobs[jobID] = job

	consumer := NewMockConsumer(queue.Message{
		ID:    "100-0",
		JobID: jobID,
		Type:  "echo",
	})

	reg := tasks.NewRegistry()
	reg.Register("echo", &tasks.EchoHandler{})

	w := New(consumer, repo, reg, testLogger()).WithWorkerID("worker-test-1")
	ctx := context.Background()

	err := w.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error running worker: %v", err)
	}

	if job.Status != jobs.StatusCompleted {
		t.Errorf("expected job status COMPLETED, got %s", job.Status)
	}
	if len(consumer.acked) != 1 || consumer.acked[0] != "100-0" {
		t.Errorf("expected message to be acked, got %v", consumer.acked)
	}

	attempts, _ := repo.GetAttemptsByJobID(ctx, jobID)
	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt recorded, got %d", len(attempts))
	}
	if attempts[0].Status != "COMPLETED" || attempts[0].AttemptNumber != 1 || attempts[0].WorkerID == nil || *attempts[0].WorkerID != "worker-test-1" {
		t.Errorf("unexpected attempt record: %+v", attempts[0])
	}
}

func TestWorker_RetryableFailure_TransitionsToRetryWait(t *testing.T) {
	jobID := uuid.New()
	job := &jobs.Job{
		ID:           jobID,
		Type:         "transient",
		Payload:      json.RawMessage(`{}`),
		Status:       jobs.StatusQueued,
		AttemptCount: 0,
		MaxAttempts:  3,
	}

	repo := NewMockRepo()
	repo.jobs[jobID] = job

	consumer := NewMockConsumer(queue.Message{
		ID:    "101-0",
		JobID: jobID,
		Type:  "transient",
	})

	reg := tasks.NewRegistry()
	reg.Register("transient", &FailingTaskHandler{
		err: tasks.NewRetryableError(errors.New("transient network timeout")),
	})

	policy := retry.Policy{
		BaseDelay: 2 * time.Second,
		MaxDelay:  10 * time.Second,
		Jitter:    false,
	}
	w := New(consumer, repo, reg, testLogger()).WithRetryPolicy(policy)
	ctx := context.Background()

	err := w.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error running worker: %v", err)
	}

	if job.Status != jobs.StatusRetryWait {
		t.Errorf("expected job status RETRY_WAIT, got %s", job.Status)
	}
	if job.LastError == nil || *job.LastError != "transient network timeout" {
		t.Errorf("expected last_error recorded, got: %v", job.LastError)
	}
	if job.NextAttemptAt == nil || job.NextAttemptAt.Before(time.Now()) {
		t.Errorf("expected NextAttemptAt in the future, got: %v", job.NextAttemptAt)
	}
	if len(consumer.acked) != 1 || consumer.acked[0] != "101-0" {
		t.Errorf("expected message to be acked, got %v", consumer.acked)
	}

	attempts, _ := repo.GetAttemptsByJobID(ctx, jobID)
	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt recorded, got %d", len(attempts))
	}
	if attempts[0].Status != "FAILED" || attempts[0].AttemptNumber != 1 {
		t.Errorf("unexpected attempt record: %+v", attempts[0])
	}
}

func TestWorker_PermanentFailure_TransitionsToFailed(t *testing.T) {
	jobID := uuid.New()
	job := &jobs.Job{
		ID:           jobID,
		Type:         "permanent",
		Payload:      json.RawMessage(`{}`),
		Status:       jobs.StatusQueued,
		AttemptCount: 0,
		MaxAttempts:  3,
	}

	repo := NewMockRepo()
	repo.jobs[jobID] = job

	consumer := NewMockConsumer(queue.Message{
		ID:    "102-0",
		JobID: jobID,
		Type:  "permanent",
	})

	reg := tasks.NewRegistry()
	reg.Register("permanent", &FailingTaskHandler{
		err: tasks.NewPermanentError(errors.New("invalid payload structure")),
	})

	w := New(consumer, repo, reg, testLogger())
	ctx := context.Background()

	err := w.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error running worker: %v", err)
	}

	if job.Status != jobs.StatusFailed {
		t.Errorf("expected job status FAILED immediately, got %s", job.Status)
	}
	if job.Error == nil || *job.Error != "invalid payload structure" {
		t.Errorf("expected error message recorded, got: %v", job.Error)
	}
	if len(consumer.acked) != 1 || consumer.acked[0] != "102-0" {
		t.Errorf("expected message to be acked, got %v", consumer.acked)
	}
}

func TestWorker_RetryExhaustion_TransitionsToFailed(t *testing.T) {
	jobID := uuid.New()
	job := &jobs.Job{
		ID:           jobID,
		Type:         "always_fail",
		Payload:      json.RawMessage(`{}`),
		Status:       jobs.StatusQueued,
		AttemptCount: 2, // 2 previous attempts
		MaxAttempts:  3,
	}

	repo := NewMockRepo()
	repo.jobs[jobID] = job

	consumer := NewMockConsumer(queue.Message{
		ID:    "103-0",
		JobID: jobID,
		Type:  "always_fail",
	})

	reg := tasks.NewRegistry()
	reg.Register("always_fail", &FailingTaskHandler{
		err: tasks.NewRetryableError(errors.New("flaky error")),
	})

	w := New(consumer, repo, reg, testLogger())
	ctx := context.Background()

	err := w.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error running worker: %v", err)
	}

	// 3rd attempt reached max_attempts (3) -> should transition to FAILED
	if job.Status != jobs.StatusFailed {
		t.Errorf("expected job status FAILED on retry exhaustion, got %s", job.Status)
	}
	if job.AttemptCount != 3 {
		t.Errorf("expected attempt count 3, got %d", job.AttemptCount)
	}
	if len(consumer.acked) != 1 || consumer.acked[0] != "103-0" {
		t.Errorf("expected message to be acked, got %v", consumer.acked)
	}
}

func TestWorker_PanicRecovery(t *testing.T) {
	jobID := uuid.New()
	job := &jobs.Job{
		ID:          jobID,
		Type:        "panic",
		Payload:     json.RawMessage(`{}`),
		Status:      jobs.StatusQueued,
		MaxAttempts: 3,
	}

	repo := NewMockRepo()
	repo.jobs[jobID] = job

	consumer := NewMockConsumer(queue.Message{
		ID:    "104-0",
		JobID: jobID,
		Type:  "panic",
	})

	reg := tasks.NewRegistry()
	reg.Register("panic", &PanicTaskHandler{})

	w := New(consumer, repo, reg, testLogger())
	ctx := context.Background()

	err := w.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error running worker: %v", err)
	}

	if job.Status != jobs.StatusFailed {
		t.Errorf("expected job status FAILED after panic, got %s", job.Status)
	}
	if job.Error == nil || *job.Error != "task execution panicked: unexpected nil pointer dereference in task execution" {
		t.Errorf("expected sanitized panic error recorded, got: %v", job.Error)
	}
	if len(consumer.acked) != 1 || consumer.acked[0] != "104-0" {
		t.Errorf("expected message to be acked after panic, got %v", consumer.acked)
	}
}

func TestWorker_DuplicateDelivery_SafeDeduplication(t *testing.T) {
	jobID := uuid.New()
	job := &jobs.Job{
		ID:      jobID,
		Type:    "echo",
		Payload: json.RawMessage(`{}`),
		Status:  jobs.StatusCompleted, // already completed
	}

	repo := NewMockRepo()
	repo.jobs[jobID] = job

	consumer := NewMockConsumer(queue.Message{
		ID:    "105-0",
		JobID: jobID,
		Type:  "echo",
	})

	reg := tasks.NewRegistry()
	reg.Register("echo", &tasks.EchoHandler{})

	w := New(consumer, repo, reg, testLogger())
	ctx := context.Background()

	err := w.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Job status must remain COMPLETED and not be executed again
	if job.Status != jobs.StatusCompleted {
		t.Errorf("expected job status to remain COMPLETED, got %s", job.Status)
	}
	if len(consumer.acked) != 1 || consumer.acked[0] != "105-0" {
		t.Errorf("expected duplicate message to be acked safely, got %v", consumer.acked)
	}

	attempts, _ := repo.GetAttemptsByJobID(ctx, jobID)
	if len(attempts) != 0 {
		t.Errorf("expected 0 attempts for duplicate delivery, got %d", len(attempts))
	}
}
