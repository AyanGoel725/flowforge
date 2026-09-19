package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/flowforge/flowforge/internal/jobs"
)

type mockRepo struct {
	jobs     map[uuid.UUID]*jobs.Job
	attempts map[uuid.UUID][]*jobs.JobAttempt
	events   []*jobs.OutboxEvent
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		jobs:     make(map[uuid.UUID]*jobs.Job),
		attempts: make(map[uuid.UUID][]*jobs.JobAttempt),
	}
}

func (m *mockRepo) Create(ctx context.Context, job *jobs.Job) error {
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	m.jobs[job.ID] = job
	return nil
}

func (m *mockRepo) CreateWithOutbox(ctx context.Context, job *jobs.Job, event *jobs.OutboxEvent) error {
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	m.jobs[job.ID] = job
	m.events = append(m.events, event)
	return nil
}

func (m *mockRepo) GetByID(ctx context.Context, id uuid.UUID) (*jobs.Job, error) {
	job, ok := m.jobs[id]
	if !ok {
		return nil, jobs.ErrJobNotFound
	}
	return job, nil
}

func (m *mockRepo) List(ctx context.Context, limit, offset int) ([]*jobs.Job, int, error) {
	var list []*jobs.Job
	for _, j := range m.jobs {
		list = append(list, j)
	}
	return list, len(list), nil
}

func (m *mockRepo) UpdateStatus(ctx context.Context, id uuid.UUID, from, to jobs.Status) error {
	job, ok := m.jobs[id]
	if !ok || job.Status != from {
		return jobs.ErrInvalidState
	}
	job.Status = to
	return nil
}

func (m *mockRepo) SetRunning(ctx context.Context, id uuid.UUID) error {
	return m.UpdateStatus(ctx, id, jobs.StatusQueued, jobs.StatusRunning)
}

func (m *mockRepo) SetCompleted(ctx context.Context, id uuid.UUID, result json.RawMessage) error {
	if err := m.UpdateStatus(ctx, id, jobs.StatusRunning, jobs.StatusCompleted); err != nil {
		return err
	}
	m.jobs[id].Result = result
	return nil
}

func (m *mockRepo) SetFailed(ctx context.Context, id uuid.UUID, errMsg string) error {
	if err := m.UpdateStatus(ctx, id, jobs.StatusRunning, jobs.StatusFailed); err != nil {
		return err
	}
	m.jobs[id].Error = &errMsg
	return nil
}

func (m *mockRepo) SetRetryWait(ctx context.Context, id uuid.UUID, errMsg string, nextAttemptAt time.Time) error {
	if err := m.UpdateStatus(ctx, id, jobs.StatusRunning, jobs.StatusRetryWait); err != nil {
		return err
	}
	m.jobs[id].LastError = &errMsg
	m.jobs[id].NextAttemptAt = &nextAttemptAt
	return nil
}

func (m *mockRepo) ProcessPendingOutboxEvents(ctx context.Context, limit int, processor jobs.OutboxProcessor) (int, error) {
	return 0, nil
}

func (m *mockRepo) ProcessRetryEligibleJobs(ctx context.Context, limit int, processor jobs.RetryProcessor) (int, error) {
	return 0, nil
}

func (m *mockRepo) RecordAttempt(ctx context.Context, attempt *jobs.JobAttempt) error {
	m.attempts[attempt.JobID] = append(m.attempts[attempt.JobID], attempt)
	return nil
}

func (m *mockRepo) GetAttemptsByJobID(ctx context.Context, jobID uuid.UUID) ([]*jobs.JobAttempt, error) {
	return m.attempts[jobID], nil
}

type mockPublisher struct {
	err error
}

func (p *mockPublisher) Publish(ctx context.Context, jobID uuid.UUID, jobType string) error {
	return p.err
}

type mockPinger struct {
	err error
}

func (p *mockPinger) Ping(ctx context.Context) error {
	return p.err
}

func setupTestRouter() (*chiRouterWrapper, *mockRepo, *mockPublisher) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := jobs.NewService(repo, nil)
	r := NewRouter(svc, nil, nil, nil)
	return &chiRouterWrapper{r}, repo, pub
}

type chiRouterWrapper struct {
	http.Handler
}

func TestHandleHealth(t *testing.T) {
	router, _, _ := setupTestRouter()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status 'ok', got '%s'", body["status"])
	}
}

func TestHandleCreateJob(t *testing.T) {
	t.Run("valid job", func(t *testing.T) {
		router, _, _ := setupTestRouter()
		reqBody := []byte(`{"type":"echo","payload":{"message":"hello"}}`)
		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("expected status 201, got %d, body: %s", rec.Code, rec.Body.String())
		}

		var resp createJobResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if resp.Type != "echo" {
			t.Errorf("expected type 'echo', got '%s'", resp.Type)
		}
		if resp.Status != jobs.StatusPending {
			t.Errorf("expected status PENDING, got %s", resp.Status)
		}
	})

	t.Run("empty type", func(t *testing.T) {
		router, _, _ := setupTestRouter()
		reqBody := []byte(`{"type":"","payload":{"message":"hello"}}`)
		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", rec.Code)
		}
	})

	t.Run("invalid json payload", func(t *testing.T) {
		router, _, _ := setupTestRouter()
		reqBody := []byte(`invalid json`)
		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", rec.Code)
		}
	})

	t.Run("unknown fields rejected", func(t *testing.T) {
		router, _, _ := setupTestRouter()
		reqBody := []byte(`{"type":"echo","payload":{},"unexpected":"field"}`)
		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400 for unknown fields, got %d", rec.Code)
		}
	})
}

func TestHandleGetJob(t *testing.T) {
	router, repo, _ := setupTestRouter()
	jobID := uuid.New()
	repo.jobs[jobID] = &jobs.Job{
		ID:     jobID,
		Type:   "echo",
		Status: jobs.StatusCompleted,
	}

	t.Run("existing job", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/jobs/"+jobID.String(), nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rec.Code)
		}
	})

	t.Run("non-existent job", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/jobs/"+uuid.New().String(), nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected status 404, got %d", rec.Code)
		}
	})

	t.Run("invalid uuid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/jobs/not-a-valid-uuid", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", rec.Code)
		}
	})
}

func TestHandleListJobs(t *testing.T) {
	router, repo, _ := setupTestRouter()
	id1 := uuid.New()
	id2 := uuid.New()
	repo.jobs[id1] = &jobs.Job{ID: id1, Type: "echo", Status: jobs.StatusQueued}
	repo.jobs[id2] = &jobs.Job{ID: id2, Type: "sleep", Status: jobs.StatusCompleted}

	t.Run("successful list with query params", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/jobs?limit=10&offset=0", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rec.Code)
		}

		var resp listJobsResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if resp.Total != 2 {
			t.Errorf("expected total 2, got %d", resp.Total)
		}
	})

	t.Run("invalid limit parameter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/jobs?limit=abc", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for invalid limit, got %d", rec.Code)
		}
	})

	t.Run("negative limit parameter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/jobs?limit=-5", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for negative limit, got %d", rec.Code)
		}
	})

	t.Run("invalid offset parameter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/jobs?offset=invalid", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for invalid offset, got %d", rec.Code)
		}
	})
}

func TestHandleReady_Scenarios(t *testing.T) {
	t.Run("nil dependencies", func(t *testing.T) {
		handler := handleReady(nil, nil)
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		rec := httptest.NewRecorder()

		handler(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 for nil dependencies, got %d", rec.Code)
		}
	})

	t.Run("both dependencies healthy", func(t *testing.T) {
		pg := &mockPinger{}
		rdb := &mockPinger{}
		handler := handleReady(pg, rdb)

		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		rec := httptest.NewRecorder()

		handler(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 when healthy, got %d", rec.Code)
		}

		var body map[string]interface{}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if body["status"] != "ok" {
			t.Errorf("expected status 'ok', got %v", body["status"])
		}
	})

	t.Run("postgres unhealthy", func(t *testing.T) {
		pg := &mockPinger{err: errors.New("connection refused")}
		rdb := &mockPinger{}
		handler := handleReady(pg, rdb)

		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		rec := httptest.NewRecorder()

		handler(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 when postgres unhealthy, got %d", rec.Code)
		}
	})

	t.Run("redis unhealthy", func(t *testing.T) {
		pg := &mockPinger{}
		rdb := &mockPinger{err: errors.New("redis timeout")}
		handler := handleReady(pg, rdb)

		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		rec := httptest.NewRecorder()

		handler(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 when redis unhealthy, got %d", rec.Code)
		}
	})
}
