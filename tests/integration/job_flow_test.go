//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/flowforge/flowforge/internal/api"
	"github.com/flowforge/flowforge/internal/config"
	"github.com/flowforge/flowforge/internal/database"
	"github.com/flowforge/flowforge/internal/dispatcher"
	"github.com/flowforge/flowforge/internal/jobs"
	"github.com/flowforge/flowforge/internal/queue"
	"github.com/flowforge/flowforge/internal/retry"
	"github.com/flowforge/flowforge/internal/tasks"
	"github.com/flowforge/flowforge/internal/worker"
)

type testEnv struct {
	cfg       *config.Config
	pgPool    *pgxpool.Pool
	rdb       *redis.Client
	logger    *slog.Logger
	repo      jobs.Repository
	publisher queue.Publisher
	service   *jobs.Service
	router    http.Handler
	registry  *tasks.Registry
}

type panicTaskHandler struct{}

func (h *panicTaskHandler) Handle(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	panic("simulated worker panic in integration test")
}

func setupIntegrationEnv(t *testing.T) *testEnv {
	t.Helper()

	cfg, err := config.Load()
	if err != nil {
		t.Skipf("skipping integration test: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pgPool, err := database.Connect(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		t.Skipf("cannot connect to postgres: %v", err)
	}
	t.Cleanup(func() { pgPool.Close() })

	if err := database.RunMigrations(cfg.DatabaseURL, logger); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		t.Fatalf("invalid redis url: %v", err)
	}
	rdb := redis.NewClient(redisOpts)
	t.Cleanup(func() { rdb.Close() })

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("cannot ping redis: %v", err)
	}

	repo := jobs.NewPostgresRepository(pgPool)
	publisher := queue.NewRedisPublisher(rdb, cfg.RedisStream)
	service := jobs.NewService(repo, logger)
	router := api.NewRouter(service, pgPool, api.NewRedisPinger(rdb), logger)

	registry := tasks.NewRegistry()
	registry.Register("echo", &tasks.EchoHandler{})
	registry.Register("sleep", &tasks.SleepHandler{})
	registry.Register("flaky", tasks.NewFlakyHandler())
	registry.Register("always_fail", &tasks.AlwaysFailHandler{})
	registry.Register("permanent_fail", &tasks.PermanentFailHandler{})
	registry.Register("panic", &panicTaskHandler{})

	return &testEnv{
		cfg:       cfg,
		pgPool:    pgPool,
		rdb:       rdb,
		logger:    logger,
		repo:      repo,
		publisher: publisher,
		service:   service,
		router:    router,
		registry:  registry,
	}
}

// Test 1 — Transaction Atomicity: Verify job and outbox event are created atomically.
func TestJobFlow_TransactionAtomicity(t *testing.T) {
	env := setupIntegrationEnv(t)
	ctx := context.Background()

	jobID := uuid.New()
	job := &jobs.Job{
		ID:          jobID,
		Type:        "echo",
		Payload:     json.RawMessage(`{"message":"atomic-test"}`),
		Status:      jobs.StatusPending,
		MaxAttempts: 3,
	}

	payloadJSON, _ := json.Marshal(&jobs.JobCreatedPayload{
		JobID: jobID,
		Type:  "echo",
	})
	event := &jobs.OutboxEvent{
		ID:          uuid.New(),
		AggregateID: jobID,
		EventType:   "JOB_CREATED",
		Payload:     payloadJSON,
		Status:      jobs.OutboxStatusPending,
	}

	// Create atomically in PostgreSQL
	err := env.repo.CreateWithOutbox(ctx, job, event)
	if err != nil {
		t.Fatalf("failed to create job with outbox: %v", err)
	}

	// Verify both exist
	storedJob, err := env.repo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if storedJob.Status != jobs.StatusPending {
		t.Errorf("expected initial status PENDING, got %s", storedJob.Status)
	}

	var outboxCount int
	err = env.pgPool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND status = 'PENDING'", jobID).Scan(&outboxCount)
	if err != nil {
		t.Fatalf("failed to query outbox_events: %v", err)
	}
	if outboxCount != 1 {
		t.Errorf("expected 1 PENDING outbox event, got %d", outboxCount)
	}
}

// Test 2 — Normal Publication: POST /jobs -> outbox PENDING -> Dispatcher -> Redis -> Worker -> COMPLETED.
func TestJobFlow_NormalPublication(t *testing.T) {
	env := setupIntegrationEnv(t)

	// Start Dispatcher
	d := dispatcher.New(env.repo, env.publisher, env.logger)
	dispCtx, dispCancel := context.WithCancel(context.Background())
	defer dispCancel()
	go func() {
		_ = d.Run(dispCtx, dispatcher.Options{PollInterval: 50 * time.Millisecond, BatchSize: 10})
	}()

	// Start Worker
	consumerGroup := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	consumer := queue.NewRedisConsumer(env.rdb, env.cfg.RedisStream, consumerGroup, "worker-normal", env.logger)
	defer consumer.Close()

	w := worker.New(consumer, env.repo, env.registry, env.logger)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()
	go func() {
		_ = w.Run(workerCtx)
	}()

	// 1. Submit Echo Job via API
	reqBody := `{"type": "echo", "payload": {"message": "integration test message"}}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	wRecorder := httptest.NewRecorder()
	env.router.ServeHTTP(wRecorder, req)

	if wRecorder.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", wRecorder.Code, wRecorder.Body.String())
	}

	var createResp struct {
		ID     uuid.UUID   `json:"id"`
		Type   string      `json:"type"`
		Status jobs.Status `json:"status"`
	}
	if err := json.Unmarshal(wRecorder.Body.Bytes(), &createResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if createResp.Status != jobs.StatusPending {
		t.Errorf("expected initial status PENDING from API, got %s", createResp.Status)
	}

	// 2. Poll for completion
	jobID := createResp.ID
	var completedJob *jobs.Job
	for i := 0; i < 30; i++ {
		time.Sleep(150 * time.Millisecond)

		getReq := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/jobs/%s", jobID.String()), nil)
		getRecorder := httptest.NewRecorder()
		env.router.ServeHTTP(getRecorder, getReq)

		if getRecorder.Code != http.StatusOK {
			t.Fatalf("failed to get job: %d", getRecorder.Code)
		}

		var j jobs.Job
		if err := json.Unmarshal(getRecorder.Body.Bytes(), &j); err != nil {
			t.Fatalf("failed to unmarshal job: %v", err)
		}

		if j.Status == jobs.StatusCompleted {
			completedJob = &j
			break
		}
	}

	if completedJob == nil {
		t.Fatalf("job did not complete within timeout")
	}

	// Verify outbox status transitioned to PUBLISHED
	var outboxStatus string
	err := env.pgPool.QueryRow(context.Background(), "SELECT status FROM outbox_events WHERE aggregate_id = $1", jobID).Scan(&outboxStatus)
	if err != nil {
		t.Fatalf("failed to query outbox event status: %v", err)
	}
	if outboxStatus != "PUBLISHED" {
		t.Errorf("expected outbox event status PUBLISHED, got %s", outboxStatus)
	}

	// Verify attempt history
	attempts, err := env.repo.GetAttemptsByJobID(context.Background(), jobID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("expected 1 attempt record, got %d (err: %v)", len(attempts), err)
	}
	if attempts[0].Status != "COMPLETED" {
		t.Errorf("expected attempt status COMPLETED, got %s", attempts[0].Status)
	}
}

// Test 3 — Redis Outage: Verify job and outbox event remain durable in PostgreSQL during Redis outage,
// and dispatcher successfully publishes it once Redis recovers.
func TestJobFlow_RedisOutage_OutboxDurability(t *testing.T) {
	env := setupIntegrationEnv(t)
	ctx := context.Background()

	// 1. Submit job via API (which writes to PostgreSQL only, no Redis dependency)
	reqBody := `{"type": "echo", "payload": {"message": "redis-outage-message"}}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	wRecorder := httptest.NewRecorder()
	env.router.ServeHTTP(wRecorder, req)

	if wRecorder.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created during Redis outage scenario, got %d: %s", wRecorder.Code, wRecorder.Body.String())
	}

	var createResp struct {
		ID uuid.UUID `json:"id"`
	}
	_ = json.Unmarshal(wRecorder.Body.Bytes(), &createResp)
	jobID := createResp.ID

	// 2. Verify job is PENDING and outbox event is PENDING in PostgreSQL
	storedJob, err := env.repo.GetByID(ctx, jobID)
	if err != nil || storedJob.Status != jobs.StatusPending {
		t.Fatalf("expected job in status PENDING, got %v (err: %v)", storedJob, err)
	}

	// 3. Start Dispatcher and Worker (simulating Redis recovering and queue processing starting)
	d := dispatcher.New(env.repo, env.publisher, env.logger)
	dispCtx, dispCancel := context.WithCancel(context.Background())
	defer dispCancel()
	go func() {
		_ = d.Run(dispCtx, dispatcher.Options{PollInterval: 50 * time.Millisecond, BatchSize: 10})
	}()

	consumerGroup := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	consumer := queue.NewRedisConsumer(env.rdb, env.cfg.RedisStream, consumerGroup, "worker-outage-test", env.logger)
	defer consumer.Close()

	w := worker.New(consumer, env.repo, env.registry, env.logger)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()
	go func() {
		_ = w.Run(workerCtx)
	}()

	// 4. Verify job reaches COMPLETED
	var completedJob *jobs.Job
	for i := 0; i < 30; i++ {
		time.Sleep(150 * time.Millisecond)
		j, err := env.repo.GetByID(ctx, jobID)
		if err == nil && j.Status == jobs.StatusCompleted {
			completedJob = j
			break
		}
	}

	if completedJob == nil {
		t.Fatalf("expected job to complete after Redis recovery")
	}
}

// Test 4 — Duplicate Delivery Deduplication: A duplicate Redis message for an already-completed
// job must be acknowledged and discarded without re-executing.
func TestJobFlow_DuplicateDelivery_SafeDeduplication(t *testing.T) {
	env := setupIntegrationEnv(t)
	ctx := context.Background()

	jobID := uuid.New()
	job := &jobs.Job{
		ID:          jobID,
		Type:        "echo",
		Payload:     json.RawMessage(`{"message":"dedup-test"}`),
		Status:      jobs.StatusCompleted,
		MaxAttempts: 3,
	}

	// Directly insert completed job into PostgreSQL
	_ = env.repo.Create(ctx, job)
	_ = env.repo.SetRunning(ctx, jobID)
	_ = env.repo.SetCompleted(ctx, jobID, json.RawMessage(`{"message":"dedup-test"}`))

	// Publish duplicate delivery to Redis
	if err := env.publisher.Publish(ctx, jobID, "echo"); err != nil {
		t.Fatalf("failed to publish duplicate delivery: %v", err)
	}

	consumerGroup := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	consumer := queue.NewRedisConsumer(env.rdb, env.cfg.RedisStream, consumerGroup, "worker-dedup", env.logger)
	defer consumer.Close()

	w := worker.New(consumer, env.repo, env.registry, env.logger)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()
	go func() {
		_ = w.Run(workerCtx)
	}()

	// Allow worker time to receive and discard duplicate
	time.Sleep(600 * time.Millisecond)

	// Status must still be COMPLETED and attempt count must remain unchanged
	j, err := env.repo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if j.Status != jobs.StatusCompleted {
		t.Errorf("expected job status COMPLETED, got %s", j.Status)
	}

	attempts, err := env.repo.GetAttemptsByJobID(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get attempts: %v", err)
	}
	if len(attempts) != 0 {
		t.Errorf("expected 0 attempts from duplicate delivery on already-completed job, got %d", len(attempts))
	}
}

// Test 5 — Retryable Failure: Flaky task retries twice and succeeds on attempt 3.
func TestJobFlow_RetryableFailure_Flaky(t *testing.T) {
	env := setupIntegrationEnv(t)
	ctx := context.Background()

	// Configure aggressive fast retry policy for integration testing
	fastPolicy := retry.Policy{
		BaseDelay: 200 * time.Millisecond,
		MaxDelay:  500 * time.Millisecond,
		Jitter:    false,
	}

	// Start Dispatcher
	d := dispatcher.New(env.repo, env.publisher, env.logger)
	dispCtx, dispCancel := context.WithCancel(context.Background())
	defer dispCancel()
	go func() {
		_ = d.Run(dispCtx, dispatcher.Options{PollInterval: 50 * time.Millisecond, BatchSize: 10})
	}()

	// Start Worker
	consumerGroup := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	consumer := queue.NewRedisConsumer(env.rdb, env.cfg.RedisStream, consumerGroup, "worker-flaky", env.logger)
	defer consumer.Close()

	w := worker.New(consumer, env.repo, env.registry, env.logger).WithRetryPolicy(fastPolicy)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()
	go func() {
		_ = w.Run(workerCtx)
	}()

	// Submit flaky job requiring 2 failures before success
	reqBody := `{"type": "flaky", "payload": {"failures_before_success": 2}}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	wRecorder := httptest.NewRecorder()
	env.router.ServeHTTP(wRecorder, req)

	if wRecorder.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", wRecorder.Code, wRecorder.Body.String())
	}

	var createResp struct {
		ID uuid.UUID `json:"id"`
	}
	_ = json.Unmarshal(wRecorder.Body.Bytes(), &createResp)
	jobID := createResp.ID

	// Poll until completed (attempt 1 fail -> attempt 2 fail -> attempt 3 success)
	var completedJob *jobs.Job
	for i := 0; i < 50; i++ {
		time.Sleep(200 * time.Millisecond)
		j, err := env.repo.GetByID(ctx, jobID)
		if err == nil && j.Status == jobs.StatusCompleted {
			completedJob = j
			break
		}
	}

	if completedJob == nil {
		t.Fatalf("flaky job did not complete within timeout")
	}

	if completedJob.AttemptCount != 3 {
		t.Errorf("expected attempt_count 3, got %d", completedJob.AttemptCount)
	}

	// Verify attempt history in job_attempts
	attempts, err := env.repo.GetAttemptsByJobID(ctx, jobID)
	if err != nil || len(attempts) != 3 {
		t.Fatalf("expected 3 attempt records, got %d (err: %v)", len(attempts), err)
	}

	if attempts[0].Status != "FAILED" || attempts[0].AttemptNumber != 1 {
		t.Errorf("expected attempt 1 to be FAILED, got %+v", attempts[0])
	}
	if attempts[1].Status != "FAILED" || attempts[1].AttemptNumber != 2 {
		t.Errorf("expected attempt 2 to be FAILED, got %+v", attempts[1])
	}
	if attempts[2].Status != "COMPLETED" || attempts[2].AttemptNumber != 3 {
		t.Errorf("expected attempt 3 to be COMPLETED, got %+v", attempts[2])
	}
}

// Test 6 — Retry Exhaustion: Always-failing task exhausts retries and transitions to FAILED.
func TestJobFlow_RetryExhaustion_AlwaysFail(t *testing.T) {
	env := setupIntegrationEnv(t)
	ctx := context.Background()

	fastPolicy := retry.Policy{
		BaseDelay: 100 * time.Millisecond,
		MaxDelay:  300 * time.Millisecond,
		Jitter:    false,
	}

	d := dispatcher.New(env.repo, env.publisher, env.logger)
	dispCtx, dispCancel := context.WithCancel(context.Background())
	defer dispCancel()
	go func() {
		_ = d.Run(dispCtx, dispatcher.Options{PollInterval: 50 * time.Millisecond, BatchSize: 10})
	}()

	consumerGroup := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	consumer := queue.NewRedisConsumer(env.rdb, env.cfg.RedisStream, consumerGroup, "worker-always-fail", env.logger)
	defer consumer.Close()

	w := worker.New(consumer, env.repo, env.registry, env.logger).WithRetryPolicy(fastPolicy)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()
	go func() {
		_ = w.Run(workerCtx)
	}()

	// Submit always_fail job (max_attempts = 3)
	reqBody := `{"type": "always_fail", "payload": {"message": "exhaustion test"}}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	wRecorder := httptest.NewRecorder()
	env.router.ServeHTTP(wRecorder, req)

	if wRecorder.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", wRecorder.Code)
	}

	var createResp struct {
		ID uuid.UUID `json:"id"`
	}
	_ = json.Unmarshal(wRecorder.Body.Bytes(), &createResp)
	jobID := createResp.ID

	// Poll until terminal FAILED status
	var failedJob *jobs.Job
	for i := 0; i < 50; i++ {
		time.Sleep(200 * time.Millisecond)
		j, err := env.repo.GetByID(ctx, jobID)
		if err == nil && j.Status == jobs.StatusFailed {
			failedJob = j
			break
		}
	}

	if failedJob == nil {
		t.Fatalf("always_fail job did not transition to FAILED within timeout")
	}

	if failedJob.AttemptCount != 3 {
		t.Errorf("expected attempt_count 3 on retry exhaustion, got %d", failedJob.AttemptCount)
	}

	// Verify attempt history has 3 failed attempts
	attempts, err := env.repo.GetAttemptsByJobID(ctx, jobID)
	if err != nil || len(attempts) != 3 {
		t.Fatalf("expected 3 attempt records, got %d (err: %v)", len(attempts), err)
	}
	for i, att := range attempts {
		if att.Status != "FAILED" || att.AttemptNumber != i+1 {
			t.Errorf("expected attempt %d to be FAILED, got %+v", i+1, att)
		}
	}
}

// Test 7 — Permanent Failure: Permanent error transitions immediately to FAILED without retry.
func TestJobFlow_PermanentFailure(t *testing.T) {
	env := setupIntegrationEnv(t)
	ctx := context.Background()

	d := dispatcher.New(env.repo, env.publisher, env.logger)
	dispCtx, dispCancel := context.WithCancel(context.Background())
	defer dispCancel()
	go func() {
		_ = d.Run(dispCtx, dispatcher.Options{PollInterval: 50 * time.Millisecond, BatchSize: 10})
	}()

	consumerGroup := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	consumer := queue.NewRedisConsumer(env.rdb, env.cfg.RedisStream, consumerGroup, "worker-permanent-fail", env.logger)
	defer consumer.Close()

	w := worker.New(consumer, env.repo, env.registry, env.logger)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()
	go func() {
		_ = w.Run(workerCtx)
	}()

	reqBody := `{"type": "permanent_fail", "payload": {"message": "unrecoverable schema mismatch"}}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	wRecorder := httptest.NewRecorder()
	env.router.ServeHTTP(wRecorder, req)

	if wRecorder.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", wRecorder.Code)
	}

	var createResp struct {
		ID uuid.UUID `json:"id"`
	}
	_ = json.Unmarshal(wRecorder.Body.Bytes(), &createResp)
	jobID := createResp.ID

	// Poll for FAILED status
	var failedJob *jobs.Job
	for i := 0; i < 30; i++ {
		time.Sleep(150 * time.Millisecond)
		j, err := env.repo.GetByID(ctx, jobID)
		if err == nil && j.Status == jobs.StatusFailed {
			failedJob = j
			break
		}
	}

	if failedJob == nil {
		t.Fatalf("permanent failure job did not reach FAILED")
	}

	if failedJob.AttemptCount != 1 {
		t.Errorf("expected 1 attempt count for permanent failure, got %d", failedJob.AttemptCount)
	}

	attempts, err := env.repo.GetAttemptsByJobID(ctx, jobID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("expected exactly 1 attempt record for permanent failure, got %d", len(attempts))
	}
}

// Test 8 — Backoff Timing: Verifies retry attempts do not occur immediately.
func TestJobFlow_BackoffTiming(t *testing.T) {
	env := setupIntegrationEnv(t)
	ctx := context.Background()

	// Set backoff delay of 2 seconds
	backoffPolicy := retry.Policy{
		BaseDelay: 2 * time.Second,
		MaxDelay:  5 * time.Second,
		Jitter:    false,
	}

	consumerGroup := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	consumer := queue.NewRedisConsumer(env.rdb, env.cfg.RedisStream, consumerGroup, "worker-backoff", env.logger)
	defer consumer.Close()

	w := worker.New(consumer, env.repo, env.registry, env.logger).WithRetryPolicy(backoffPolicy)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()
	go func() {
		_ = w.Run(workerCtx)
	}()

	jobID := uuid.New()
	job := &jobs.Job{
		ID:          jobID,
		Type:        "always_fail",
		Payload:     json.RawMessage(`{}`),
		Status:      jobs.StatusQueued,
		MaxAttempts: 3,
	}
	_ = env.repo.Create(ctx, job)
	_ = env.publisher.Publish(ctx, jobID, "always_fail")

	// Wait for attempt 1 to complete and transition into RETRY_WAIT
	var retryWaitJob *jobs.Job
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		j, err := env.repo.GetByID(ctx, jobID)
		if err == nil && j.Status == jobs.StatusRetryWait {
			retryWaitJob = j
			break
		}
	}

	if retryWaitJob == nil {
		t.Fatalf("expected job to be in RETRY_WAIT after attempt 1")
	}

	if retryWaitJob.NextAttemptAt == nil {
		t.Fatalf("expected next_attempt_at to be set in RETRY_WAIT")
	}

	// Verify next_attempt_at is in the future (~2 seconds from now)
	remaining := time.Until(*retryWaitJob.NextAttemptAt)
	if remaining < 1*time.Second {
		t.Errorf("expected backoff delay remaining > 1s, got %v", remaining)
	}
}

func TestJobFlow_Sleep_Cancellation(t *testing.T) {
	env := setupIntegrationEnv(t)
	ctx := context.Background()

	consumerGroup := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	consumer := queue.NewRedisConsumer(env.rdb, env.cfg.RedisStream, consumerGroup, "worker-sleep", env.logger)
	defer consumer.Close()

	w := worker.New(consumer, env.repo, env.registry, env.logger)

	workerCtx, workerCancel := context.WithCancel(context.Background())

	go func() {
		_ = w.Run(workerCtx)
	}()

	jobID := uuid.New()
	job := &jobs.Job{
		ID:          jobID,
		Type:        "sleep",
		Payload:     json.RawMessage(`{"seconds": 10}`),
		Status:      jobs.StatusQueued,
		MaxAttempts: 1,
	}
	_ = env.repo.Create(ctx, job)
	_ = env.publisher.Publish(ctx, jobID, "sleep")

	// Wait briefly until running
	time.Sleep(300 * time.Millisecond)

	// Cancel worker context
	workerCancel()

	// Poll for FAILED status
	var failedJob *jobs.Job
	for i := 0; i < 25; i++ {
		time.Sleep(200 * time.Millisecond)
		j, err := env.repo.GetByID(ctx, jobID)
		if err == nil && j.Status == jobs.StatusFailed {
			failedJob = j
			break
		}
	}

	if failedJob == nil {
		t.Fatalf("expected job to be marked FAILED after worker context cancellation")
	}
}

func TestJobFlow_PanicRecovery(t *testing.T) {
	env := setupIntegrationEnv(t)

	consumerGroup := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	consumer := queue.NewRedisConsumer(env.rdb, env.cfg.RedisStream, consumerGroup, "worker-panic", env.logger)
	defer consumer.Close()

	w := worker.New(consumer, env.repo, env.registry, env.logger)

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	go func() {
		_ = w.Run(workerCtx)
	}()

	jobID := uuid.New()
	job := &jobs.Job{
		ID:          jobID,
		Type:        "panic",
		Payload:     json.RawMessage(`{}`),
		Status:      jobs.StatusQueued,
		MaxAttempts: 3,
	}
	if err := env.repo.Create(context.Background(), job); err != nil {
		t.Fatalf("failed to create panic job: %v", err)
	}
	if err := env.publisher.Publish(context.Background(), jobID, "panic"); err != nil {
		t.Fatalf("failed to publish panic job: %v", err)
	}

	var failedJob *jobs.Job
	for i := 0; i < 25; i++ {
		time.Sleep(200 * time.Millisecond)

		j, err := env.repo.GetByID(context.Background(), jobID)
		if err == nil && j.Status == jobs.StatusFailed {
			failedJob = j
			break
		}
	}

	if failedJob == nil {
		t.Fatalf("expected panic job to transition to FAILED")
	}
	if failedJob.Error == nil || *failedJob.Error == "" {
		t.Errorf("expected error message recorded on panic job")
	}
}

func TestJobFlow_UnsupportedTaskType(t *testing.T) {
	env := setupIntegrationEnv(t)

	consumerGroup := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	consumer := queue.NewRedisConsumer(env.rdb, env.cfg.RedisStream, consumerGroup, "worker-unsupported", env.logger)
	defer consumer.Close()

	w := worker.New(consumer, env.repo, env.registry, env.logger)

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	go func() {
		_ = w.Run(workerCtx)
	}()

	jobID := uuid.New()
	job := &jobs.Job{
		ID:          jobID,
		Type:        "unknown_type",
		Payload:     json.RawMessage(`{}`),
		Status:      jobs.StatusQueued,
		MaxAttempts: 3,
	}
	if err := env.repo.Create(context.Background(), job); err != nil {
		t.Fatalf("failed to create job: %v", err)
	}
	if err := env.publisher.Publish(context.Background(), jobID, "unknown_type"); err != nil {
		t.Fatalf("failed to publish job: %v", err)
	}

	var failedJob *jobs.Job
	for i := 0; i < 25; i++ {
		time.Sleep(200 * time.Millisecond)

		j, err := env.repo.GetByID(context.Background(), jobID)
		if err == nil && j.Status == jobs.StatusFailed {
			failedJob = j
			break
		}
	}

	if failedJob == nil {
		t.Fatalf("expected unsupported job to transition to FAILED")
	}
}

func TestJobFlow_ConcurrentJobs(t *testing.T) {
	env := setupIntegrationEnv(t)

	d := dispatcher.New(env.repo, env.publisher, env.logger)
	dispCtx, dispCancel := context.WithCancel(context.Background())
	defer dispCancel()
	go func() {
		_ = d.Run(dispCtx, dispatcher.Options{PollInterval: 50 * time.Millisecond, BatchSize: 10})
	}()

	consumerGroup := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	consumer := queue.NewRedisConsumer(env.rdb, env.cfg.RedisStream, consumerGroup, "worker-concurrent", env.logger)
	defer consumer.Close()

	w := worker.New(consumer, env.repo, env.registry, env.logger)

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	go func() {
		_ = w.Run(workerCtx)
	}()

	const numJobs = 5
	var jobIDs []uuid.UUID
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < numJobs; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			reqBody := fmt.Sprintf(`{"type": "echo", "payload": {"message": "msg-%d"}}`, idx)
			req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(reqBody))
			req.Header.Set("Content-Type", "application/json")
			wRecorder := httptest.NewRecorder()
			env.router.ServeHTTP(wRecorder, req)

			if wRecorder.Code != http.StatusCreated {
				t.Errorf("failed to create job %d: %d", idx, wRecorder.Code)
				return
			}

			var resp struct {
				ID uuid.UUID `json:"id"`
			}
			_ = json.Unmarshal(wRecorder.Body.Bytes(), &resp)

			mu.Lock()
			jobIDs = append(jobIDs, resp.ID)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if len(jobIDs) != numJobs {
		t.Fatalf("expected %d jobs created, got %d", numJobs, len(jobIDs))
	}

	// Wait for all jobs to complete
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		allCompleted := true
		for _, id := range jobIDs {
			j, err := env.repo.GetByID(context.Background(), id)
			if err != nil || j.Status != jobs.StatusCompleted {
				allCompleted = false
				break
			}
		}
		if allCompleted {
			return // Success
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("not all concurrent jobs completed in time")
}
