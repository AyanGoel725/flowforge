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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/flowforge/flowforge/internal/api"
	"github.com/flowforge/flowforge/internal/config"
	"github.com/flowforge/flowforge/internal/database"
	"github.com/flowforge/flowforge/internal/jobs"
	"github.com/flowforge/flowforge/internal/queue"
	"github.com/flowforge/flowforge/internal/tasks"
	"github.com/flowforge/flowforge/internal/worker"
)

func TestJobFlow_Echo_EndToEnd(t *testing.T) {
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
	defer pgPool.Close()

	if err := database.RunMigrations(cfg.DatabaseURL, logger); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		t.Fatalf("invalid redis url: %v", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("cannot ping redis: %v", err)
	}

	// 1. Setup API & Worker
	repo := jobs.NewPostgresRepository(pgPool)
	publisher := queue.NewRedisPublisher(rdb, cfg.RedisStream)
	service := jobs.NewService(repo, publisher, logger)
	router := api.NewRouter(service, pgPool, rdb, logger)

	consumer := queue.NewRedisConsumer(rdb, cfg.RedisStream, cfg.RedisConsumerGroup, "test-worker", logger)
	defer consumer.Close()

	registry := tasks.NewRegistry()
	registry.Register("echo", &tasks.EchoHandler{})
	registry.Register("sleep", &tasks.SleepHandler{})

	w := worker.New(consumer, repo, registry, logger)

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	go func() {
		_ = w.Run(workerCtx)
	}()

	// 2. Submit Echo Job via API
	reqBody := `{"type": "echo", "payload": {"message": "integration test message"}}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	wRecorder := httptest.NewRecorder()
	router.ServeHTTP(wRecorder, req)

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

	if createResp.Status != jobs.StatusQueued {
		t.Errorf("expected initial status QUEUED, got %s", createResp.Status)
	}

	// 3. Poll for completion
	jobID := createResp.ID
	var completedJob *jobs.Job
	for i := 0; i < 20; i++ {
		time.Sleep(200 * time.Millisecond)

		getReq := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/jobs/%s", jobID.String()), nil)
		getRecorder := httptest.NewRecorder()
		router.ServeHTTP(getRecorder, getReq)

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

	if completedJob.StartedAt == nil || completedJob.CompletedAt == nil {
		t.Errorf("expected non-nil started_at and completed_at timestamps")
	}

	var resPayload struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(completedJob.Result, &resPayload); err != nil {
		t.Fatalf("invalid result json: %v", err)
	}
	if resPayload.Message != "integration test message" {
		t.Errorf("expected result message %q, got %q", "integration test message", resPayload.Message)
	}
}
