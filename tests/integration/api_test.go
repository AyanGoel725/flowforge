//go:build integration

package integration

import (
	"bytes"
	"context"
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
)

func setupTestApp(t *testing.T) (http.Handler, *jobs.Service, *config.Config) {
	t.Helper()

	cfg, err := config.Load()
	if err != nil {
		t.Skipf("skipping integration test: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	service := jobs.NewService(repo, publisher, logger)

	router := api.NewRouter(service, pgPool, rdb, logger)
	return router, service, cfg
}

func TestAPI_HealthAndReady(t *testing.T) {
	router, _, _ := setupTestApp(t)

	// Test /healthz
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	// Test /readyz
	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

func TestAPI_InvalidInputs(t *testing.T) {
	router, _, _ := setupTestApp(t)

	// 1. Invalid JSON body
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString("{invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad JSON, got %d", w.Code)
	}

	// 2. Empty job type
	req = httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(`{"type": "", "payload": {}}`))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty type, got %d", w.Code)
	}

	// 3. Invalid UUID on GET
	req = httptest.NewRequest(http.MethodGet, "/jobs/not-a-valid-uuid", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid UUID, got %d", w.Code)
	}

	// 4. Non-existent UUID
	randomID := uuid.New().String()
	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/jobs/%s", randomID), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for missing job, got %d", w.Code)
	}
}
