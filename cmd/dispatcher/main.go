package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/flowforge/flowforge/internal/config"
	"github.com/flowforge/flowforge/internal/database"
	"github.com/flowforge/flowforge/internal/dispatcher"
	"github.com/flowforge/flowforge/internal/jobs"
	"github.com/flowforge/flowforge/internal/queue"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting FlowForge Outbox Dispatcher")

	// 1. Load config
	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Connect to PostgreSQL
	pgPool, err := database.Connect(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()

	// Ensure migrations run
	if err := database.RunMigrations(cfg.DatabaseURL, logger); err != nil {
		logger.Error("failed to run migrations", "error", err)
		os.Exit(1)
	}

	// 3. Connect to Redis (used for publishing via queue.RedisPublisher)
	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		logger.Error("invalid redis URL", "error", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Error("failed to ping redis", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to Redis")

	// 4. Initialize dependencies
	repo := jobs.NewPostgresRepository(pgPool)
	publisher := queue.NewRedisPublisher(rdb, cfg.RedisStream)

	// Create dispatcher
	disp := dispatcher.New(repo, publisher, logger)

	// 5. Run dispatcher in a goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		opts := dispatcher.Options{
			PollInterval: 500 * time.Millisecond,
			BatchSize:    100,
		}
		if err := disp.Run(ctx, opts); err != nil {
			logger.Error("dispatcher failed", "error", err)
		}
	}()

	// 6. Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	logger.Info("received shutdown signal", "signal", sig)
	cancel()

	// Wait for dispatcher to finish (or timeout)
	select {
	case <-done:
		logger.Info("dispatcher shut down cleanly")
	case <-time.After(5 * time.Second):
		logger.Warn("dispatcher shutdown timed out")
	}
}
