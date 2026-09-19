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
	"github.com/flowforge/flowforge/internal/jobs"
	"github.com/flowforge/flowforge/internal/queue"
	"github.com/flowforge/flowforge/internal/tasks"
	"github.com/flowforge/flowforge/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting FlowForge Worker service")

	// 1. Load config
	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	// 2. Connect to PostgreSQL
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pgPool, err := database.Connect(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()

	// 3. Connect to Redis
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

	// 4. Initialize repository, consumer, task registry
	repo := jobs.NewPostgresRepository(pgPool)
	consumer := queue.NewRedisConsumer(rdb, cfg.RedisStream, cfg.RedisConsumerGroup, "", logger)
	defer consumer.Close()

	registry := tasks.NewRegistry()
	registry.Register("echo", &tasks.EchoHandler{})
	registry.Register("sleep", &tasks.SleepHandler{})
	registry.Register("flaky", tasks.NewFlakyHandler())
	registry.Register("always_fail", &tasks.AlwaysFailHandler{})
	registry.Register("permanent_fail", &tasks.PermanentFailHandler{})
	logger.Info("registered task handlers", "handlers", []string{"echo", "sleep", "flaky", "always_fail", "permanent_fail"})

	// 5. Construct worker
	w := worker.New(consumer, repo, registry, logger)

	// 6. Run worker with signal context
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("received shutdown signal", "signal", sig.String())
		workerCancel()
	}()

	if err := w.Run(workerCtx); err != nil {
		logger.Error("worker terminated with error", "error", err)
		os.Exit(1)
	}

	logger.Info("FlowForge Worker service stopped cleanly")
}
