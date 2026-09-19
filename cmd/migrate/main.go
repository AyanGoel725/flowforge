package main

import (
	"log/slog"
	"os"

	"github.com/flowforge/flowforge/internal/database"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting FlowForge database migration")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		logger.Error("failed to load configuration", "error", "DATABASE_URL is required")
		os.Exit(1)
	}

	if err := database.RunMigrations(dbURL, logger); err != nil {
		logger.Error("database migration failed", "error", err)
		os.Exit(1)
	}

	logger.Info("database migration completed successfully")
}
