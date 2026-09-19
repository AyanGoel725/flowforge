package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/flowforge/flowforge/internal/jobs"
)

// NewRouter constructs the chi router for the FlowForge HTTP API.
func NewRouter(service *jobs.Service, pool *pgxpool.Pool, rdb *redis.Client, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	r := chi.NewRouter()

	// Global Middleware
	r.Use(RequestID)
	r.Use(Logging(logger))
	r.Use(Recovery(logger))

	// Liveness and Readiness
	r.Get("/healthz", handleHealth())
	r.Get("/readyz", handleReady(pool, rdb))

	// Jobs API
	r.Route("/jobs", func(r chi.Router) {
		r.With(EnforceJSON).Post("/", handleCreateJob(service))
		r.Get("/{id}", handleGetJob(service))
		r.Get("/", handleListJobs(service))
	})

	return r
}
