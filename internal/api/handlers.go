package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/flowforge/flowforge/internal/jobs"
)

type createJobResponse struct {
	ID        uuid.UUID   `json:"id"`
	Type      string      `json:"type"`
	Status    jobs.Status `json:"status"`
	CreatedAt time.Time   `json:"created_at"`
}

type listJobsResponse struct {
	Jobs   []*jobs.Job `json:"jobs"`
	Total  int         `json:"total"`
	Limit  int         `json:"limit"`
	Offset int         `json:"offset"`
}

type readyResponse struct {
	Status   string `json:"status"`
	Postgres string `json:"postgres"`
	Redis    string `json:"redis"`
}

// handleCreateJob submits a new background job.
func handleCreateJob(svc *jobs.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, jobs.MaxPayloadSizeBytes)

		var req jobs.CreateJobRequest
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				writeError(w, http.StatusBadRequest, jobs.ErrPayloadTooLarge.Error(), "PAYLOAD_TOO_LARGE")
				return
			}
			writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), "INVALID_REQUEST_BODY")
			return
		}

		job, err := svc.CreateJob(r.Context(), &req)
		if err != nil {
			switch {
			case errors.Is(err, jobs.ErrEmptyType):
				writeError(w, http.StatusBadRequest, err.Error(), "EMPTY_TYPE")
			case errors.Is(err, jobs.ErrUnsupportedType):
				writeError(w, http.StatusBadRequest, err.Error(), "UNSUPPORTED_TYPE")
			case errors.Is(err, jobs.ErrInvalidPayload):
				writeError(w, http.StatusBadRequest, err.Error(), "INVALID_PAYLOAD")
			case errors.Is(err, jobs.ErrPayloadTooLarge):
				writeError(w, http.StatusBadRequest, err.Error(), "PAYLOAD_TOO_LARGE")
			default:
				writeError(w, http.StatusInternalServerError, "failed to create job", "INTERNAL_ERROR")
			}
			return
		}

		w.Header().Set("Location", fmt.Sprintf("/jobs/%s", job.ID.String()))
		writeJSON(w, http.StatusCreated, createJobResponse{
			ID:        job.ID,
			Type:      job.Type,
			Status:    job.Status,
			CreatedAt: job.CreatedAt,
		})
	}
}

// handleGetJob fetches a job by UUID.
func handleGetJob(svc *jobs.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := chi.URLParam(r, "id")
		id, err := uuid.Parse(idStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid UUID format", "INVALID_ID")
			return
		}

		job, err := svc.GetJob(r.Context(), id)
		if err != nil {
			if errors.Is(err, jobs.ErrJobNotFound) {
				writeError(w, http.StatusNotFound, "job not found", "NOT_FOUND")
				return
			}
			writeError(w, http.StatusInternalServerError, "failed to get job", "INTERNAL_ERROR")
			return
		}

		writeJSON(w, http.StatusOK, job)
	}
}

// handleListJobs returns a paginated list of jobs.
func handleListJobs(svc *jobs.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := 20
		offset := 0

		if lStr := r.URL.Query().Get("limit"); lStr != "" {
			l, err := strconv.Atoi(lStr)
			if err != nil || l <= 0 {
				writeError(w, http.StatusBadRequest, "invalid limit parameter", "INVALID_LIMIT")
				return
			}
			if l > 100 {
				limit = 100
			} else {
				limit = l
			}
		}

		if oStr := r.URL.Query().Get("offset"); oStr != "" {
			o, err := strconv.Atoi(oStr)
			if err != nil || o < 0 {
				writeError(w, http.StatusBadRequest, "invalid offset parameter", "INVALID_OFFSET")
				return
			}
			offset = o
		}

		jobList, total, err := svc.ListJobs(r.Context(), limit, offset)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list jobs", "INTERNAL_ERROR")
			return
		}

		if jobList == nil {
			jobList = []*jobs.Job{}
		}

		writeJSON(w, http.StatusOK, listJobsResponse{
			Jobs:   jobList,
			Total:  total,
			Limit:  limit,
			Offset: offset,
		})
	}
}

// handleHealth returns basic liveness check.
func handleHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// handleReady returns readiness check for dependencies.
func handleReady(pg Pinger, rdb Pinger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		pgStatus := "connected"
		if pg != nil {
			if err := pg.Ping(ctx); err != nil {
				pgStatus = "disconnected"
			}
		} else {
			pgStatus = "disconnected"
		}

		redisStatus := "connected"
		if rdb != nil {
			if err := rdb.Ping(ctx); err != nil {
				redisStatus = "disconnected"
			}
		} else {
			redisStatus = "disconnected"
		}

		allOK := pgStatus == "connected" && redisStatus == "connected"
		status := "ok"
		statusCode := http.StatusOK
		if !allOK {
			status = "unavailable"
			statusCode = http.StatusServiceUnavailable
		}

		writeJSON(w, statusCode, readyResponse{
			Status:   status,
			Postgres: pgStatus,
			Redis:    redisStatus,
		})
	}
}
