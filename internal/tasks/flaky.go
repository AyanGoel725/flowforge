package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// FlakyHandler simulates transient failures before eventually succeeding.
// Input JSON: {"failures_before_success": 2}
type FlakyHandler struct {
	mu     sync.Mutex
	counts map[string]int
}

// NewFlakyHandler creates a new FlakyHandler.
func NewFlakyHandler() *FlakyHandler {
	return &FlakyHandler{
		counts: make(map[string]int),
	}
}

type flakyPayload struct {
	FailuresBeforeSuccess int `json:"failures_before_success"`
}

func (h *FlakyHandler) Handle(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var p flakyPayload
	if len(raw) > 0 && string(raw) != "{}" && string(raw) != "null" {
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, NewPermanentError(fmt.Errorf("invalid flaky payload: %w", err))
		}
	}
	if p.FailuresBeforeSuccess <= 0 {
		p.FailuresBeforeSuccess = 2
	}

	attempt := GetAttemptNumber(ctx)
	jobID := GetJobID(ctx)

	// Fallback to in-memory counter if attempt not present in context
	if attempt <= 1 && jobID.String() != "00000000-0000-0000-0000-000000000000" {
		h.mu.Lock()
		h.counts[jobID.String()]++
		attempt = h.counts[jobID.String()]
		h.mu.Unlock()
	}

	if attempt <= p.FailuresBeforeSuccess {
		return nil, NewRetryableError(fmt.Errorf("transient failure on attempt %d (will succeed after %d failures)", attempt, p.FailuresBeforeSuccess))
	}

	result, _ := json.Marshal(map[string]interface{}{
		"status":                 "success",
		"successful_attempt":     attempt,
		"failures_encountered":  p.FailuresBeforeSuccess,
	})
	return result, nil
}

var _ Handler = (*FlakyHandler)(nil)
