package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// SleepHandler pauses execution for a specified number of seconds (1..300) and respects context cancellation.
type SleepHandler struct{}

type sleepPayload struct {
	Seconds int `json:"seconds"`
}

type sleepResult struct {
	SleptSeconds int `json:"slept_seconds"`
}

func (h *SleepHandler) Handle(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var p sleepPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, errors.New("sleep payload must be a JSON object containing a 'seconds' integer field")
	}

	if p.Seconds <= 0 {
		return nil, errors.New("sleep 'seconds' must be greater than 0")
	}

	if p.Seconds > 300 {
		return nil, errors.New("sleep 'seconds' cannot exceed 300 (5 minutes)")
	}

	timer := time.NewTimer(time.Duration(p.Seconds) * time.Second)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("sleep interrupted: %w", ctx.Err())
	case <-timer.C:
	}

	resultBytes, err := json.Marshal(sleepResult{SleptSeconds: p.Seconds})
	if err != nil {
		return nil, fmt.Errorf("marshaling sleep result: %w", err)
	}

	return resultBytes, nil
}

// Ensure interface compliance
var _ Handler = (*SleepHandler)(nil)
