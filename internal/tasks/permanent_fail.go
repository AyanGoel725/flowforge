package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// PermanentFailHandler always returns a non-retryable (permanent) failure.
type PermanentFailHandler struct{}

type permanentFailPayload struct {
	Message string `json:"message,omitempty"`
}

func (h *PermanentFailHandler) Handle(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var p permanentFailPayload
	_ = json.Unmarshal(raw, &p)

	msg := "simulated permanent (non-retryable) failure"
	if p.Message != "" {
		msg = fmt.Sprintf("simulated permanent failure: %s", p.Message)
	}

	return nil, NewPermanentError(errors.New(msg))
}

var _ Handler = (*PermanentFailHandler)(nil)
