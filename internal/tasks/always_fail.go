package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// AlwaysFailHandler always returns a retryable (transient) failure.
type AlwaysFailHandler struct{}

type alwaysFailPayload struct {
	Message string `json:"message,omitempty"`
}

func (h *AlwaysFailHandler) Handle(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var p alwaysFailPayload
	_ = json.Unmarshal(raw, &p)

	msg := "simulated transient failure from always_fail handler"
	if p.Message != "" {
		msg = fmt.Sprintf("simulated transient failure: %s", p.Message)
	}

	return nil, NewRetryableError(errors.New(msg))
}

var _ Handler = (*AlwaysFailHandler)(nil)
