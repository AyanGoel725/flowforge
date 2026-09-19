package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

// EchoHandler simply returns its input payload as the result.
type EchoHandler struct{}

type echoPayload struct {
	Message string `json:"message"`
}

func (h *EchoHandler) Handle(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var p echoPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, errors.New("echo payload must be a JSON object containing a 'message' string field")
	}

	if strings.TrimSpace(p.Message) == "" {
		return nil, errors.New("echo payload 'message' field cannot be empty")
	}

	// Echo back the same payload as result
	return raw, nil
}

// Ensure interface compliance
var _ Handler = (*EchoHandler)(nil)
