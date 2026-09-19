package tasks

import (
	"context"
	"encoding/json"
	"testing"
)

func TestPermanentFailHandler_Handle(t *testing.T) {
	handler := &PermanentFailHandler{}

	_, err := handler.Handle(context.Background(), json.RawMessage(`{}`))
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if IsRetryable(err) {
		t.Errorf("expected permanent error, got retryable: %v", err)
	}

	_, errMessage := handler.Handle(context.Background(), json.RawMessage(`{"message": "custom error"}`))
	if errMessage.Error() != "simulated permanent failure: custom error" {
		t.Errorf("expected custom error message, got: %v", errMessage)
	}
}
