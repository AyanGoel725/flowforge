package tasks

import (
	"context"
	"encoding/json"
	"testing"
)

func TestAlwaysFailHandler_Handle(t *testing.T) {
	handler := &AlwaysFailHandler{}

	_, err := handler.Handle(context.Background(), json.RawMessage(`{}`))
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if !IsRetryable(err) {
		t.Errorf("expected retryable error, got non-retryable: %v", err)
	}

	_, errMessage := handler.Handle(context.Background(), json.RawMessage(`{"message": "custom error"}`))
	if errMessage.Error() != "simulated transient failure: custom error" {
		t.Errorf("expected custom error message, got: %v", errMessage)
	}
}
