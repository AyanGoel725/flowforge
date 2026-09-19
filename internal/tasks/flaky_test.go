package tasks

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestFlakyHandler_Handle(t *testing.T) {
	handler := NewFlakyHandler()
	jobID := uuid.New()

	// Attempt 1 -> should fail with RetryableError
	ctx1 := WithJobContext(context.Background(), jobID, 1)
	_, err1 := handler.Handle(ctx1, json.RawMessage(`{}`))
	if err1 == nil {
		t.Fatalf("expected error on attempt 1, got nil")
	}
	if !IsRetryable(err1) {
		t.Fatalf("expected retryable error on attempt 1, got non-retryable: %v", err1)
	}

	// Attempt 2 -> should fail with RetryableError
	ctx2 := WithJobContext(context.Background(), jobID, 2)
	_, err2 := handler.Handle(ctx2, json.RawMessage(`{}`))
	if err2 == nil {
		t.Fatalf("expected error on attempt 2, got nil")
	}
	if !IsRetryable(err2) {
		t.Fatalf("expected retryable error on attempt 2, got non-retryable: %v", err2)
	}

	// Attempt 3 -> should succeed
	ctx3 := WithJobContext(context.Background(), jobID, 3)
	res, err3 := handler.Handle(ctx3, json.RawMessage(`{}`))
	if err3 != nil {
		t.Fatalf("expected success on attempt 3, got error: %v", err3)
	}
	var resMap map[string]interface{}
	if err := json.Unmarshal(res, &resMap); err != nil {
		t.Fatalf("failed to unmarshal success result: %v", err)
	}
	if resMap["status"] != "success" {
		t.Errorf("expected status 'success', got %v", resMap["status"])
	}
}
