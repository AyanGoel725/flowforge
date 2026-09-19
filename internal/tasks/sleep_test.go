package tasks

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestSleepHandler_Handle(t *testing.T) {
	handler := &SleepHandler{}

	t.Run("valid sleep short duration", func(t *testing.T) {
		ctx := context.Background()
		start := time.Now()
		res, err := handler.Handle(ctx, json.RawMessage(`{"seconds": 1}`))
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if elapsed < 900*time.Millisecond {
			t.Errorf("expected sleep to take ~1s, took %v", elapsed)
		}

		var out sleepResult
		if err := json.Unmarshal(res, &out); err != nil {
			t.Fatalf("unmarshaling result: %v", err)
		}
		if out.SleptSeconds != 1 {
			t.Errorf("expected slept_seconds=1, got %d", out.SleptSeconds)
		}
	})

	t.Run("zero seconds invalid", func(t *testing.T) {
		_, err := handler.Handle(context.Background(), json.RawMessage(`{"seconds": 0}`))
		if err == nil {
			t.Fatalf("expected error for 0 seconds")
		}
	})

	t.Run("negative seconds invalid", func(t *testing.T) {
		_, err := handler.Handle(context.Background(), json.RawMessage(`{"seconds": -5}`))
		if err == nil {
			t.Fatalf("expected error for negative seconds")
		}
	})

	t.Run("exceed max 300 seconds invalid", func(t *testing.T) {
		_, err := handler.Handle(context.Background(), json.RawMessage(`{"seconds": 301}`))
		if err == nil {
			t.Fatalf("expected error for seconds > 300")
		}
	})

	t.Run("context cancellation interrupts sleep", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		start := time.Now()
		_, err := handler.Handle(ctx, json.RawMessage(`{"seconds": 10}`))
		elapsed := time.Since(start)

		if err == nil {
			t.Fatalf("expected error on cancelled context")
		}
		if elapsed > 2*time.Second {
			t.Errorf("expected fast cancellation, took %v", elapsed)
		}
	})
}
