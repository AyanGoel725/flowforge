package retry

import (
	"testing"
	"time"
)

func TestBackoff_Calculations(t *testing.T) {
	policy := Policy{
		BaseDelay: 1 * time.Second,
		MaxDelay:  8 * time.Second,
		Jitter:    false,
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{attempt: 1, expected: 1 * time.Second}, // 1 * 2^0 = 1s
		{attempt: 2, expected: 2 * time.Second}, // 1 * 2^1 = 2s
		{attempt: 3, expected: 4 * time.Second}, // 1 * 2^2 = 4s
		{attempt: 4, expected: 8 * time.Second}, // 1 * 2^3 = 8s
		{attempt: 5, expected: 8 * time.Second}, // capped at max 8s
	}

	for _, tt := range tests {
		delay := policy.NextDelay(tt.attempt)
		if delay != tt.expected {
			t.Errorf("attempt %d: expected delay %v, got %v", tt.attempt, tt.expected, delay)
		}
	}
}

func TestBackoff_WithJitter(t *testing.T) {
	policy := Policy{
		BaseDelay: 1 * time.Second,
		MaxDelay:  10 * time.Second,
		Jitter:    true,
	}

	for attempt := 1; attempt <= 4; attempt++ {
		delay := policy.NextDelay(attempt)
		maxPossible := policy.BaseDelay * (1 << (attempt - 1))
		if maxPossible > policy.MaxDelay {
			maxPossible = policy.MaxDelay
		}
		minPossible := maxPossible / 2

		if delay < minPossible || delay > maxPossible {
			t.Errorf("attempt %d: delay %v was out of expected jitter bounds [%v, %v]", attempt, delay, minPossible, maxPossible)
		}
	}
}
