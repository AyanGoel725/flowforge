package jobs

import (
	"testing"
)

func TestValidTransitions(t *testing.T) {
	// Table of valid transitions
	validPairs := [][2]Status{
		{StatusPending, StatusQueued},
		{StatusQueued, StatusRunning},
		{StatusRunning, StatusCompleted},
		{StatusRunning, StatusFailed},
	}

	for _, pair := range validPairs {
		from, to := pair[0], pair[1]
		if !ValidTransition(from, to) {
			t.Errorf("expected transition %s -> %s to be valid, but got false", from, to)
		}
	}

	// Table of invalid transitions
	invalidPairs := [][2]Status{
		{StatusPending, StatusRunning},
		{StatusPending, StatusCompleted},
		{StatusPending, StatusFailed},
		{StatusQueued, StatusPending},
		{StatusQueued, StatusCompleted},
		{StatusQueued, StatusFailed},
		{StatusRunning, StatusPending},
		{StatusRunning, StatusQueued},
		{StatusCompleted, StatusPending},
		{StatusCompleted, StatusQueued},
		{StatusCompleted, StatusRunning},
		{StatusCompleted, StatusFailed},
		{StatusFailed, StatusPending},
		{StatusFailed, StatusQueued},
		{StatusFailed, StatusRunning},
		{StatusFailed, StatusCompleted},
		{"UNKNOWN", StatusPending},
	}

	for _, pair := range invalidPairs {
		from, to := pair[0], pair[1]
		if ValidTransition(from, to) {
			t.Errorf("expected transition %s -> %s to be invalid, but got true", from, to)
		}
	}
}
