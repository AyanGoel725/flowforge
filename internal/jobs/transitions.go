package jobs

// ValidTransition checks if a job state transition is allowed.
// Valid transitions in Stage 2:
//
//	PENDING    -> QUEUED     (Outbox dispatcher publishes job)
//	QUEUED     -> RUNNING    (Worker picks up message)
//	RUNNING    -> COMPLETED  (Task execution succeeds)
//	RUNNING    -> RETRY_WAIT (Task fails with retryable error and attempts remain)
//	RUNNING    -> FAILED     (Task fails permanently or retries exhausted)
//	RETRY_WAIT -> QUEUED     (Retry scheduler re-queues job when next_attempt_at <= now)
func ValidTransition(from, to Status) bool {
	switch from {
	case StatusPending:
		return to == StatusQueued
	case StatusQueued:
		return to == StatusRunning
	case StatusRunning:
		return to == StatusCompleted || to == StatusFailed || to == StatusRetryWait
	case StatusRetryWait:
		return to == StatusQueued
	default:
		return false
	}
}
