package jobs

// ValidTransition checks if a job state transition is allowed.
// Valid transitions:
//
//	PENDING -> QUEUED
//	QUEUED  -> RUNNING
//	RUNNING -> COMPLETED
//	RUNNING -> FAILED
func ValidTransition(from, to Status) bool {
	switch from {
	case StatusPending:
		return to == StatusQueued
	case StatusQueued:
		return to == StatusRunning
	case StatusRunning:
		return to == StatusCompleted || to == StatusFailed
	default:
		return false
	}
}
