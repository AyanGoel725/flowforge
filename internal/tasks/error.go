package tasks

import (
	"errors"
)

// TaskError represents an execution error from a task handler that specifies
// whether the failure is transient (retryable) or permanent (fatal).
type TaskError struct {
	Err       error
	Retryable bool
}

func (e *TaskError) Error() string {
	if e == nil || e.Err == nil {
		return "task execution failed"
	}
	return e.Err.Error()
}

func (e *TaskError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// NewRetryableError wraps an error as a retryable (transient) task error.
func NewRetryableError(err error) error {
	if err == nil {
		return nil
	}
	return &TaskError{
		Err:       err,
		Retryable: true,
	}
}

// NewPermanentError wraps an error as a permanent (non-retryable) task error.
func NewPermanentError(err error) error {
	if err == nil {
		return nil
	}
	return &TaskError{
		Err:       err,
		Retryable: false,
	}
}

// IsRetryable determines whether an error indicates a transient failure that should be retried.
// If the error is a *TaskError, its Retryable field is returned.
// For unclassified/standard errors, the default policy is permanent (non-retryable) to prevent
// infinite retry loops on application logic errors or invalid input.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var taskErr *TaskError
	if errors.As(err, &taskErr) {
		return taskErr.Retryable
	}
	return false
}
