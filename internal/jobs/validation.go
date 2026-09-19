package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	// MaxPayloadSizeBytes is the maximum allowed size for a job payload (1MB).
	MaxPayloadSizeBytes = 1024 * 1024
)

var (
	ErrEmptyType       = errors.New("job type is required")
	ErrUnsupportedType = errors.New("unsupported job type")
	ErrInvalidPayload  = errors.New("payload must be a valid JSON object or value")
	ErrPayloadTooLarge = fmt.Errorf("payload exceeds maximum size of %d bytes", MaxPayloadSizeBytes)
	ErrJobNotFound     = errors.New("job not found")
	ErrInvalidState    = errors.New("invalid state transition or concurrent update")
	ErrInvalidAttempts = errors.New("max_attempts must be greater than 0")
)

// SupportedTypes defines the allowlisted job types.
var SupportedTypes = map[string]bool{
	"echo":           true,
	"sleep":          true,
	"flaky":          true,
	"always_fail":    true,
	"permanent_fail": true,
}

// CreateJobRequest represents the payload to create a new job.
type CreateJobRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts *int            `json:"max_attempts,omitempty"`
}

// ValidateCreateRequest checks that the incoming job creation request is valid.
func ValidateCreateRequest(req *CreateJobRequest) error {
	if req == nil {
		return errors.New("request cannot be nil")
	}

	trimmedType := strings.TrimSpace(req.Type)
	if trimmedType == "" {
		return ErrEmptyType
	}

	if !SupportedTypes[trimmedType] {
		return ErrUnsupportedType
	}

	if len(req.Payload) == 0 {
		return ErrInvalidPayload
	}

	if len(req.Payload) > MaxPayloadSizeBytes {
		return ErrPayloadTooLarge
	}

	if !json.Valid(req.Payload) {
		return ErrInvalidPayload
	}

	if req.MaxAttempts != nil && *req.MaxAttempts <= 0 {
		return ErrInvalidAttempts
	}

	return nil
}
