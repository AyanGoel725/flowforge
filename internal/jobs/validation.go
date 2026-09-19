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
	ErrInvalidPayload  = errors.New("payload must be a valid JSON object or value")
	ErrPayloadTooLarge = fmt.Errorf("payload exceeds maximum size of %d bytes", MaxPayloadSizeBytes)
	ErrJobNotFound     = errors.New("job not found")
	ErrInvalidState    = errors.New("invalid state transition or concurrent update")
)

// CreateJobRequest represents the payload to create a new job.
type CreateJobRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// ValidateCreateRequest checks that the incoming job creation request is valid.
func ValidateCreateRequest(req *CreateJobRequest) error {
	if req == nil {
		return errors.New("request cannot be nil")
	}

	if strings.TrimSpace(req.Type) == "" {
		return ErrEmptyType
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

	return nil
}
