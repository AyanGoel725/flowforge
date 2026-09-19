package jobs

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestValidateCreateRequest(t *testing.T) {
	tests := []struct {
		name        string
		req         *CreateJobRequest
		expectedErr error
	}{
		{
			name:        "nil request",
			req:         nil,
			expectedErr: errors.New("request cannot be nil"),
		},
		{
			name: "empty type",
			req: &CreateJobRequest{
				Type:    "",
				Payload: json.RawMessage(`{"msg": "hi"}`),
			},
			expectedErr: ErrEmptyType,
		},
		{
			name: "whitespace type",
			req: &CreateJobRequest{
				Type:    "   ",
				Payload: json.RawMessage(`{"msg": "hi"}`),
			},
			expectedErr: ErrEmptyType,
		},
		{
			name: "empty payload",
			req: &CreateJobRequest{
				Type:    "echo",
				Payload: nil,
			},
			expectedErr: ErrInvalidPayload,
		},
		{
			name: "invalid json payload",
			req: &CreateJobRequest{
				Type:    "echo",
				Payload: json.RawMessage(`{not valid json}`),
			},
			expectedErr: ErrInvalidPayload,
		},
		{
			name: "oversized payload",
			req: &CreateJobRequest{
				Type:    "echo",
				Payload: json.RawMessage(`"` + strings.Repeat("a", MaxPayloadSizeBytes+10) + `"`),
			},
			expectedErr: ErrPayloadTooLarge,
		},
		{
			name: "valid request with object",
			req: &CreateJobRequest{
				Type:    "echo",
				Payload: json.RawMessage(`{"message": "hello world"}`),
			},
			expectedErr: nil,
		},
		{
			name: "valid request with scalar json",
			req: &CreateJobRequest{
				Type:    "sleep",
				Payload: json.RawMessage(`123`),
			},
			expectedErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCreateRequest(tc.req)
			if tc.expectedErr == nil {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.expectedErr.Error())
				}
				if !strings.Contains(err.Error(), tc.expectedErr.Error()) {
					t.Fatalf("expected error %q to contain %q", err.Error(), tc.expectedErr.Error())
				}
			}
		})
	}
}
