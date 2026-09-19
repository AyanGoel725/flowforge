package tasks

import (
	"context"
	"encoding/json"
	"testing"
)

func TestEchoHandler_Handle(t *testing.T) {
	handler := &EchoHandler{}
	ctx := context.Background()

	tests := []struct {
		name        string
		payload     string
		expectError bool
	}{
		{
			name:        "valid message",
			payload:     `{"message": "hello world"}`,
			expectError: false,
		},
		{
			name:        "empty message value",
			payload:     `{"message": "   "}`,
			expectError: true,
		},
		{
			name:        "missing message field",
			payload:     `{"other": 123}`,
			expectError: true,
		},
		{
			name:        "invalid json type",
			payload:     `"plain string"`,
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := handler.Handle(ctx, json.RawMessage(tc.payload))
			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				if string(res) != tc.payload {
					t.Errorf("expected result %s, got %s", tc.payload, string(res))
				}
			}
		})
	}
}
