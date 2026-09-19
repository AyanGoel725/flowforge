package tasks

import (
	"context"
	"encoding/json"
	"testing"
)

type dummyHandler struct{}

func (d *dummyHandler) Handle(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	return payload, nil
}

func TestRegistry(t *testing.T) {
	reg := NewRegistry()

	// Register valid handler
	h := &dummyHandler{}
	reg.Register("dummy", h)

	// Get registered handler
	found, err := reg.Get("dummy")
	if err != nil {
		t.Fatalf("expected to find dummy handler, got error: %v", err)
	}
	if found != h {
		t.Fatalf("expected handler pointer match")
	}

	// Get unregistered handler
	_, err = reg.Get("nonexistent")
	if err == nil {
		t.Fatalf("expected error for nonexistent handler")
	}

	// Double registration panics
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected duplicate registration to panic")
		}
	}()
	reg.Register("dummy", h)
}
