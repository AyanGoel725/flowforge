package tasks

import (
	"fmt"
	"sync"
)

// Registry maintains a mapping of task types to their respective Handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry creates an empty task registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]Handler),
	}
}

// Register associates a task type name with a handler.
// Panics if the name is already registered or the handler is nil.
func (r *Registry) Register(name string, handler Handler) {
	if handler == nil {
		panic(fmt.Sprintf("tasks: cannot register nil handler for type %q", name))
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[name]; exists {
		panic(fmt.Sprintf("tasks: handler already registered for type %q", name))
	}

	r.handlers[name] = handler
}

// Get finds and returns the handler for the given task type.
// Returns an error if no handler is registered.
func (r *Registry) Get(name string) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	handler, exists := r.handlers[name]
	if !exists {
		return nil, fmt.Errorf("no handler registered for job type %q", name)
	}

	return handler, nil
}
