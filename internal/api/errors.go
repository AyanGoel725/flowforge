package api

import (
	"encoding/json"
	"net/http"
)

// APIError represents a JSON error response.
type APIError struct {
	StatusCode int    `json:"-"`
	Error      string `json:"error"`
	Code       string `json:"code,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if data != nil {
		_ = json.NewEncoder(w).Encode(data)
	}
}

func writeError(w http.ResponseWriter, status int, msg, code string) {
	writeJSON(w, status, APIError{
		StatusCode: status,
		Error:      msg,
		Code:       code,
	})
}
