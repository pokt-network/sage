package router

import (
	"encoding/json"
	"net/http"
)

// jsonRPCError is the standard JSON-RPC 2.0 error response.
type jsonRPCError struct {
	JSONRPC string          `json:"jsonrpc"`
	Error   jsonRPCErrBody  `json:"error"`
	ID      json.RawMessage `json:"id"`
}

type jsonRPCErrBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// jsonErrorBody is a simple JSON error response for non-JSON-RPC requests.
type jsonErrorBody struct {
	Error string `json:"error"`
}

// writeJSONError writes a plain JSON error response with the given HTTP status code.
func writeJSONError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, jsonErrorBody{Error: message})
}

// orEmpty returns s, or an empty slice when s is nil, so JSON encodes []
// rather than null.
func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// writeJSON marshals data to JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	body, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, _ = w.Write(body)
}
