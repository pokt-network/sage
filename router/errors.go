package router

import (
	"encoding/json"
	"net/http"

	"github.com/pokt-network/sage/relay"
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

func jsonRPCErrorValue(code int, message string, id json.RawMessage) jsonRPCError {
	return jsonRPCError{
		JSONRPC: "2.0",
		Error:   jsonRPCErrBody{Code: code, Message: message},
		ID:      id,
	}
}

// renderJSONRPCError and renderJSONError write an error through the relay
// response writer rather than the raw http.ResponseWriter.
//
// This is the path the relay handler must use, not writeJSON*. The relay writer
// holds a write-once guard (relay.HTTPResponseWriter.Write), and it is the same
// writer a middleware writes an error through — so a middleware that already
// answered (parse rejecting a bad body, validate rejecting an RPC type, batch
// rejecting an oversized fan-out) makes this a no-op instead of a second body
// concatenated onto the first. Writing to the raw w bypassed that guard, which
// is the bug this exists to close. It also means an error in shadow mode is
// suppressed, since the guard's shadow check is honoured too.
func renderJSONRPCError(rw relay.ResponseWriter, code int, message string, id json.RawMessage) {
	body, err := json.Marshal(jsonRPCErrorValue(code, message, id))
	if err != nil {
		return
	}
	renderJSON(rw, http.StatusOK, body)
}

func renderJSONError(rw relay.ResponseWriter, statusCode int, message string) {
	body, err := json.Marshal(jsonErrorBody{Error: message})
	if err != nil {
		return
	}
	renderJSON(rw, statusCode, body)
}

func renderJSON(rw relay.ResponseWriter, statusCode int, body []byte) {
	rw.SetHeader("Content-Type", "application/json")
	rw.SetStatusCode(statusCode)
	_ = rw.Write(body)
}
