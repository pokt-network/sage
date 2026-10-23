package router

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/protocol"
)

// DebugRelayFunc sends one probe relay to exactly target (see
// protocol/shannon/debug.go).
type DebugRelayFunc func(ctx context.Context, serviceID domain.ServiceID, target string, rpcType domain.RPCType, path, httpMethod string, body []byte) (any, error)

// DebugSubscribeFunc holds one probe subscription on exactly target.
type DebugSubscribeFunc func(ctx context.Context, serviceID domain.ServiceID, target string, subscribe []byte, duration time.Duration, maxEvents int, includePayload, includeSigned bool) (any, error)

// SetDebugProbe installs the probe the debug routes serve. Without it they
// answer 501.
func (a *AdminAPI) SetDebugProbe(relay DebugRelayFunc, subscribe DebugSubscribeFunc) {
	a.debugRelay, a.debugSub = relay, subscribe
}

// debugBodyLimit bounds a probe request: the payload's own 64 KiB plus room
// for the fields around it.
const debugBodyLimit = 128 << 10

// handleDebugRelay sends one signed relay to exactly the named target — an
// endpoint address, a URL, an operator, a supplier address, an owner
// address, or "reference" — in the service's current session, outside the middleware chain (no retry,
// hedge, scoring or metrics), and returns the answer with the signed request
// and response bytes (base64) as evidence, and "analysis", the response
// analyzer's verdict on it before the plugin's refinements and per-service
// flags. Body: {"service_id", "target", "rpc_type" (default json_rpc),
// "payload" (a JSON-RPC request), "path", "http_method"}; with a path
// ("/cosmos/base/tendermint/v1beta1/blocks/latest", "/status?height=5") the
// relay is that HTTP request instead, http_method GET (default) or POST,
// payload its body if any: rest needs one, comet_bft takes either. 404 when
// the target is not in the current session (the error lists the operators
// that are), 429 past 10 probe relays a second, 400 for a request it cannot
// send (no method and no path, a path not starting with "/", grpc or
// websocket). A supplier failure is a 200 with "error" set.
func (a *AdminAPI) handleDebugRelay(w http.ResponseWriter, req *http.Request) {
	if a.debugRelay == nil {
		writeJSONError(w, http.StatusNotImplemented, "the debug probe is not available in this build")
		return
	}
	var body struct {
		ServiceID string          `json:"service_id"`
		Target    string          `json:"target"`
		RPCType   string          `json:"rpc_type"`
		Payload   json.RawMessage `json:"payload"`
		Path      string          `json:"path"`
		Method    string          `json:"http_method"`
	}
	if !decodeDebugBody(w, req, &body) {
		return
	}
	if body.ServiceID == "" || body.Target == "" {
		writeJSONError(w, http.StatusBadRequest, "service_id and target are required")
		return
	}
	rpcType := cmp.Or(domain.RPCType(body.RPCType), domain.RPCTypeJSONRPC)
	res, err := a.debugRelay(req.Context(), domain.ServiceID(body.ServiceID), body.Target, rpcType, body.Path, body.Method, body.Payload)
	writeDebugResult(w, res, err)
}

// handleDebugSubscribe opens a WebSocket to exactly the named target (as for
// the relay route), sends the subscribe, and records every frame with its
// time until duration_s (at most 300), max_events (at most 20000) or the end
// of the session it was signed for; it does not rebind. Body:
// {"service_id", "target", "payload" (the subscribe), "duration_s",
// "max_events", "include_payload", "include_signed"}. Each event carries
// t_ms and what it names (block_number, block_hash, tx_hash, log_index);
// include_signed adds each signed frame (base64). At most 5 run at once
// (429). The request blocks for the whole run; a disconnect cancels it.
func (a *AdminAPI) handleDebugSubscribe(w http.ResponseWriter, req *http.Request) {
	if a.debugSub == nil {
		writeJSONError(w, http.StatusNotImplemented, "the debug probe is not available in this build")
		return
	}
	var body struct {
		ServiceID      string          `json:"service_id"`
		Target         string          `json:"target"`
		Payload        json.RawMessage `json:"payload"`
		DurationS      int             `json:"duration_s"`
		MaxEvents      int             `json:"max_events"`
		IncludePayload bool            `json:"include_payload"`
		IncludeSigned  bool            `json:"include_signed"`
	}
	if !decodeDebugBody(w, req, &body) {
		return
	}
	if body.ServiceID == "" || body.Target == "" {
		writeJSONError(w, http.StatusBadRequest, "service_id and target are required")
		return
	}
	res, err := a.debugSub(req.Context(), domain.ServiceID(body.ServiceID), body.Target, body.Payload,
		time.Duration(body.DurationS)*time.Second, body.MaxEvents, body.IncludePayload, body.IncludeSigned)
	writeDebugResult(w, res, err)
}

func decodeDebugBody(w http.ResponseWriter, req *http.Request, v any) bool {
	req.Body = http.MaxBytesReader(w, req.Body, debugBodyLimit)
	if err := json.NewDecoder(req.Body).Decode(v); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func writeDebugResult(w http.ResponseWriter, res any, err error) {
	switch {
	case errors.Is(err, protocol.ErrDebugBadRequest):
		writeJSONError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, protocol.ErrDebugTargetNotFound):
		writeJSONError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, protocol.ErrDebugBusy):
		writeJSONError(w, http.StatusTooManyRequests, err.Error())
	case err != nil:
		writeJSONError(w, http.StatusInternalServerError, err.Error())
	default:
		writeJSON(w, http.StatusOK, res)
	}
}
