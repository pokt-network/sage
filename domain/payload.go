package domain

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/tidwall/gjson"
)

// Payload is the raw bytes of a service request.
type Payload struct {
	data    []byte
	rpcType RPCType
	method  string // JSON-RPC method name, if applicable

	// path and httpMethod describe the HTTP request the supplier's backend
	// should see. For JSON-RPC the body is the whole request and these stay
	// empty (POST "/"), but for REST and CometBFT the path *is* the request —
	// dropping it makes every relay hit the backend's root.
	path       string // request URI, including query string ("/status?height=5")
	httpMethod string // GET, POST, … ("" means POST)

	// contentType is the media type the backend should see. Empty means
	// application/json, which is right for every JSON transport; a gRPC relay
	// carries protobuf and has to say so.
	contentType string
}

// NewPayload creates a new Payload. The relay is sent as POST to the
// supplier's root path; use WithHTTP for requests whose path or verb matter.
func NewPayload(data []byte, rpcType RPCType, method string) Payload {
	return Payload{data: data, rpcType: rpcType, method: method}
}

// WithHTTP returns a copy of the payload carrying the HTTP path (request URI,
// query string included) and verb to replay against the supplier's backend.
// An empty path or verb keeps the "/" + POST default.
func (p Payload) WithHTTP(path, httpMethod string) Payload {
	p.path = path
	p.httpMethod = httpMethod
	return p
}

// WithContentType returns a copy of the payload carrying the media type to
// send to the supplier's backend. Empty keeps the application/json default.
func (p Payload) WithContentType(contentType string) Payload {
	p.contentType = contentType
	return p
}

// Bytes returns the raw payload bytes.
func (p Payload) Bytes() []byte { return p.data }

// RPCType returns the RPC type of this payload.
func (p Payload) RPCType() RPCType { return p.rpcType }

// JSONRPCID returns the request's "id" member exactly as it was written —
// number, string or null — so an error answered on the request's behalf
// carries the id the client will match it by. A payload with no id, or one
// that is not JSON at all, yields null: per JSON-RPC 2.0 that is the id of an
// error whose request could not be read.
func (p Payload) JSONRPCID() json.RawMessage {
	id := gjson.GetBytes(p.data, "id")
	if !id.Exists() {
		return json.RawMessage("null")
	}
	return json.RawMessage(id.Raw)
}

// Method returns the JSON-RPC method name (empty for non-JSON-RPC).
func (p Payload) Method() string { return p.method }

// Path returns the HTTP request URI to replay ("" means root).
func (p Payload) Path() string { return p.path }

// HTTPMethod returns the HTTP verb to replay ("" means POST).
func (p Payload) HTTPMethod() string { return p.httpMethod }

// ContentType returns the media type for the backend ("" means
// application/json).
func (p Payload) ContentType() string { return p.contentType }

// Response holds the result of a relay.
type Response struct {
	Body           []byte
	HTTPStatusCode int
	Latency        time.Duration
	EndpointAddr   EndpointAddr

	// Headers carries response headers that the caller cannot reconstruct from
	// the body. It stays nil for the JSON transports — allocating a map per
	// relay to hold nothing is a hot-path cost for no one's benefit — and is
	// populated only where a header changes the meaning of the response, which
	// today means gRPC: its outcome travels in grpc-status, not in the body.
	Headers map[string]string
}

// GRPCStatus reports the gRPC status code and message a response carried, and
// whether it carried one at all. An absent grpc-status means OK, per gRPC.
func (r *Response) GRPCStatus() (code int, message string, ok bool) {
	if r == nil || r.Headers == nil {
		return 0, "", false
	}
	raw, present := r.Headers["grpc-status"]
	if !present {
		return 0, "", false
	}
	code, err := strconv.Atoi(raw)
	if err != nil {
		return 0, "", false
	}
	return code, r.Headers["grpc-message"], true
}
