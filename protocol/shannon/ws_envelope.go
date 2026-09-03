package shannon

import (
	"errors"

	sdktypes "github.com/pokt-network/shannon-sdk/types"
)

// ErrEndpointControlFrame marks a frame the relay miner sent to report a
// condition rather than to carry data — a session expiry, a rejected request.
//
// It travels to the frame callback in the error slot, but it is deliberately
// NOT returned from ProcessEndpointMessage: the bridge treats a returned error
// as terminal, and the decoded body is exactly what the client should see. The
// callback recognises it and grades nothing, because the supplier is not at
// fault for a session boundary and must be neither rewarded nor penalised for
// reporting one.
var ErrEndpointControlFrame = errors.New("shannon ws: endpoint control frame")

// extractEndpointFrameBody returns the bytes a WebSocket frame is really
// carrying, and the HTTP status the relay miner reported alongside them.
//
// The miner puts a backend's raw WebSocket frame straight into
// RelayResponse.Payload, so the overwhelmingly common case is that the payload
// IS the frame and there is nothing to decode. But it delivers its own
// control and error responses — "session expired" as an HTTP 410 when a
// connection lands on a session that has already ended — through the same
// field, as a serialized POKTHTTPResponse. Returning those verbatim ships an
// undecoded protobuf blob to a client expecting JSON, and grades the blob as
// an unparseable frame against a supplier that did nothing wrong.
//
// Found upstream in PATH (commit 1ff57772) with a thirty-minute live
// session-cycle test: connecting on a session boundary reproduced a raw
// protobuf frame followed by close 4000. SAGE became exposed to it when
// WebSocket rebind let connections outlive a session boundary at all.
//
// What keeps a normal frame from being misread as an envelope — which matters
// more than decoding the envelope does, since a false positive would rewrite
// live subscription data — is the status check. proto.Unmarshal is permissive:
// arbitrary bytes routinely parse as a message with unknown fields and every
// known field left at its zero value, so a successful parse proves nothing and
// the result must carry a real HTTP status to be believed.
//
// The JSON check in front of it is a fast path, not a second guard, and the
// difference is worth stating because it looks like one. Every data frame this
// gateway carries opens with '{' or '[', and this is the WebSocket hot path —
// one subscription can push thousands of frames — so skipping proto.Unmarshal
// on all of them is the point. It cannot also be catching false positives:
// '{' is 0x7B, a protobuf tag for field 15 with wire type 3 (start group), and
// '[' is 0x5B, field 11 with the same wire type; the proto runtime rejects
// groups outright, so a payload opening with either never parses at all. Keep
// it for the cost, not for the safety, and do not delete the status check on
// the strength of it.
//
// Anything that is not provably an envelope is returned untouched with status
// 200, which is the behaviour that predates this function.
func extractEndpointFrameBody(payload []byte) ([]byte, int) {
	if looksLikeJSONFrame(payload) {
		return payload, 200
	}
	envelope, err := sdktypes.DeserializeHTTPResponse(payload)
	if err != nil || envelope == nil || envelope.StatusCode == 0 {
		return payload, 200
	}
	return envelope.BodyBz, int(envelope.StatusCode)
}

// looksLikeJSONFrame reports whether the payload opens as JSON, skipping the
// leading whitespace a backend may send. A frame that does is data, and is
// never decoded as anything else.
func looksLikeJSONFrame(payload []byte) bool {
	for _, b := range payload {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '{', '[':
			return true
		default:
			return false
		}
	}
	return false
}
