package shannon

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"google.golang.org/protobuf/proto"

	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/reputation"
)

// envelopeBytes builds the thing the relay miner actually sends for a control
// response: a serialized POKTHTTPResponse carrying a status and a body.
func envelopeBytes(t *testing.T, status uint32, body string) []byte {
	t.Helper()
	bz, err := proto.Marshal(&sdktypes.POKTHTTPResponse{
		StatusCode: status,
		BodyBz:     []byte(body),
	})
	if err != nil {
		t.Fatal(err)
	}
	return bz
}

// The false positive is the dangerous direction: mistaking live subscription
// data for an envelope would rewrite what a client receives.
func TestExtractEndpointFrameBody_DataFramesPassThroughUntouched(t *testing.T) {
	frames := []struct {
		name    string
		payload string
	}{
		{"json-rpc response", `{"jsonrpc":"2.0","result":"0x1","id":1}`},
		{"subscription push", `{"jsonrpc":"2.0","method":"eth_subscription","params":{"result":{}}}`},
		{"batch", `[{"jsonrpc":"2.0","result":"0x1","id":1}]`},
		{"leading whitespace", "  \n\t" + `{"jsonrpc":"2.0","result":1,"id":1}`},
		{"not json and not an envelope", "some plain text a backend sent"},
		{"empty", ""},
	}
	for _, f := range frames {
		t.Run(f.name, func(t *testing.T) {
			body, status := extractEndpointFrameBody([]byte(f.payload))
			if string(body) != f.payload {
				t.Errorf("body = %q, want the payload unchanged (%q)", body, f.payload)
			}
			if status != 200 {
				t.Errorf("status = %d, want 200 for a frame that is not an envelope", status)
			}
		})
	}
}

// The case this exists for: a session expiry arrives as an envelope, and both
// the body and the status have to come out of it.
func TestExtractEndpointFrameBody_UnwrapsEnvelopes(t *testing.T) {
	cases := []struct {
		name   string
		status uint32
		body   string
	}{
		{"session expired", 410, `{"error":"session expired"}`},
		{"bad request", 400, `{"error":"malformed"}`},
		{"a 200 envelope is still an envelope", 200, `{"jsonrpc":"2.0","result":"0x1","id":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, status := extractEndpointFrameBody(envelopeBytes(t, tc.status, tc.body))
			if string(body) != tc.body {
				t.Errorf("body = %q, want %q — the client must get the decoded body, not the protobuf", body, tc.body)
			}
			if status != int(tc.status) {
				t.Errorf("status = %d, want %d", status, tc.status)
			}
		})
	}
}

// proto.Unmarshal is permissive, so "it parsed" is not evidence. A message
// with no status is not a POKTHTTPResponse we are willing to act on.
func TestExtractEndpointFrameBody_ParsesButIsNotAnEnvelope(t *testing.T) {
	// Marshals cleanly as a POKTHTTPResponse with every known field zero.
	payload := envelopeBytes(t, 0, "")
	if len(payload) == 0 {
		payload = []byte{0x18, 0x00} // a bare varint field, still valid protobuf
	}
	body, status := extractEndpointFrameBody(payload)
	if !bytes.Equal(body, payload) || status != 200 {
		t.Errorf("got (%q, %d), want the payload unchanged at 200: a zero status is not a real envelope", body, status)
	}
}

// A frame that opens as JSON is never decoded, even if its bytes would also
// parse as protobuf. The JSON check runs first for exactly this reason.
func TestExtractEndpointFrameBody_JSONWins(t *testing.T) {
	payload := []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)
	body, status := extractEndpointFrameBody(payload)
	if !bytes.Equal(body, payload) || status != 200 {
		t.Errorf("got (%q, %d), want the JSON frame untouched at 200", body, status)
	}
}

// End to end through the processor: the client gets readable JSON, and the
// callback is told this frame is not the supplier's doing.
func TestWSProcessor_ControlFrameForwardsDecodedBodyUngraded(t *testing.T) {
	const body = `{"error":"session expired"}`
	fn := &mockRelayFullNode{
		validateResponse: &servicetypes.RelayResponse{Payload: envelopeBytes(t, 410, body)},
	}
	p := &Protocol{
		fullNode: fn, signer: &countingSigner{}, bl: newBlacklist(),
		logger: newTestLogger(),
	}

	var gotPayload []byte
	var gotErr error
	cb := func(payload []byte, err error, _ time.Duration) {
		gotPayload, gotErr = payload, err
	}

	proc := newWSMessageProcessor(
		context.Background(), p,
		&sessiontypes.SessionHeader{ServiceId: "eth", SessionEndBlockHeight: 200},
		"pokt1supplier", "endpoint-1", &apptypes.Application{Address: "pokt1app"}, cb,
	)

	out, err := proc.ProcessEndpointMessage([]byte(`inbound-wire-bytes`))
	if err != nil {
		t.Fatalf("returned an error: %v — the bridge would treat that as terminal", err)
	}
	if string(out) != body {
		t.Errorf("forwarded %q, want the decoded body %q — a client must never see the protobuf", out, body)
	}
	if string(gotPayload) != body {
		t.Errorf("callback payload = %q, want the decoded body %q", gotPayload, body)
	}
	if !errors.Is(gotErr, ErrEndpointControlFrame) {
		t.Errorf("callback err = %v, want ErrEndpointControlFrame so the frame is graded neither up nor down", gotErr)
	}
}

// The ordinary path must be untouched by all of the above.
func TestWSProcessor_DataFrameStillGradedNormally(t *testing.T) {
	const body = `{"jsonrpc":"2.0","result":"0x1","id":1}`
	fn := &mockRelayFullNode{
		validateResponse: &servicetypes.RelayResponse{Payload: []byte(body)},
	}
	p := &Protocol{
		fullNode: fn, signer: &countingSigner{}, bl: newBlacklist(),
		logger: newTestLogger(),
	}

	var gotErr error
	called := false
	cb := func(_ []byte, err error, _ time.Duration) { gotErr, called = err, true }

	proc := newWSMessageProcessor(
		context.Background(), p,
		&sessiontypes.SessionHeader{ServiceId: "eth", SessionEndBlockHeight: 200},
		"pokt1supplier", "endpoint-1", &apptypes.Application{Address: "pokt1app"}, cb,
	)

	out, err := proc.ProcessEndpointMessage([]byte(`inbound-wire-bytes`))
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != body {
		t.Errorf("forwarded %q, want %q", out, body)
	}
	if !called {
		t.Fatal("callback not invoked for a data frame")
	}
	if gotErr != nil {
		t.Errorf("callback err = %v, want nil so the frame is graded as usual", gotErr)
	}
}

// A control frame is the one frame graded in neither direction. Rewarding a
// supplier for reporting a session expiry was the upstream bug; penalising it
// would be a new one, since a session boundary is not the supplier's doing.
func TestWSRelayer_ControlFrameRecordsNoSignal(t *testing.T) {
	rep := &spyRepSvc{}
	r := NewWSRelayer(WSRelayerDeps{
		Protocol: &Protocol{}, Reputation: rep, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(),
	})

	r.handleEndpointFrame("eth", "ep1", []byte(`{"error":"session expired"}`),
		ErrEndpointControlFrame, 10*time.Millisecond)

	if len(rep.calls) != 0 {
		t.Fatalf("recorded %d signals for a control frame, want 0 (got %q)",
			len(rep.calls), rep.calls[0].signal.Type)
	}
}

// And the frames either side of it still grade, so the branch above cannot
// swallow real verdicts.
func TestWSRelayer_ControlFrameBranchDoesNotSwallowRealErrors(t *testing.T) {
	rep := &spyRepSvc{}
	r := NewWSRelayer(WSRelayerDeps{
		Protocol: &Protocol{}, Reputation: rep, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(),
	})

	// A validation failure is still the supplier's, and still major.
	r.handleEndpointFrame("eth", "ep1", nil, errors.New("bad signature"), 0)
	if len(rep.calls) != 1 || rep.calls[0].signal.Type != reputation.SignalMajorError {
		t.Fatalf("validation failure: got %+v, want one major error", rep.calls)
	}
}
