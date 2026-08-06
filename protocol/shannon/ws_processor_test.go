package shannon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sdk "github.com/pokt-network/shannon-sdk"

	"github.com/pokt-network/sage/domain"
)

// countingSigner tracks signRelayRequest calls so we can assert signing
// cadence across many WS frames.
type countingSigner struct {
	calls int
}

func (c *countingSigner) signRelayRequest(_ context.Context, req *servicetypes.RelayRequest, _ *apptypes.Application) (*servicetypes.RelayRequest, error) {
	c.calls++
	return req, nil
}

func buildProcessorFixture() (*Protocol, *wsMessageProcessor, *countingSigner, *mockRelayFullNode) {
	fn := &mockRelayFullNode{
		validateResponse: &servicetypes.RelayResponse{Payload: []byte(`ok`)},
	}
	signer := &countingSigner{}
	p := &Protocol{
		fullNode:  fn,
		signer:    signer,
		bl:        newBlacklist(),
		ownedApps: map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		logger:    newTestLogger(),
	}
	header := &sessiontypes.SessionHeader{
		ServiceId:             "eth",
		SessionId:             "s-1",
		SessionEndBlockHeight: 200,
	}
	proc := newWSMessageProcessor(
		context.Background(),
		p,
		header,
		"pokt1supplier",
		"endpoint-1",
		&apptypes.Application{Address: "pokt1app"},
		nil,
	)
	return p, proc, signer, fn
}

func TestWSProcessor_ProcessClientMessage_SignsEachFrame(t *testing.T) {
	_, proc, signer, _ := buildProcessorFixture()

	const n = 25
	for i := 0; i < n; i++ {
		out, err := proc.ProcessClientMessage([]byte(`{"method":"eth_subscribe"}`))
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if len(out) == 0 {
			t.Fatalf("frame %d: empty wire bytes", i)
		}
	}
	if signer.calls != n {
		t.Errorf("signer invoked %d times, want %d (one per frame)", signer.calls, n)
	}
}

func TestWSProcessor_ProcessClientMessage_SessionExpired(t *testing.T) {
	_, proc, _, _ := buildProcessorFixture()
	proc.sessionActive.Store(false)

	_, err := proc.ProcessClientMessage([]byte(`x`))
	if !errors.Is(err, ErrSessionExpired) {
		t.Errorf("want ErrSessionExpired, got %v", err)
	}
}

func TestWSProcessor_ProcessEndpointMessage_InvokesCallback(t *testing.T) {
	fn := &mockRelayFullNode{
		validateResponse: &servicetypes.RelayResponse{Payload: []byte(`{"result":"0x1"}`)},
	}
	p := &Protocol{
		fullNode: fn, signer: &countingSigner{}, bl: newBlacklist(),
		logger: newTestLogger(),
	}

	var gotPayload []byte
	var gotLatency time.Duration
	var gotErr error
	cb := func(payload []byte, err error, latency time.Duration) {
		gotPayload, gotErr, gotLatency = payload, err, latency
	}

	proc := newWSMessageProcessor(
		context.Background(), p,
		&sessiontypes.SessionHeader{ServiceId: "eth", SessionEndBlockHeight: 200},
		"pokt1supplier", "endpoint-1", &apptypes.Application{Address: "pokt1app"}, cb,
	)

	out, err := proc.ProcessEndpointMessage([]byte(`inbound-wire-bytes`))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if string(out) != `{"result":"0x1"}` {
		t.Errorf("output payload = %q, want %q", out, `{"result":"0x1"}`)
	}
	if string(gotPayload) != `{"result":"0x1"}` {
		t.Errorf("callback payload = %q, want %q", gotPayload, `{"result":"0x1"}`)
	}
	if gotErr != nil {
		t.Errorf("callback err = %v, want nil", gotErr)
	}
	if gotLatency < 0 {
		t.Errorf("callback latency = %v, want ≥ 0", gotLatency)
	}
}

func TestWSProcessor_ProcessEndpointMessage_ValidationFailure_Blacklists(t *testing.T) {
	// A signature failure is the supplier's: it signed with a key that is not
	// the one its onchain address publishes.
	fn := &mockRelayFullNode{
		validateErr: fmt.Errorf("%w: bad signature", sdk.ErrRelayResponseValidationSignatureError),
	}
	p := &Protocol{
		fullNode: fn, signer: &countingSigner{}, bl: newBlacklist(),
		logger: newTestLogger(),
	}

	var gotErr error
	proc := newWSMessageProcessor(
		context.Background(), p,
		&sessiontypes.SessionHeader{ServiceId: "eth", SessionEndBlockHeight: 200},
		"pokt1bad", "endpoint-1", &apptypes.Application{Address: "pokt1app"},
		func(_ []byte, err error, _ time.Duration) { gotErr = err },
	)

	_, err := proc.ProcessEndpointMessage([]byte(`garbage`))
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if gotErr == nil {
		t.Error("callback should receive the validation error")
	}
	if !p.bl.IsBlacklisted("eth", "pokt1bad") {
		t.Error("supplier should be blacklisted after validation failure")
	}
}
