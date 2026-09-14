package middleware_test

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/relay/middleware"
)

type phaseRelayer struct{}

func (phaseRelayer) SendRelay(_ context.Context, _ domain.ServiceID, ep domain.EndpointAddr, _ domain.Payload) (*domain.Response, error) {
	return &domain.Response{
		HTTPStatusCode: 200, Body: []byte(`{}`), EndpointAddr: ep,
		Phases: domain.RelayPhases{Prepare: time.Millisecond, Sign: 4 * time.Millisecond, HTTP: 90 * time.Millisecond, Verify: 2 * time.Millisecond},
	}, nil
}

// The protocol's split of one relay lands in the stage times under
// send_relay.* names, so the upstream call is not one opaque number.
func TestSendRelay_RecordsPhases(t *testing.T) {
	ctx := &relay.Context{Ctx: context.Background(), ServiceID: "eth", Stages: relay.NewStageTimes()}
	ctx.Endpoint = domain.EndpointAddr("s-https://a.example")
	ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_blockNumber")}
	if err := middleware.SendRelay(phaseRelayer{})(relay.Noop).HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	got := ctx.Stages.Exclusive()
	if got["send_relay.sign"] != 4*time.Millisecond || got["send_relay.http"] != 90*time.Millisecond || got["send_relay.verify"] != 2*time.Millisecond || got["send_relay.prepare"] != time.Millisecond {
		t.Fatalf("phases = %v", got)
	}
}
