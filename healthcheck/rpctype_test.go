package healthcheck

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// perTypeEndpoints answers per RPC type, the way a real session does: a
// service is staked per type, and asking for a type nothing staked returns
// nothing.
type perTypeEndpoints struct {
	mu     sync.Mutex
	byType map[domain.RPCType]domain.EndpointAddrList
	asked  []domain.RPCType
}

func (p *perTypeEndpoints) AvailableEndpoints(_ context.Context, _ domain.ServiceID, rpcType domain.RPCType) (domain.EndpointAddrList, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.asked = append(p.asked, rpcType)
	return p.byType[rpcType], nil
}

func (p *perTypeEndpoints) askedFor() []domain.RPCType {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]domain.RPCType(nil), p.asked...)
}

func checkOfType(name string, rpcType domain.RPCType) qos.HealthCheck {
	return qos.HealthCheck{
		Name:    name,
		Payload: domain.NewPayload([]byte(`{}`), rpcType, name),
	}
}

func newRPCTypeExecutor(t *testing.T, eps *perTypeEndpoints, checks ...qos.HealthCheck) (*Executor, *stubRelayer) {
	t.Helper()
	relayer := &stubRelayer{response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)}}
	reg := qos.NewRegistry()
	if err := reg.Register("svc", &checkOnlyPlugin{checks: checks}); err != nil {
		t.Fatal(err)
	}
	e := NewExecutor(
		relayer, eps,
		&stubSessionManager{services: map[domain.ServiceID]struct{}{"svc": {}}},
		reg, &stubRepService{}, nil,
		time.Minute, 4, slog.New(slog.DiscardHandler),
	)
	e.configured.Store(&ConfiguredChecks{})
	e.warm.Store(true)
	return e, relayer
}

// The defect: a service staked only for REST was never health-checked at all,
// and said nothing about it.
//
// The executor fetched ONE endpoint list per service, hardcoded to JSON-RPC,
// and ran every check against it whatever type the check's payload carried. A
// REST-only service returns an empty list for JSON-RPC, so the loop found no
// backends and moved on — silently, every cycle, for the life of the process.
// Five services on the mainnet canary sat at zero probes on 2026-09-03 and it
// took reading the code to say why.
func TestRunOnce_ProbesAServiceStakedOnlyForREST(t *testing.T) {
	eps := &perTypeEndpoints{byType: map[domain.RPCType]domain.EndpointAddrList{
		// Nothing staked JSON-RPC. This is the shape of a beacon-chain service.
		domain.RPCTypeREST: {"supA-https://rest.example.com"},
	}}
	e, relayer := newRPCTypeExecutor(t, eps, checkOfType("status", domain.RPCTypeREST))

	e.runOnce(context.Background())
	e.wg.Wait()

	relayer.mu.Lock()
	sent := len(relayer.calls)
	relayer.mu.Unlock()
	if sent == 0 {
		t.Fatalf("no probe sent to a REST-staked service; asked for %v", eps.askedFor())
	}
}

// Each check is asked for against the type it declares, so two types on one
// service both get probed rather than one silently winning.
func TestRunOnce_FetchesEndpointsPerRPCType(t *testing.T) {
	eps := &perTypeEndpoints{byType: map[domain.RPCType]domain.EndpointAddrList{
		domain.RPCTypeJSONRPC: {"supA-https://rpc.example.com"},
		domain.RPCTypeREST:    {"supB-https://rest.example.com"},
	}}
	e, relayer := newRPCTypeExecutor(t, eps,
		checkOfType("block", domain.RPCTypeJSONRPC),
		checkOfType("status", domain.RPCTypeREST),
	)

	e.runOnce(context.Background())
	e.wg.Wait()

	asked := eps.askedFor()
	seen := map[domain.RPCType]bool{}
	for _, rt := range asked {
		seen[rt] = true
	}
	for _, want := range []domain.RPCType{domain.RPCTypeJSONRPC, domain.RPCTypeREST} {
		if !seen[want] {
			t.Errorf("never asked for %s endpoints; asked for %v", want, asked)
		}
	}
	relayer.mu.Lock()
	sent := len(relayer.calls)
	relayer.mu.Unlock()
	if sent != 2 {
		t.Errorf("sent %d probes, want one per type", sent)
	}
}

// A check with no RPC type on its payload is treated as JSON-RPC rather than
// asked for under the empty string, which would match no staking at all.
func TestChecksByRPCType_DefaultsToJSONRPC(t *testing.T) {
	got := checksByRPCType([]qos.HealthCheck{{Name: "bare"}})
	if len(got[domain.RPCTypeJSONRPC]) != 1 {
		t.Errorf("a typeless check landed under %v, want json_rpc", got)
	}
}
