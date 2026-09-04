package jsonheight

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

const ep = domain.EndpointAddr("supA-https://node.example.com")

// A height read out of a real response shape, per chain. These are the two
// facts each declaration asserts, and getting either wrong grades a healthy
// supplier down every cycle — so they are pinned against the response bodies
// the chains actually return.
func TestExtractData_ReadsTheDeclaredPath(t *testing.T) {
	cases := []struct {
		chain    Chain
		response string
		want     uint64
	}{
		{
			chain:    NEAR,
			response: `{"jsonrpc":"2.0","result":{"header":{"height":123456789,"hash":"abc"}},"id":1}`,
			want:     123456789,
		},
		{
			// Sui returns the sequence number as a decimal STRING.
			chain:    Sui,
			response: `{"jsonrpc":"2.0","result":"48219933","id":1}`,
			want:     48219933,
		},
		{
			chain:    EthBeacon,
			response: `{"data":{"header":{"message":{"slot":"9482113","proposer_index":"1"}}}}`,
			want:     9482113,
		},
		{
			chain:    Radix,
			response: `{"ledger_state":{"network":"mainnet","state_version":88123456,"epoch":1234}}`,
			want:     88123456,
		},
	}

	for _, tc := range cases {
		t.Run(tc.chain.Name, func(t *testing.T) {
			p := NewPlugin(nil, tc.chain, 100)
			data, err := p.ExtractData(ep, tc.chain.Probe.Bytes(), []byte(tc.response))
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			if data.BlockHeight == nil {
				t.Fatalf("no height read at %q from %s", tc.chain.HeightPath, tc.response)
			}
			if *data.BlockHeight != tc.want {
				t.Errorf("height = %d, want %d", *data.BlockHeight, tc.want)
			}
		})
	}
}

// Most client traffic is not asking for the head. Reading a height out of a
// response to something else, or grading a supplier for not supplying one,
// would both be wrong.
func TestExtractData_IgnoresRequestsThatCarryNoHeight(t *testing.T) {
	p := NewPlugin(nil, NEAR, 100)

	data, err := p.ExtractData(ep,
		[]byte(`{"jsonrpc":"2.0","id":1,"method":"query","params":{}}`),
		[]byte(`{"jsonrpc":"2.0","result":{"anything":1},"id":1}`),
	)
	if err != nil {
		t.Fatalf("a request that is not asking for the head must not be an error: %v", err)
	}
	if data.BlockHeight != nil {
		t.Errorf("read a height of %d out of a query response", *data.BlockHeight)
	}
}

// A chain that declares how a client names its method learns heights from
// client traffic too, not only from its own probe. That is what keeps a busy
// service's chain view fresh between cycles.
func TestExtractData_ReadsClientTrafficWhenTheChainSaysHow(t *testing.T) {
	p := NewPlugin(nil, NEAR, 100)

	// Not the probe body — a client's own block call, formatted differently.
	data, err := p.ExtractData(ep,
		[]byte(`{"method":"block","params":{"finality":"optimistic"},"id":"c","jsonrpc":"2.0"}`),
		[]byte(`{"jsonrpc":"2.0","result":{"header":{"height":42}},"id":"c"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if data.BlockHeight == nil || *data.BlockHeight != 42 {
		t.Errorf("client traffic did not contribute a height: %+v", data)
	}
}

// A REST chain's probe carries no body, so no request-shaped test could tell
// it from a client call. The response is what decides: a body carrying the
// declared path answered the question, whoever asked it.
func TestExtractData_RESTChainReadsTheResponseNotTheRequest(t *testing.T) {
	p := NewPlugin(nil, EthBeacon, 100)

	data, err := p.ExtractData(ep, nil, []byte(`{"data":{"header":{"message":{"slot":"9482113"}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if data.BlockHeight == nil || *data.BlockHeight != 9482113 {
		t.Fatalf("bodyless REST probe read no height: %+v", data)
	}

	// And a REST response that is not a head header is simply not a height,
	// not a fault: nothing here can tell whose request it was.
	data, err = p.ExtractData(ep, nil, []byte(`{"data":{"something":"else"}}`))
	if err != nil {
		t.Errorf("graded an endpoint for a response to a request it cannot identify: %v", err)
	}
	if data.BlockHeight != nil {
		t.Error("invented a height from a response with none")
	}
}

// A response that does not carry the declared path is an error against the
// endpoint that sent it: the probe asked a question it did not answer.
func TestExtractData_MissingPathIsAnError(t *testing.T) {
	p := NewPlugin(nil, NEAR, 100)
	if _, err := p.ExtractData(ep, NEAR.Probe.Bytes(), []byte(`{"jsonrpc":"2.0","result":{},"id":1}`)); err == nil {
		t.Error("a probe response with no height at the declared path must not pass silently")
	}

	// The same empty response to a request that was not asking for the head is
	// not the endpoint's fault and must not be graded.
	if _, err := p.ExtractData(ep,
		[]byte(`{"jsonrpc":"2.0","id":1,"method":"query","params":{}}`),
		[]byte(`{"jsonrpc":"2.0","result":{},"id":1}`),
	); err != nil {
		t.Errorf("graded an endpoint for a query response having no height: %v", err)
	}
}

// Selection must not empty a pool before anything has reported, and must not
// filter at all when the operator set no allowance.
//
// Both come from qos.MinAllowedHeight, which yields 0 for a zero allowance or
// a zero perceived and so makes the filter pass everything — this plugin adds
// no guard of its own, the same as the EVM plugin. So these cases pin a rule
// that lives in the shared selector rather than here, which is the point: they
// are what a caller depends on, whichever file enforces it.
func TestSelectEndpoints_DoesNotFilterWithoutHeightsOrAnAllowance(t *testing.T) {
	const fresh = domain.EndpointAddr("supA-https://fresh.example.com")
	const stale = domain.EndpointAddr("supB-https://stale.example.com")
	endpoints := domain.EndpointAddrList{fresh, stale}

	t.Run("no allowance, even with a laggard known", func(t *testing.T) {
		p := NewPlugin(nil, NEAR, 0)
		p.UpdateBlockHeight(fresh, 1000)
		p.UpdateBlockHeight(stale, 1)

		got, err := p.SelectEndpoints(endpoints, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Errorf("selected %v with sync_allowance 0; an operator who set no allowance asked for no filtering", got)
		}
	})

	t.Run("an allowance, but nothing has reported yet", func(t *testing.T) {
		p := NewPlugin(nil, NEAR, 10)
		got, err := p.SelectEndpoints(endpoints, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Errorf("selected %v before any height was known; a cold start must not empty a pool", got)
		}
	})
}

// And once heights are known it filters, which is the whole point: on the
// passthrough these services had a sync_allowance that governed nothing.
func TestSelectEndpoints_FiltersOnceHeightsAreKnown(t *testing.T) {
	p := NewPlugin(nil, NEAR, 10)
	const fresh = domain.EndpointAddr("supA-https://fresh.example.com")
	const stale = domain.EndpointAddr("supB-https://stale.example.com")

	p.UpdateBlockHeight(fresh, 1000)
	p.UpdateBlockHeight(stale, 500)

	got, err := p.SelectEndpoints(domain.EndpointAddrList{fresh, stale}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != fresh {
		t.Errorf("selected %v, want only the fresh endpoint — 500 blocks behind is outside an allowance of 10", got)
	}
}

// The probe is the only source of the fact this plugin exists for, so it must
// be Essential or traffic-informed probing could skip it away.
func TestHealthChecks_TheProbeIsEssential(t *testing.T) {
	for name, chain := range Declared() {
		checks := NewPlugin(nil, chain, 100).HealthChecks()
		if len(checks) != 1 {
			t.Errorf("%s: %d checks, want 1", name, len(checks))
			continue
		}
		if !checks[0].Essential {
			t.Errorf("%s: the height probe is not Essential and could be skipped away", name)
		}
	}
}

// A request must reach the supplier as the chain expects it: a REST chain's
// probe carries a path and a verb, or the miner replays a bare POST at the
// backend's root and the check fails for a reason that has nothing to do with
// the endpoint.
func TestChains_RESTProbesCarryTheirPath(t *testing.T) {
	for _, chain := range []Chain{EthBeacon, Radix} {
		if chain.Probe.RPCType() != domain.RPCTypeREST {
			t.Errorf("%s: expected a REST probe", chain.Name)
		}
		if chain.Probe.Path() == "" {
			t.Errorf("%s: REST probe carries no path; the miner would POST at the backend root", chain.Name)
		}
		if chain.Probe.HTTPMethod() == "" {
			t.Errorf("%s: REST probe carries no HTTP method", chain.Name)
		}
	}
}

// Every declared chain has to be a type config reporting recognises, or a
// service either claims QoS it does not get or gets QoS nobody is told about.
func TestEveryDeclaredChainIsAKnownType(t *testing.T) {
	for serviceType := range Declared() {
		if !domain.IsKnownServiceType(serviceType) {
			t.Errorf("%q has a plugin but domain.KnownServiceTypes does not list it: "+
				"the startup report would call it a passthrough service", serviceType)
		}
	}
}

// The plugin must carry every optional capability the executor gates on, or it
// silently degrades to the passthrough it replaces.
func TestPlugin_CarriesTheCapabilitiesTheExecutorGatesOn(t *testing.T) {
	var p any = NewPlugin(nil, NEAR, 100)
	for name, ok := range map[string]bool{
		"HealthChecker":      func() bool { _, ok := p.(qos.HealthChecker); return ok }(),
		"DataExtractor":      func() bool { _, ok := p.(qos.DataExtractor); return ok }(),
		"BlockHeightTracker": func() bool { _, ok := p.(qos.BlockHeightTracker); return ok }(),
		"ChainViewer":        func() bool { _, ok := p.(qos.ChainViewer); return ok }(),
		"HeightObserver":     func() bool { _, ok := p.(qos.HeightObserver); return ok }(),
	} {
		if !ok {
			t.Errorf("not a %s: the executor gates height tracking on it and would skip this plugin", name)
		}
	}
}

// ParseRequest carries a REST path through, the way the passthrough does —
// these chains are still ones SAGE knows one fact about.
func TestParseRequest_CarriesRESTShapeThrough(t *testing.T) {
	p := NewPlugin(nil, EthBeacon, 100)
	payloads, err := p.ParseRequest(context.Background(), nil, []byte(`{"a":1}`), domain.RPCTypeREST)
	if err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 1 {
		t.Fatalf("got %d payloads, want 1", len(payloads))
	}
	if string(payloads[0].Bytes()) != `{"a":1}` {
		t.Errorf("body was altered: %q", payloads[0].Bytes())
	}
}
