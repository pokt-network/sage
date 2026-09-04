package tron

import (
	"context"
	"log/slog"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

func newTestPlugin(t *testing.T) *Plugin {
	t.Helper()
	return NewPlugin(slog.New(slog.DiscardHandler), Config{SyncAllowance: 100})
}

// The reason this plugin exists: the EVM plugin refuses a non-JSON-RPC request
// with a non-retryable validation error, which on the PATH fleet's measured
// split would have turned 28% of TRON's relays into errors.
func TestParseRequest_AcceptsBothFramings(t *testing.T) {
	p := newTestPlugin(t)

	t.Run("json-rpc is parsed as json-rpc", func(t *testing.T) {
		body := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
		payloads, err := p.ParseRequest(context.Background(), nil, body, domain.RPCTypeJSONRPC)
		if err != nil {
			t.Fatalf("json-rpc refused: %v", err)
		}
		if len(payloads) != 1 {
			t.Fatalf("got %d payloads, want 1", len(payloads))
		}
		// The method matters: it is what drives height extraction and caching.
		if got := payloads[0].Method(); got != "eth_blockNumber" {
			t.Errorf("method = %q, want eth_blockNumber", got)
		}
	})

	// The passthrough also extracts a method best-effort, so a well-formed
	// body cannot tell the two parsers apart. A malformed one can: EVM
	// VALIDATES the envelope and refuses, the passthrough shrugs and carries
	// it. A JSON-RPC request with no method is a client error and TRON should
	// answer it as one rather than spend a relay discovering it.
	t.Run("a malformed json-rpc body is refused, not shrugged at", func(t *testing.T) {
		body := []byte(`{"jsonrpc":"2.0","params":[],"id":1}`)
		if _, err := p.ParseRequest(context.Background(), nil, body, domain.RPCTypeJSONRPC); err == nil {
			t.Error("accepted a JSON-RPC body with no method; that is the passthrough parser, not EVM's")
		}
	})

	t.Run("a TRON REST body is carried through, not refused", func(t *testing.T) {
		body := []byte(`{"address":"TBMQ...","visible":true}`)
		payloads, err := p.ParseRequest(context.Background(), nil, body, domain.RPCTypeREST)
		if err != nil {
			t.Fatalf("REST refused: %v — this is the whole bug this plugin exists to avoid", err)
		}
		if len(payloads) != 1 {
			t.Fatalf("got %d payloads, want 1", len(payloads))
		}
		if got := payloads[0].RPCType(); got != domain.RPCTypeREST {
			t.Errorf("rpc type = %q, want rest", got)
		}
	})
}

// A REST body that is not JSON-RPC at all must not be pushed through the EVM
// parser, which would reject it for having no method.
func TestParseRequest_RESTNeedsNoJSONRPCEnvelope(t *testing.T) {
	p := newTestPlugin(t)
	for _, body := range [][]byte{
		[]byte(`{"address":"TBMQ...","visible":true}`),
		[]byte(``),
		[]byte(`not json at all`),
	} {
		if _, err := p.ParseRequest(context.Background(), nil, body, domain.RPCTypeREST); err != nil {
			t.Errorf("REST body %q refused: %v", body, err)
		}
	}
}

// Everything except ParseRequest is EVM's, and the interfaces that carry QoS
// have to survive the composition — a plugin that lost its HealthChecker would
// silently go back to having no probes, which is the state this replaces.
func TestPlugin_KeepsTheEVMExtensionInterfaces(t *testing.T) {
	var p any = newTestPlugin(t)

	if _, ok := p.(qos.HealthChecker); !ok {
		t.Error("not a HealthChecker: TRON would have no probes, which is what it had before")
	}
	if _, ok := p.(qos.DataExtractor); !ok {
		t.Error("not a DataExtractor: no block heights, so the height filter would never receive one")
	}
	if _, ok := p.(qos.BlockHeightTracker); !ok {
		t.Error("not a BlockHeightTracker")
	}
	if _, ok := p.(qos.ChainViewer); !ok {
		t.Error("not a ChainViewer: no chain view for TRON")
	}
	if _, ok := p.(qos.HeightObserver); !ok {
		t.Error("not a HeightObserver")
	}
}

// The health checks are EVM's, and ops verified both against live TRON
// suppliers on 2026-09-04: eth_blockNumber returned a moving height and
// eth_chainId returned 0x2b6653dc.
func TestPlugin_HealthChecksAreTheVerifiedOnes(t *testing.T) {
	checks := newTestPlugin(t).HealthChecks()
	if len(checks) != 2 {
		t.Fatalf("got %d checks, want 2", len(checks))
	}
	names := map[string]bool{}
	essential := 0
	for _, c := range checks {
		names[c.Name] = true
		if c.Essential {
			essential++
		}
	}
	for _, want := range []string{"eth_blockNumber", "eth_chainId"} {
		if !names[want] {
			t.Errorf("missing the %s check; ops verified it works against TRON", want)
		}
	}
	if essential != 1 {
		t.Errorf("%d essential checks, want exactly the height one", essential)
	}
}

// Height extraction is EVM's, which is what makes sync_allowance mean
// something for TRON — on the passthrough it meant nothing, because nothing
// ever produced a height for the filter to use.
func TestPlugin_ExtractsHeightFromJSONRPC(t *testing.T) {
	p := newTestPlugin(t)
	const ep = domain.EndpointAddr("supA-https://tron.example.com")

	data, err := p.ExtractData(ep,
		[]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`),
		[]byte(`{"jsonrpc":"2.0","result":"0x51f83ce","id":1}`),
	)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if data.BlockHeight == nil {
		t.Fatal("no block height extracted from eth_blockNumber")
	}
	if *data.BlockHeight != 0x51f83ce {
		t.Errorf("height = %d, want %d", *data.BlockHeight, 0x51f83ce)
	}
}
