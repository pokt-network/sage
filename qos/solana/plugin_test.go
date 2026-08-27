package solana_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos/solana"
)

// --- ParseRequest --- //

func TestParseRequest_ValidJSONRPC(t *testing.T) {
	p := solana.NewPlugin(nil, 10)
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}`)
	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewReader(body))

	payloads, err := p.ParseRequest(context.Background(), req, body, domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}
	if payloads[0].Method() != "getSlot" {
		t.Errorf("expected method getSlot, got %q", payloads[0].Method())
	}
	if payloads[0].RPCType() != domain.RPCTypeJSONRPC {
		t.Errorf("expected RPCType json_rpc, got %q", payloads[0].RPCType())
	}
}

func TestParseRequest_MissingMethod(t *testing.T) {
	p := solana.NewPlugin(nil, 10)
	body := []byte(`{"jsonrpc":"2.0","id":1,"params":[]}`)
	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewReader(body))

	_, err := p.ParseRequest(context.Background(), req, body, domain.RPCTypeJSONRPC)
	if err == nil {
		t.Fatal("expected error for missing method, got nil")
	}
}

func TestParseRequest_NilBody(t *testing.T) {
	p := solana.NewPlugin(nil, 10)
	req, _ := http.NewRequest(http.MethodPost, "/", nil)

	_, err := p.ParseRequest(context.Background(), req, nil, domain.RPCTypeJSONRPC)
	if err == nil {
		t.Fatal("expected error for nil body, got nil")
	}
}

func TestParseRequest_EmptyBody(t *testing.T) {
	p := solana.NewPlugin(nil, 10)
	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte{}))

	_, err := p.ParseRequest(context.Background(), req, []byte{}, domain.RPCTypeJSONRPC)
	if err == nil {
		t.Fatal("expected error for empty body, got nil")
	}
}

// --- ParseBlockHeight --- //

func TestParseBlockHeight_BlockHeightField(t *testing.T) {
	p := solana.NewPlugin(nil, 10)
	resp := []byte(`{"jsonrpc":"2.0","id":1,"result":{"blockHeight":123456,"absoluteSlot":200000}}`)

	h, err := p.ParseBlockHeight(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h != 123456 {
		t.Errorf("expected 123456, got %d", h)
	}
}

// A slot is not a block height — absoluteSlot runs ahead of blockHeight by the
// number of skipped slots, so accepting it as a height poisons the perceived
// height that every other endpoint is compared against.
func TestParseBlockHeight_AbsoluteSlotIsNotAHeight(t *testing.T) {
	p := solana.NewPlugin(nil, 10)
	// blockHeight missing, only absoluteSlot present
	resp := []byte(`{"jsonrpc":"2.0","id":1,"result":{"absoluteSlot":99999}}`)

	if _, err := p.ParseBlockHeight(resp); err == nil {
		t.Fatal("expected error when only absoluteSlot is present, got nil")
	}
}

func TestParseBlockHeight_NoHeightData(t *testing.T) {
	p := solana.NewPlugin(nil, 10)
	resp := []byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`)

	_, err := p.ParseBlockHeight(resp)
	if err == nil {
		t.Fatal("expected error when no height data, got nil")
	}
}

// --- IsCoalescable --- //

func TestIsCoalescable_ReadOnlyMethods(t *testing.T) {
	p := solana.NewPlugin(nil, 10)
	readOnly := []string{"getSlot", "getEpochInfo", "getRecentBlockhash", "getBlockHeight"}
	for _, m := range readOnly {
		if !p.IsCoalescable(m) {
			t.Errorf("expected %q to be coalescable", m)
		}
	}
}

func TestIsCoalescable_WriteMethods(t *testing.T) {
	p := solana.NewPlugin(nil, 10)
	write := []string{"sendTransaction", "simulateTransaction", "requestAirdrop"}
	for _, m := range write {
		if p.IsCoalescable(m) {
			t.Errorf("expected %q to NOT be coalescable", m)
		}
	}
}

// --- SelectEndpoints --- //

func TestSelectEndpoints_BlockHeightFiltering(t *testing.T) {
	p := solana.NewPlugin(nil, 5)

	ep1 := domain.EndpointAddr("supplier1-https://rpc1.example.com")
	ep2 := domain.EndpointAddr("supplier2-https://rpc2.example.com")
	ep3 := domain.EndpointAddr("supplier3-https://rpc3.example.com")

	// ep1 is well synced, ep2 is too far behind, ep3 is unknown (no data).
	p.UpdateBlockHeight(ep1, 1000)
	p.UpdateBlockHeight(ep2, 900) // perceived ~1000, allowance 5 → min 995; 900 < 995 → filtered

	endpoints := domain.EndpointAddrList{ep1, ep2, ep3}
	result, err := p.SelectEndpoints(endpoints, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ep1 should pass (1000 >= 995), ep2 should be filtered, ep3 passes (unknown → allowed through).
	if !result.Contains(ep1) {
		t.Error("ep1 (height 1000) should be included")
	}
	if result.Contains(ep2) {
		t.Error("ep2 (height 900) should be filtered out")
	}
	if !result.Contains(ep3) {
		t.Error("ep3 (unknown) should be allowed through")
	}
}

func TestSelectEndpoints_AllEndpointsReturned_WhenNoData(t *testing.T) {
	p := solana.NewPlugin(nil, 10)
	ep1 := domain.EndpointAddr("s1-https://a.example.com")
	ep2 := domain.EndpointAddr("s2-https://b.example.com")

	endpoints := domain.EndpointAddrList{ep1, ep2}
	result, err := p.SelectEndpoints(endpoints, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// No height data → perceived is 0 → all endpoints pass.
	if len(result) != 2 {
		t.Errorf("expected 2 endpoints, got %d", len(result))
	}
}

// --- PerceivedBlockHeight --- //

func TestPerceivedBlockHeight_UpdatesWithObservations(t *testing.T) {
	p := solana.NewPlugin(nil, 10)

	p.UpdateBlockHeight("s1-https://a.example.com", 500)
	p.UpdateBlockHeight("s2-https://b.example.com", 510)
	p.UpdateBlockHeight("s3-https://c.example.com", 505)

	perceived := p.PerceivedBlockHeight()
	if perceived == 0 {
		t.Error("perceived block height should be non-zero after updates")
	}
}

// An unconfigured service must still filter on block height. Asserted through
// SelectEndpoints rather than on the plugin's field: the field being set proves
// nothing about whether selection reads it.
func TestSelectEndpoints_UnconfiguredSyncAllowanceStillFilters(t *testing.T) {
	p := solana.NewPlugin(nil, 0) // no sync_allowance in config

	const tip = 300_000_000

	fresh := domain.EndpointAddr("pokt1fresh-https://fresh.example.com")
	other := domain.EndpointAddr("pokt1other-https://other.example.com")
	stale := domain.EndpointAddr("pokt1stale-https://stale.example.com")

	// Two fresh endpoints so the perceived height is theirs and the stale one
	// cannot drag the median.
	p.UpdateBlockHeight(fresh, tip)
	p.UpdateBlockHeight(other, tip)
	// Far past the default allowance — roughly an hour behind.
	p.UpdateBlockHeight(stale, tip-500_000)

	got, err := p.SelectEndpoints(domain.EndpointAddrList{fresh, other, stale}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Contains(stale) {
		t.Errorf("stale endpoint selected: an unset sync_allowance must fall back to the Solana default, not disable the check (got %v)", got)
	}
	if !got.Contains(fresh) {
		t.Errorf("fresh endpoint missing from %v", got)
	}
}

// The fallback must not be so tight that an endpoint refreshed only by health
// checks — always a few blocks behind whatever reported last — leaves the pool.
// That is the starvation loop the generous default exists to avoid.
func TestSelectEndpoints_DefaultSyncAllowanceKeepsTrailingEndpoints(t *testing.T) {
	p := solana.NewPlugin(nil, 0)

	// One block short of the 1500-block default.
	const tip = 300_000_000
	const trailingHeight = tip - 1499

	leader := domain.EndpointAddr("pokt1leader-https://leader.example.com")
	trailing := domain.EndpointAddr("pokt1trailing-https://trailing.example.com")

	p.UpdateBlockHeight(leader, tip)
	p.UpdateBlockHeight(trailing, trailingHeight)

	got, err := p.SelectEndpoints(domain.EndpointAddrList{leader, trailing}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.Contains(trailing) {
		t.Errorf("endpoint inside the allowance was filtered out: %v", got)
	}
}

// An operator can configure health checks (active_health_checks.local), and a
// getBlockHeight probe answers with a bare number rather than the getEpochInfo
// object. Before this it parsed as nothing: the check ran, passed, and fed the
// staleness filter no height at all.
func TestExtractData_AcceptsGetBlockHeightResponse(t *testing.T) {
	p := solana.NewPlugin(nil, 100)

	req := []byte(`{"jsonrpc":"2.0","id":1,"method":"getBlockHeight"}`)
	resp := []byte(`{"jsonrpc":"2.0","id":1,"result":300000000}`)

	data, err := p.ExtractData("pokt1a-https://a.example.com", req, resp)
	if err != nil {
		t.Fatalf("ExtractData: %v", err)
	}
	if data.BlockHeight == nil {
		t.Fatal("no block height extracted from a getBlockHeight response")
	}
	if *data.BlockHeight != 300000000 {
		t.Errorf("height = %d, want 300000000", *data.BlockHeight)
	}
}

// The gate is the point. getSlot also answers with a bare number, and a slot is
// not a height — Solana skips slots, so absoluteSlot runs tens of millions
// ahead. Accepting any bare number would poison the perceived-height consensus
// exactly the way the absoluteSlot fallback did.
func TestExtractData_RejectsBareNumberFromOtherMethods(t *testing.T) {
	p := solana.NewPlugin(nil, 100)

	for _, method := range []string{"getSlot", "getTransactionCount", ""} {
		req := []byte(`{"jsonrpc":"2.0","id":1,"method":"` + method + `"}`)
		resp := []byte(`{"jsonrpc":"2.0","id":1,"result":390000000}`)

		data, err := p.ExtractData("pokt1a-https://a.example.com", req, resp)
		if err != nil {
			t.Fatalf("ExtractData(%q): %v", method, err)
		}
		if data.BlockHeight != nil {
			t.Errorf("method %q: a bare number was taken as a block height (%d)", method, *data.BlockHeight)
		}
	}
}

// getEpochInfo keeps working, and absoluteSlot is still not a fallback.
func TestExtractData_EpochInfoUnchanged(t *testing.T) {
	p := solana.NewPlugin(nil, 100)
	req := []byte(`{"jsonrpc":"2.0","id":1,"method":"getEpochInfo"}`)

	withHeight := []byte(`{"result":{"absoluteSlot":390000000,"blockHeight":300000000}}`)
	data, err := p.ExtractData("pokt1a-https://a.example.com", req, withHeight)
	if err != nil {
		t.Fatalf("ExtractData: %v", err)
	}
	if data.BlockHeight == nil || *data.BlockHeight != 300000000 {
		t.Errorf("blockHeight not preferred over absoluteSlot: %+v", data.BlockHeight)
	}

	slotOnly := []byte(`{"result":{"absoluteSlot":390000000}}`)
	data, err = p.ExtractData("pokt1a-https://a.example.com", req, slotOnly)
	if err != nil {
		t.Fatalf("ExtractData: %v", err)
	}
	if data.BlockHeight != nil {
		t.Errorf("absoluteSlot was accepted as a height (%d)", *data.BlockHeight)
	}
}

// PATH lost this property and halved its selectable pool with it (their
// 5c1c8d60): once health checks began storing block heights, an endpoint that
// had been probed for height but not yet for health went from ABSENT — and
// therefore waved through — to present-but-incomplete, which their validator
// treated as fatal. Learning more about an endpoint made it less selectable.
//
// SAGE cannot reach that state through the same door, because selection filters
// on measurements alone and has no completeness requirement. This pins it:
// SAGE runs two probes too (getEpochInfo and getHealth), they land at different
// moments, and every endpoint passes through the gap after every restart.
func TestSelectEndpoints_PartiallyProbedEndpointStaysSelectable(t *testing.T) {
	p := solana.NewPlugin(nil, 100)

	const tip = 300_000_000
	probed := domain.EndpointAddr("pokt1probed-https://probed.example.com")
	healthOnly := domain.EndpointAddr("pokt1health-https://health.example.com")

	// probed answered getBlockHeight; healthOnly answered only getHealth, so
	// nothing about its height is known yet.
	p.UpdateBlockHeight(probed, tip)
	if _, err := p.ExtractData(healthOnly, []byte(`{"method":"getHealth"}`), []byte(`{"result":"ok"}`)); err != nil {
		t.Fatalf("a getHealth response should not error: %v", err)
	}

	got, err := p.SelectEndpoints(domain.EndpointAddrList{probed, healthOnly}, nil)
	if err != nil {
		t.Fatalf("SelectEndpoints: %v", err)
	}
	if !got.Contains(healthOnly) {
		t.Errorf("an endpoint with no height observation was dropped: %v — absence of a measurement is not evidence of staleness", got)
	}
	if !got.Contains(probed) {
		t.Errorf("the fully probed endpoint was dropped: %v", got)
	}
}

// The opposite direction: a height that IS measured and is far behind must
// still narrow the pool, or the filter does nothing.
func TestSelectEndpoints_MeasuredStalenessStillFilters(t *testing.T) {
	p := solana.NewPlugin(nil, 100)

	const tip = 300_000_000
	fresh := domain.EndpointAddr("pokt1fresh-https://fresh.example.com")
	other := domain.EndpointAddr("pokt1other-https://other.example.com")
	stale := domain.EndpointAddr("pokt1stale-https://stale.example.com")

	p.UpdateBlockHeight(fresh, tip)
	p.UpdateBlockHeight(other, tip)
	p.UpdateBlockHeight(stale, tip-50_000)

	got, err := p.SelectEndpoints(domain.EndpointAddrList{fresh, other, stale}, nil)
	if err != nil {
		t.Fatalf("SelectEndpoints: %v", err)
	}
	if got.Contains(stale) {
		t.Errorf("a measurably stale endpoint survived the filter: %v", got)
	}
}

// TestResetState pins what an operator-triggered chain-state reset discards:
// the perceived block height and the per-endpoint QoS store.
func TestResetState(t *testing.T) {
	p := solana.NewPlugin(nil, 100)

	addrs := domain.EndpointAddrList{"a", "b"}
	p.UpdateBlockHeight("a", 300_000_000)
	p.UpdateBlockHeight("b", 1) // far behind; would be filtered pre-reset

	if got := p.PerceivedBlockHeight(); got == 0 {
		t.Fatalf("expected nonzero perceived block height before reset, got %d", got)
	}

	p.ResetState()

	if got := p.PerceivedBlockHeight(); got != 0 {
		t.Fatalf("PerceivedBlockHeight() = %d after ResetState, want 0", got)
	}

	selected, err := p.SelectEndpoints(addrs, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(selected) != len(addrs) {
		t.Fatalf("SelectEndpoints after ResetState = %v, want every endpoint to pass (%v)", selected, addrs)
	}
}
