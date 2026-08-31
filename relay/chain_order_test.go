package relay

import (
	"strings"
	"testing"
)

// canonicalOrder is the production chain. It is DefaultChainOrder itself rather
// than a copy of it: this file used to keep its own hand-synced list, which
// could agree with itself while disagreeing with what the gateway actually ran.
var canonicalOrder = DefaultChainOrder()

func TestValidateChainOrder_Canonical(t *testing.T) {
	if err := ValidateChainOrder(canonicalOrder); err != nil {
		t.Fatalf("canonical order should validate, got: %v", err)
	}
}

// TestDefaultChainOrder_ReturnsFreshSlice guards the promise in the doc comment:
// a caller that mutates the returned slice must not affect the next caller.
func TestDefaultChainOrder_ReturnsFreshSlice(t *testing.T) {
	first := DefaultChainOrder()
	first[0] = "clobbered"

	if second := DefaultChainOrder(); second[0] != MWShadow {
		t.Errorf("DefaultChainOrder()[0] = %q after a caller mutated an earlier result, want %q",
			second[0], MWShadow)
	}
}

func TestValidateChainOrder_SendRelayNotLast(t *testing.T) {
	order := append([]string{}, canonicalOrder...)
	order[len(order)-1], order[len(order)-2] = order[len(order)-2], order[len(order)-1]
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "send_relay") {
		t.Errorf("expected send_relay-not-last error, got %v", err)
	}
}

func TestValidateChainOrder_SelectEndpointBeforeRetry(t *testing.T) {
	order := []string{MWParse, MWSelectEndpoint, MWRetry, MWSendRelay}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "retry") || !strings.Contains(err.Error(), "select_endpoint") {
		t.Errorf("expected retry→select_endpoint violation, got %v", err)
	}
}

func TestValidateChainOrder_SelectEndpointBeforeHedge(t *testing.T) {
	order := []string{MWParse, MWSelectEndpoint, MWHedge, MWSendRelay}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "hedge") {
		t.Errorf("expected hedge→select_endpoint violation, got %v", err)
	}
}

func TestValidateChainOrder_ParseAfterValidate(t *testing.T) {
	order := []string{MWValidate, MWParse, MWSendRelay}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "parse") || !strings.Contains(err.Error(), "validate") {
		t.Errorf("expected parse→validate violation, got %v", err)
	}
}

func TestValidateChainOrder_CircuitBreakAfterSelectEndpoint(t *testing.T) {
	order := []string{MWParse, MWSelectEndpoint, MWCircuitBreak, MWSendRelay}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "circuit_break") {
		t.Errorf("expected circuit_break→select_endpoint violation, got %v", err)
	}
}

func TestValidateChainOrder_HeuristicAfterSendRelay(t *testing.T) {
	// Cannot literally put heuristic after send_relay because send_relay-last
	// invariant trips first; verify that rule is enforced.
	order := []string{MWParse, MWSendRelay, MWHeuristic}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "send_relay") {
		t.Errorf("expected send_relay-not-last error, got %v", err)
	}
}

func TestValidateChainOrder_Duplicates(t *testing.T) {
	order := []string{MWParse, MWRetry, MWRetry, MWSelectEndpoint, MWSendRelay}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected duplicate error, got %v", err)
	}
}

func TestValidateChainOrder_PartialChainOK(t *testing.T) {
	// A subset with only the mandatory precedence pairs should validate.
	order := []string{MWParse, MWRetry, MWSelectEndpoint, MWSendRelay}
	if err := ValidateChainOrder(order); err != nil {
		t.Errorf("partial chain should validate, got: %v", err)
	}
}

// TestValidateChainOrder_UnknownNamesAreAbsent pins the documented behaviour a
// third-party middleware depends on: a name this package has never heard of is
// treated as absent, so registering "rate_limit" does not have to be taught to
// chain_order.go before the chain will build. The flip side, also pinned here,
// is that such a name gets no ordering protection — the rules only constrain
// names that appear in a mustPrecede pair.
func TestValidateChainOrder_UnknownNamesAreAbsent(t *testing.T) {
	order := []string{"rate_limit", MWParse, MWRetry, MWSelectEndpoint, MWSendRelay}
	if err := ValidateChainOrder(order); err != nil {
		t.Errorf("an unknown middleware name should validate as absent, got: %v", err)
	}
}

func TestValidateChainOrder_MethodBlocksAfterSelectEndpoint(t *testing.T) {
	order := []string{MWParse, MWHedge, MWSelectEndpoint, MWMethodBlocks, MWSendRelay}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "method_blocks") {
		t.Errorf("expected method_blocks→select_endpoint violation, got %v", err)
	}
}

func TestValidateChainOrder_MethodBlocksOutsideHedge(t *testing.T) {
	order := []string{MWParse, MWMethodBlocks, MWHedge, MWSelectEndpoint, MWSendRelay}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "method_blocks") {
		t.Errorf("expected hedge→method_blocks violation, got %v", err)
	}
}

// TestValidateChainOrder_ScorePosition pins where score sits: inside
// select_endpoint (it reads the endpoint that attempt picked) and outside
// retry (one attempt is one scoring event, so every retry must pass through
// it).
func TestValidateChainOrder_ScorePosition(t *testing.T) {
	if err := ValidateChainOrder(DefaultChainOrder()); err != nil {
		t.Fatalf("canonical order should validate with score in it, got: %v", err)
	}
	var present bool
	for _, n := range DefaultChainOrder() {
		if n == MWScore {
			present = true
		}
	}
	if !present {
		t.Fatalf("DefaultChainOrder() must contain %q", MWScore)
	}

	order := []string{MWParse, MWRetry, MWScore, MWSelectEndpoint, MWHeuristic, MWSendRelay}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), `"select_endpoint" must precede "score"`) {
		t.Errorf("expected select_endpoint→score violation, got %v", err)
	}

	order = []string{MWParse, MWScore, MWRetry, MWSelectEndpoint, MWHeuristic, MWSendRelay}
	err = ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), `"retry" must precede "score"`) {
		t.Errorf("expected retry→score violation, got %v", err)
	}
}

// timeout resolves its deadline per service, and the service is what parse
// sets: a timeout that runs before parse resolves the knob for service "", so
// a per-service timeout_config or tuning override never applies. It must still
// wrap the fan-out, or a batch's sub-relays and a retry's later attempts run
// past the deadline.
func TestDefaultChainOrder_TimeoutAfterParseBeforeBatch(t *testing.T) {
	idx := func(name string) int {
		for i, n := range canonicalOrder {
			if n == name {
				return i
			}
		}
		t.Fatalf("%s missing from DefaultChainOrder", name)
		return -1
	}
	if idx(MWTimeout) < idx(MWValidate) {
		t.Errorf("timeout (%d) runs before validate (%d): ctx.ServiceID is not set yet", idx(MWTimeout), idx(MWValidate))
	}
	if idx(MWTimeout) > idx(MWBatch) {
		t.Errorf("timeout (%d) runs inside batch (%d): the deadline would not cover the fan-out", idx(MWTimeout), idx(MWBatch))
	}
}

func TestValidateChainOrder_TimeoutBeforeParseRejected(t *testing.T) {
	order := []string{MWTimeout, MWParse, MWValidate, MWSendRelay}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "timeout") || !strings.Contains(err.Error(), "parse") {
		t.Errorf("expected parse→timeout violation, got %v", err)
	}
}

func TestValidateChainOrder_TimeoutAfterRetryRejected(t *testing.T) {
	order := []string{MWParse, MWValidate, MWRetry, MWTimeout, MWSendRelay}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "timeout") || !strings.Contains(err.Error(), "retry") {
		t.Errorf("expected timeout→retry violation, got %v", err)
	}
}
