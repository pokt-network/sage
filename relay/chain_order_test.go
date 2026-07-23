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
