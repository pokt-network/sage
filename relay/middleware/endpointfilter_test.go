package middleware

import (
	"testing"

	"github.com/pokt-network/sage/domain"
)

// Hedge's Clone is shallow: the primary arm and the parent share one backing
// array. A filter that compacts in place is a write on one arm racing the
// other's read. The helper must leave the input untouched.
func TestFilterEndpoints_NeverMutatesInput(t *testing.T) {
	eps := testEndpoints(4)
	before := append(domain.EndpointAddrList(nil), eps...)

	out := filterEndpoints(eps, func(ep domain.EndpointAddr) bool { return ep != eps[1] })

	for i := range before {
		if eps[i] != before[i] {
			t.Fatalf("input mutated at %d: %v -> %v", i, before[i], eps[i])
		}
	}
	if len(out) != 3 || out[0] != eps[0] || out[1] != eps[2] || out[2] != eps[3] {
		t.Fatalf("filtered = %v", out)
	}
}

func TestFilterEndpoints_NoRemovalReturnsSameSlice(t *testing.T) {
	eps := testEndpoints(3)
	out := filterEndpoints(eps, func(domain.EndpointAddr) bool { return true })
	if &out[0] != &eps[0] {
		t.Fatal("nothing removed must not allocate")
	}
}
