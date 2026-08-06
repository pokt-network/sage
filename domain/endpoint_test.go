package domain

import "testing"

func TestEndpointAddr_Supplier(t *testing.T) {
	tests := []struct {
		addr     EndpointAddr
		expected string
	}{
		{"pokt1abc-https://example.com", "pokt1abc"},
		{"pokt1xyz-https://node.test:8080/path", "pokt1xyz"},
		{"nohyphen", "nohyphen"},
	}
	for _, tt := range tests {
		if got := tt.addr.Supplier(); got != tt.expected {
			t.Errorf("EndpointAddr(%q).Supplier() = %q, want %q", tt.addr, got, tt.expected)
		}
	}
}

func TestEndpointAddr_URL(t *testing.T) {
	tests := []struct {
		addr     EndpointAddr
		expected string
		wantErr  bool
	}{
		{"pokt1abc-https://example.com", "https://example.com", false},
		{"pokt1abc-https://node.test:8080/path", "https://node.test:8080/path", false},
		{"nohyphen", "", true},
	}
	for _, tt := range tests {
		got, err := tt.addr.URL()
		if tt.wantErr && err == nil {
			t.Errorf("EndpointAddr(%q).URL() expected error", tt.addr)
		}
		if !tt.wantErr && got != tt.expected {
			t.Errorf("EndpointAddr(%q).URL() = %q, want %q", tt.addr, got, tt.expected)
		}
	}
}

func TestEndpointAddr_Domain(t *testing.T) {
	tests := []struct {
		addr     EndpointAddr
		expected string
	}{
		{"pokt1abc-https://example.com", "example.com"},
		{"pokt1abc-https://node.test:8080/path", "node.test"},
		{"pokt1abc-http://relay01.spacebelt.xyz/", "relay01.spacebelt.xyz"},
		{"nohyphen", ""},
	}
	for _, tt := range tests {
		if got := tt.addr.Domain(); got != tt.expected {
			t.Errorf("EndpointAddr(%q).Domain() = %q, want %q", tt.addr, got, tt.expected)
		}
	}
}

func TestEndpointAddrList_Exclude(t *testing.T) {
	list := EndpointAddrList{"a", "b", "c", "d"}
	excluded := map[EndpointAddr]bool{"b": true, "d": true}
	got := list.Exclude(excluded)
	if len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Errorf("Exclude() = %v, want [a c]", got)
	}
}

func TestEndpointAddrList_Contains(t *testing.T) {
	list := EndpointAddrList{"a", "b", "c"}
	if !list.Contains("b") {
		t.Error("Contains(b) should be true")
	}
	if list.Contains("z") {
		t.Error("Contains(z) should be false")
	}
}

// --- Operator (eTLD+1) --- //

func TestEndpointAddr_Operator(t *testing.T) {
	tests := []struct {
		name string
		addr EndpointAddr
		want string
	}{
		{"plain host", "pokt1abc-https://rpc.example.com/v1", "example.com"},
		{"deep subdomain", "pokt1abc-https://a.b.c.example.com", "example.com"},
		{"multi-part public suffix", "pokt1abc-https://node.example.co.uk", "example.co.uk"},
		{"with port", "pokt1abc-https://rpc.example.com:8545", "example.com"},
		{"apex", "pokt1abc-https://example.com", "example.com"},
		{"IP literal is its own operator", "pokt1abc-http://10.0.0.1:8545", "10.0.0.1"},
		{"single label is its own operator", "pokt1abc-http://localhost:8545", "localhost"},
		{"malformed addr", "noseparatorhere", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.addr.Operator(); got != tt.want {
				t.Errorf("Operator() = %q, want %q", got, tt.want)
			}
		})
	}
}

// The whole point: distinct hostnames run by one provider are one operator.
func TestEndpointAddr_OperatorGroupsSiblingHosts(t *testing.T) {
	a := EndpointAddr("pokt1a-https://rpc-1.example.net")
	b := EndpointAddr("pokt1b-https://rpc-2.example.net")
	c := EndpointAddr("pokt1c-https://rpc.other.net")

	if a.Domain() == b.Domain() {
		t.Fatal("test setup: a and b should be different domains")
	}
	if a.Operator() != b.Operator() {
		t.Errorf("sibling hosts should share an operator: %q vs %q", a.Operator(), b.Operator())
	}
	if a.Operator() == c.Operator() {
		t.Errorf("unrelated hosts should not share an operator: both %q", a.Operator())
	}
}

func TestEndpointAddrList_Operators(t *testing.T) {
	l := EndpointAddrList{
		"pokt1a-https://rpc-1.example.net",
		"pokt1b-https://rpc-2.example.net",
		"pokt1c-https://rpc.other.net",
	}
	got := l.Operators()
	if len(got) != 2 || got[0] != "example.net" || got[1] != "other.net" {
		t.Errorf("Operators() = %v, want [example.net other.net]", got)
	}
}

func TestEndpointAddrList_ExcludeOperators(t *testing.T) {
	l := EndpointAddrList{
		"pokt1a-https://rpc-1.example.net",
		"pokt1b-https://rpc-2.example.net",
		"pokt1c-https://rpc.other.net",
	}

	got := l.ExcludeOperators(map[string]bool{"example.net": true})
	if len(got) != 1 || got[0] != "pokt1c-https://rpc.other.net" {
		t.Errorf("ExcludeOperators() = %v, want the other.net endpoint only", got)
	}

	// Excluding everything is a preference, not a reason to have no endpoint.
	all := l.ExcludeOperators(map[string]bool{"example.net": true, "other.net": true})
	if len(all) != len(l) {
		t.Errorf("excluding every operator should return the input unchanged, got %v", all)
	}

	if got := l.ExcludeOperators(nil); len(got) != len(l) {
		t.Errorf("nil exclusion set should return the input unchanged, got %v", got)
	}
}
