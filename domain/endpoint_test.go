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
