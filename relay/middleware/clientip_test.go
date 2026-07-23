package middleware

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/pokt-network/sage/relay"
)

func mustPrefixes(t *testing.T, cidrs ...string) []netip.Prefix {
	t.Helper()
	p, err := ParseTrustedProxies(cidrs)
	if err != nil {
		t.Fatalf("ParseTrustedProxies(%v): %v", cidrs, err)
	}
	return p
}

func TestResolveClientIP(t *testing.T) {
	tests := []struct {
		name      string
		peer      string
		forwarded []string
		trusted   []string
		want      string
	}{
		{
			name: "direct client, no proxies trusted, is itself",
			peer: "203.0.113.7:44321",
			want: "203.0.113.7",
		},
		{
			name:      "forwarded header from an untrusted peer is ignored",
			peer:      "203.0.113.7:44321",
			forwarded: []string{"1.2.3.4"},
			want:      "203.0.113.7",
		},
		{
			name:      "trusted proxy: the forwarded client is used",
			peer:      "10.0.0.1:5000",
			forwarded: []string{"203.0.113.7"},
			trusted:   []string{"10.0.0.0/8"},
			want:      "203.0.113.7",
		},
		{
			name:      "trusted proxy: rightmost untrusted entry wins over trusted tail",
			peer:      "10.0.0.1:5000",
			forwarded: []string{"203.0.113.7, 10.9.9.9"},
			trusted:   []string{"10.0.0.0/8"},
			want:      "203.0.113.7",
		},
		{
			name:      "spoof attempt: client prepends a fake, real client still found",
			peer:      "10.0.0.1:5000",
			forwarded: []string{"9.9.9.9, 203.0.113.7"},
			trusted:   []string{"10.0.0.0/8"},
			// The rightmost untrusted entry is the one our trusted hop saw; the
			// leftmost is whatever the client wrote and must not win.
			want: "203.0.113.7",
		},
		{
			name:      "multiple X-Forwarded-For headers are walked right-to-left",
			peer:      "10.0.0.1:5000",
			forwarded: []string{"203.0.113.7", "10.9.9.9"},
			trusted:   []string{"10.0.0.0/8"},
			want:      "203.0.113.7",
		},
		{
			name:      "trusted proxy but only trusted addresses forwarded: falls back to peer",
			peer:      "10.0.0.1:5000",
			forwarded: []string{"10.9.9.9, 10.8.8.8"},
			trusted:   []string{"10.0.0.0/8"},
			want:      "10.0.0.1",
		},
		{
			name:    "trusted proxy, no forwarded header: peer",
			peer:    "10.0.0.1:5000",
			trusted: []string{"10.0.0.0/8"},
			want:    "10.0.0.1",
		},
		{
			name:      "malformed forwarded entries are skipped",
			peer:      "10.0.0.1:5000",
			forwarded: []string{"garbage, , 203.0.113.7"},
			trusted:   []string{"10.0.0.0/8"},
			want:      "203.0.113.7",
		},
		{
			name:      "ipv6 client through a trusted proxy",
			peer:      "10.0.0.1:5000",
			forwarded: []string{"2001:db8::1"},
			trusted:   []string{"10.0.0.0/8"},
			want:      "2001:db8::1",
		},
		{
			name: "ipv4-mapped ipv6 peer is normalised to ipv4",
			peer: "[::ffff:203.0.113.7]:44321",
			want: "203.0.113.7",
		},
		{
			name: "bare peer without a port",
			peer: "203.0.113.7",
			want: "203.0.113.7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveClientIP(tt.peer, tt.forwarded, mustPrefixes(t, tt.trusted...))
			if got.String() != tt.want {
				t.Errorf("resolveClientIP(%q, %v, %v) = %q, want %q",
					tt.peer, tt.forwarded, tt.trusted, got.String(), tt.want)
			}
		})
	}
}

func TestResolveClientIP_UnparseablePeerIsInvalid(t *testing.T) {
	got := resolveClientIP("not-an-address", nil, nil)
	if got.IsValid() {
		t.Errorf("resolveClientIP with an unparseable peer = %q, want the invalid zero Addr", got)
	}
}

// TestClientIP_SetsContextField checks the middleware wires the resolver to
// ctx.ClientIP — the field every downstream module reads.
func TestClientIP_SetsContextField(t *testing.T) {
	mw := ClientIP(mustPrefixes(t, "10.0.0.0/8"))
	var seen netip.Addr
	h := mw(relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.ClientIP
		return nil
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1", nil)
	req.RemoteAddr = "10.0.0.1:5000"
	req.Header.Set("X-Forwarded-For", "203.0.113.7")
	ctx := relay.NewContext(req.Context(), req, nil, nil)

	if err := h.HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	if seen.String() != "203.0.113.7" {
		t.Errorf("ctx.ClientIP = %q, want the forwarded client 203.0.113.7", seen.String())
	}
}

func TestParseTrustedProxies(t *testing.T) {
	if _, err := ParseTrustedProxies([]string{"10.0.0.0/8", "192.168.0.0/16"}); err != nil {
		t.Errorf("valid CIDRs errored: %v", err)
	}
	if got, _ := ParseTrustedProxies(nil); got != nil {
		t.Errorf("empty input = %v, want nil", got)
	}
	if _, err := ParseTrustedProxies([]string{"10.0.0.0/8", "not-a-cidr"}); err == nil {
		t.Error("expected an error naming the bad CIDR, got nil")
	}
	// A bare IP is not a CIDR — reject it rather than silently trust a /32 the
	// operator did not write.
	if _, err := ParseTrustedProxies([]string{"10.0.0.1"}); err == nil {
		t.Error("expected a bare IP without a prefix to error")
	}
}
