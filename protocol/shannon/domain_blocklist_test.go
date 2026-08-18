package shannon

import (
	"testing"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

func mustBlocklist(t *testing.T, entries ...config.BlockedDomain) *domainBlocklist {
	t.Helper()
	b, err := newDomainBlocklist(entries)
	if err != nil {
		t.Fatalf("newDomainBlocklist: %v", err)
	}
	return b
}

func TestDomainBlocklist_NilBlocksNothing(t *testing.T) {
	b, err := newDomainBlocklist(nil)
	if err != nil {
		t.Fatalf("newDomainBlocklist: %v", err)
	}
	if b != nil {
		t.Fatalf("empty config should compile to a nil blocklist, got %v", b)
	}
	if b.IsBlocked("https://node.example.com", domain.RPCTypeJSONRPC) {
		t.Error("a nil blocklist must block nothing")
	}
	if got := b.entries(); got != nil {
		t.Errorf("entries on nil = %v, want nil", got)
	}
}

func TestDomainBlocklist_Matching(t *testing.T) {
	b := mustBlocklist(t,
		config.BlockedDomain{Domain: "op-alpha.example"},
		config.BlockedDomain{Domain: "s019.op-beta.example", RPCTypes: []string{"websocket"}},
	)

	cases := []struct {
		name    string
		url     string
		rpcType domain.RPCType
		want    bool
	}{
		{"registrable domain bans every host under it", "https://rpc-1.op-alpha.example", domain.RPCTypeJSONRPC, true},
		{"registrable domain bans every rpc type", "https://rpc-1.op-alpha.example", domain.RPCTypeWebSocket, true},
		{"the domain itself is banned", "https://op-alpha.example:8545/v1", domain.RPCTypeREST, true},
		{"case is not significant", "https://RPC-1.OP-ALPHA.example", domain.RPCTypeJSONRPC, true},
		{"exact host entry bans the listed type", "wss://s019.op-beta.example", domain.RPCTypeWebSocket, true},
		{"exact host entry leaves other types alone", "https://s019.op-beta.example", domain.RPCTypeJSONRPC, false},
		{"a sibling host is not covered by an exact entry", "wss://s020.op-beta.example", domain.RPCTypeWebSocket, false},
		{"an unrelated domain is untouched", "https://node.example.org", domain.RPCTypeJSONRPC, false},
		{"an empty URL is not a match", "", domain.RPCTypeJSONRPC, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := b.IsBlocked(tc.url, tc.rpcType); got != tc.want {
				t.Errorf("IsBlocked(%q, %s) = %v, want %v", tc.url, tc.rpcType, got, tc.want)
			}
		})
	}
}

// The memoized path must agree with the computed one — the cache is keyed on
// the raw URL, and a stale or wrong entry would silently un-ban a domain.
func TestDomainBlocklist_CachedDecisionMatchesFirst(t *testing.T) {
	b := mustBlocklist(t, config.BlockedDomain{Domain: "op-alpha.example"})

	const url = "https://rpc-1.op-alpha.example"
	first := b.IsBlocked(url, domain.RPCTypeJSONRPC)
	second := b.IsBlocked(url, domain.RPCTypeJSONRPC)
	if !first || !second {
		t.Fatalf("IsBlocked = %v then %v, want true both times", first, second)
	}
}

// An all-types entry must absorb a narrower one regardless of the order they
// appear in, or a config listing both would end up banning less than either.
func TestDomainBlocklist_AllTypesAbsorbsNarrowerEntry(t *testing.T) {
	orders := [][]config.BlockedDomain{
		{{Domain: "op.example", RPCTypes: []string{"websocket"}}, {Domain: "op.example"}},
		{{Domain: "op.example"}, {Domain: "op.example", RPCTypes: []string{"websocket"}}},
	}
	for _, entries := range orders {
		b := mustBlocklist(t, entries...)
		for _, rpcType := range domain.AllRPCTypes() {
			if !b.IsBlocked("https://op.example", rpcType) {
				t.Errorf("entries %v: %s not blocked", entries, rpcType)
			}
		}
	}
}

func TestDomainBlocklist_MalformedEntryRefusesToCompile(t *testing.T) {
	cases := []struct {
		name  string
		entry config.BlockedDomain
	}{
		{"empty domain", config.BlockedDomain{Domain: "  "}},
		{"unknown rpc type", config.BlockedDomain{Domain: "op.example", RPCTypes: []string{"websockets"}}},
		{"unknown is not bannable", config.BlockedDomain{Domain: "op.example", RPCTypes: []string{"unknown"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newDomainBlocklist([]config.BlockedDomain{tc.entry}); err == nil {
				t.Error("expected an error — a ban that silently covers less than it reads as covering is worse than none")
			}
		})
	}
}

func TestDomainBlocklist_EnvUnionsWithConfig(t *testing.T) {
	t.Setenv(envBlockedDomains, "op-beta.example:websocket|json_rpc, evil.example ,")

	b := mustBlocklist(t, config.BlockedDomain{Domain: "op-alpha.example"})

	cases := []struct {
		url     string
		rpcType domain.RPCType
		want    bool
	}{
		{"https://rpc.op-alpha.example", domain.RPCTypeJSONRPC, true}, // config entry survives
		{"wss://rpc.op-beta.example", domain.RPCTypeWebSocket, true},  // env entry, listed type
		{"https://rpc.op-beta.example", domain.RPCTypeREST, false},    // env entry, unlisted type
		{"https://rpc.evil.example", domain.RPCTypeGRPC, true},        // env entry, no types = all
	}
	for _, tc := range cases {
		if got := b.IsBlocked(tc.url, tc.rpcType); got != tc.want {
			t.Errorf("IsBlocked(%q, %s) = %v, want %v", tc.url, tc.rpcType, got, tc.want)
		}
	}
}

// The env var widens a ban and can never narrow one.
func TestDomainBlocklist_EnvCannotNarrowAConfigBan(t *testing.T) {
	t.Setenv(envBlockedDomains, "op-alpha.example:websocket")

	b := mustBlocklist(t, config.BlockedDomain{Domain: "op-alpha.example"})

	if !b.IsBlocked("https://rpc.op-alpha.example", domain.RPCTypeJSONRPC) {
		t.Error("a narrower env entry must not un-ban an RPC type the config banned")
	}
}

func TestDomainBlocklist_Entries(t *testing.T) {
	b := mustBlocklist(t,
		config.BlockedDomain{Domain: "b.example"},
		config.BlockedDomain{Domain: "a.example", RPCTypes: []string{"websocket", "rest"}},
	)

	got := b.entries()
	want := [][2]string{
		{"a.example", "rest"},
		{"a.example", "websocket"},
		{"b.example", "all"},
	}
	if len(got) != len(want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entries[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}
