package cosmos

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func TestRequestedHeight(t *testing.T) {
	jsonrpc := func(m, body string) domain.Payload { return domain.NewPayload([]byte(body), domain.RPCTypeCometBFT, m) }
	get := func(path string) domain.Payload {
		return domain.NewPayload(nil, domain.RPCTypeCometBFT, "").WithHTTP(path, "GET")
	}
	rest := func(path string) domain.Payload {
		return domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP(path, "GET")
	}
	cases := []struct {
		name string
		p    domain.Payload
		want uint64
		ok   bool
	}{
		{"block, object params, string height", jsonrpc("block", `{"method":"block","params":{"height":"27782"}}`), 27782, true},
		{"block, object params, numeric height", jsonrpc("block", `{"method":"block","params":{"height":27782}}`), 27782, true},
		{"block, positional params", jsonrpc("block", `{"method":"block","params":["84213"]}`), 84213, true},
		{"block, latest (empty params)", jsonrpc("block", `{"method":"block","params":{}}`), 0, false},
		{"block, no params", jsonrpc("block", `{"method":"block"}`), 0, false},
		{"block, height 0 is latest", jsonrpc("block", `{"method":"block","params":{"height":"0"}}`), 0, false},
		{"block_results", jsonrpc("block_results", `{"method":"block_results","params":{"height":"5"}}`), 5, true},
		{"commit", jsonrpc("commit", `{"method":"commit","params":{"height":"6"}}`), 6, true},
		{"validators", jsonrpc("validators", `{"method":"validators","params":{"height":"7","page":"1"}}`), 7, true},
		{"abci_query with height", jsonrpc("abci_query", `{"method":"abci_query","params":{"path":"/x","data":"00","height":"9"}}`), 9, true},
		{"abci_query without height", jsonrpc("abci_query", `{"method":"abci_query","params":{"path":"/x","data":"00"}}`), 0, false},
		{"blockchain range uses minHeight", jsonrpc("blockchain", `{"method":"blockchain","params":{"minHeight":"1","maxHeight":"100"}}`), 1, true},
		{"status names no height", jsonrpc("status", `{"method":"status","params":{}}`), 0, false},
		{"tx names no height", jsonrpc("tx", `{"method":"tx","params":{"hash":"AA"}}`), 0, false},
		{"GET /block?height=", get("/block?height=27782"), 27782, true},
		{"GET /block (latest)", get("/block"), 0, false},
		{"GET /commit?height=", get("/commit?height=12"), 12, true},
		{"GET /blockchain?minHeight=", get("/blockchain?minHeight=3&maxHeight=20"), 3, true},
		{"GET /status", get("/status"), 0, false},
		{"REST blocks/{height}", rest("/cosmos/base/tendermint/v1beta1/blocks/40628"), 40628, true},
		{"REST blocks/latest", rest("/cosmos/base/tendermint/v1beta1/blocks/latest"), 0, false},
		{"REST validatorsets/{height}", rest("/cosmos/base/tendermint/v1beta1/validatorsets/77"), 77, true},
		{"REST txs/block/{height}", rest("/cosmos/tx/v1beta1/txs/block/96568"), 96568, true},
		{"REST balances names no height", rest("/cosmos/bank/v1beta1/balances/cosmos1abc"), 0, false},
		{"nothing at all", domain.NewPayload(nil, domain.RPCTypeREST, ""), 0, false},
	}
	for _, tc := range cases {
		got, ok := requestedHeight(tc.p)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s: got (%d, %v), want (%d, %v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestPrunedLowestHeight(t *testing.T) {
	cases := []struct {
		name string
		body string
		want uint64
		ok   bool
	}{
		{"CometBFT -32603 data", `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":"height 27782 is not available, lowest height is 25052001"}}`, 25052001, true},
		{"REST gateway message", `{"code":2,"message":"height 40628 is not available, lowest height is 25052001","details":[]}`, 25052001, true},
		{"range error names no lowest", `{"error":{"code":-32603,"data":"min height 25052001 can't be greater than max height 100"}}`, 0, false},
		{"ordinary result", `{"jsonrpc":"2.0","id":1,"result":{"block":{}}}`, 0, false},
		{"empty", ``, 0, false},
	}
	for _, tc := range cases {
		got, ok := prunedLowestHeight([]byte(tc.body))
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s: got (%d, %v), want (%d, %v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestPrunedMemory_ExpiresAndBounds(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	m := newPrunedMemory()
	m.now = func() time.Time { return now }
	m.ttl = time.Hour
	m.max = 2

	m.set("a", 100)
	if got, ok := m.lowest("a"); !ok || got != 100 {
		t.Fatalf("lowest(a) = %d,%v want 100,true", got, ok)
	}
	now = now.Add(time.Hour + time.Second)
	if _, ok := m.lowest("a"); ok {
		t.Fatal("an hour-old observation must have aged out")
	}

	m.set("a", 100)
	m.set("b", 200)
	m.set("c", 300) // past the cap: wholesale clear, then c alone
	if _, ok := m.lowest("a"); ok {
		t.Fatal("cap exceeded should clear the memory")
	}
	if got, ok := m.lowest("c"); !ok || got != 300 {
		t.Fatalf("lowest(c) = %d,%v want 300,true", got, ok)
	}
	m.set("", 5)
	m.set("d", 0)
	if _, ok := m.lowest(""); ok {
		t.Fatal("empty host must not be stored")
	}
	if _, ok := m.lowest("d"); ok {
		t.Fatal("zero height must not be stored")
	}
}

// End to end through the plugin: a pruned answer observed from one host keeps
// that host out of later requests for heights it does not hold, on the RPC
// and REST faces alike, and leaves requests for the latest block alone. When
// every host is pruned below the height the full list comes back (degraded),
// so the query is sent once and the node's answer is delivered.
func TestSelectEndpoints_SkipsHostsPrunedBelowRequestedHeight(t *testing.T) {
	p := NewPlugin(nil, Config{})
	pruned := domain.EndpointAddr("pokt1a-https://rm02.kalorius.tech")
	prunedTwin := domain.EndpointAddr("pokt1b-https://rm02.kalorius.tech") // same host, other supplier
	archive := domain.EndpointAddr("pokt1c-https://r004.rpcgate.xyz")
	all := domain.EndpointAddrList{pruned, prunedTwin, archive}

	body := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":"height 27782 is not available, lowest height is 25052001"}}`)
	if _, err := p.ExtractData(pruned, nil, body); err != nil {
		t.Fatalf("ExtractData: %v", err)
	}

	old := domain.NewPayload([]byte(`{"method":"block","params":{"height":"27782"}}`), domain.RPCTypeCometBFT, "block")
	got, err := p.SelectEndpoints(all, []domain.Payload{old})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != archive {
		t.Fatalf("old height: got %v, want only %v (both kalorius addresses share the pruned host)", got, archive)
	}

	restOld := domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP("/cosmos/base/tendermint/v1beta1/blocks/40628", "GET")
	got, err = p.SelectEndpoints(all, []domain.Payload{restOld})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != archive {
		t.Fatalf("REST old height: got %v, want only %v", got, archive)
	}

	recent := domain.NewPayload([]byte(`{"method":"block","params":{"height":"25052002"}}`), domain.RPCTypeCometBFT, "block")
	got, err = p.SelectEndpoints(all, []domain.Payload{recent})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("height the pruned host still holds: got %v, want all three", got)
	}

	latest := domain.NewPayload([]byte(`{"method":"block","params":{}}`), domain.RPCTypeCometBFT, "block")
	got, err = p.SelectEndpoints(all, []domain.Payload{latest})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("latest: got %v, want all three", got)
	}

	onlyPruned := domain.EndpointAddrList{pruned, prunedTwin}
	got, err = p.SelectEndpoints(onlyPruned, []domain.Payload{old})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("no host holds the height: got %v, want the full list so the query is sent once", got)
	}

	p.ResetState()
	got, _ = p.SelectEndpoints(all, []domain.Payload{old})
	if len(got) != 3 {
		t.Fatalf("after ResetState the memory must be empty, got %v", got)
	}
}
