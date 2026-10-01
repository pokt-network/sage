package cosmos

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
)

func TestHeadTime(t *testing.T) {
	ts := "2026-10-01T16:59:39.123456789Z"
	get := func(path string) domain.Payload {
		return domain.NewPayload(nil, domain.RPCTypeCometBFT, "").WithHTTP(path, http.MethodGet)
	}
	post := func(body string) domain.Payload {
		return domain.NewPayload([]byte(body), domain.RPCTypeJSONRPC, gjson.Get(body, "method").String())
	}
	status := fmt.Sprintf(`{"jsonrpc":"2.0","id":-1,"result":{"sync_info":{"latest_block_height":"10","latest_block_time":%q}}}`, ts)
	block := fmt.Sprintf(`{"jsonrpc":"2.0","id":-1,"result":{"block":{"header":{"height":"10","time":%q}}}}`, ts)
	for _, tc := range []struct {
		name    string
		payload domain.Payload
		body    string
		ok      bool
	}{
		{"GET /status", get("/status"), status, true},
		{"JSON-RPC status", post(`{"jsonrpc":"2.0","id":1,"method":"status"}`), status, true},
		{"GET /block, latest", get("/block"), block, true},
		{"GET /block at a height", get("/block?height=5"), block, false},
		{"JSON-RPC block, latest", post(`{"jsonrpc":"2.0","id":1,"method":"block","params":{}}`), block, true},
		{"JSON-RPC block at a height", post(`{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"5"}}`), block, false},
		{"JSON-RPC block at a height, positional", post(`{"jsonrpc":"2.0","id":1,"method":"block","params":["5"]}`), block, false},
		{"JSON-RPC block, positional and empty", post(`{"jsonrpc":"2.0","id":1,"method":"block","params":[]}`), block, true},
		{"REST latest", restHeadCanary(), fmt.Sprintf(`{"block":{"header":{"height":"10","time":%q}}}`, ts), true},
		{"REST latest, sdk_block", restHeadCanary(), fmt.Sprintf(`{"sdk_block":{"header":{"height":"10","time":%q}}}`, ts), true},
		{"REST syncing", restSyncingPayload(), `{"syncing":false}`, false},
		{"status with no time", get("/status"), `{"result":{"sync_info":{}}}`, false},
	} {
		got, ok := headTime(tc.payload, []byte(tc.body))
		if ok != tc.ok || (ok && got.Format(time.RFC3339Nano) != ts) {
			t.Errorf("%s: %v %v, want ok=%v", tc.name, got, ok, tc.ok)
		}
	}
}

// A CometBFT status or REST latest-block answer is graded by its block's
// time; the REST canary runs with state_canary, and grades the head.
func TestCosmosHeadLagAndRESTCanary(t *testing.T) {
	on := true
	p := NewPlugin(nil, Config{SyncAllowance: 10, StateCanary: func() bool { return on }})
	p.UpdateBlockHeight("a1-https://x.a.net", 1000)
	p.UpdateBlockHeight("a1-https://x.a.net", 1010)
	now := time.Now()
	answer := func(age time.Duration) []byte {
		return []byte(fmt.Sprintf(`{"block":{"header":{"time":%q}}}`, now.Add(-age).UTC().Format(time.RFC3339Nano)))
	}
	if _, stale, ok := p.HeadLag(restHeadCanary(), answer(time.Second), now); !ok || stale {
		t.Fatalf("fresh: stale=%v ok=%v", stale, ok)
	}
	if _, stale, ok := p.HeadLag(restHeadCanary(), answer(2*time.Minute), now); !ok || !stale {
		t.Fatalf("two minutes old: stale=%v ok=%v", stale, ok)
	}
	has := func() bool {
		for _, c := range p.HealthChecks() {
			if c.Name == restCanaryName {
				return c.GradesHead && c.Payload.RPCType() == domain.RPCTypeREST
			}
		}
		return false
	}
	if !has() {
		t.Fatal("REST canary missing with state_canary on")
	}
	on = false
	if has() {
		t.Fatal("REST canary sent with state_canary off")
	}
}
