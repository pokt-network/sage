package middleware

import (
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// headPlugin names a head in every eth_blockNumber answer, 40 blocks behind
// and stale.
type headPlugin struct{ normPlugin }

func (headPlugin) HeadLag(payload domain.Payload, _ []byte) (uint64, bool, bool) {
	if payload.Method() != "eth_blockNumber" {
		return 0, false, false
	}
	return 40, true, true
}

// The head-lag recorder sees each 200 answer the plugin reads a head from,
// with the serving party and the method, whatever the heuristic flag says,
// and the verdict is untouched.
func TestHeuristic_RecordsHeadLag(t *testing.T) {
	type rec struct {
		party, method string
		lag           uint64
		stale         bool
	}
	var got []rec
	mw := Heuristic(newFlags(), nil, WithHeadLag(func(_ domain.ServiceID, party, method string, lag uint64, stale bool) {
		got = append(got, rec{party, method, lag, stale})
	}))
	run := func(method string, status int) {
		ctx := baseContext()
		ctx.Plugin = headPlugin{}
		ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{"method":"`+method+`"}`), domain.RPCTypeJSONRPC, method)}
		h := relay.HandlerFunc(func(c *relay.Context) error {
			c.Endpoint = "s1-https://r1.cache.example.xyz"
			c.Response = &domain.Response{HTTPStatusCode: status, Body: []byte(`{"result":"0x1"}`)}
			return nil
		})
		if err := mw(h).HandleRelay(ctx); err != nil {
			t.Fatal(err)
		}
		if ctx.HeuristicResult != nil {
			t.Errorf("%s: heuristic flag off, verdict must stay unset", method)
		}
	}
	run("eth_blockNumber", 200)
	run("eth_call", 200)
	run("eth_blockNumber", 502)
	if len(got) != 1 || got[0] != (rec{"example.xyz", "eth_blockNumber", 40, true}) {
		t.Fatalf("recorded %v, want one eth_blockNumber record for example.xyz, lag 40, stale", got)
	}
}
