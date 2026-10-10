package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/relay"
)

// A pruned host's refusal of historical state marks it on the attempt itself,
// whatever answer the request is finally delivered.
func TestHeuristic_RecordsArchivalPerAttempt(t *testing.T) {
	for name, tc := range map[string]struct {
		flags []string
		want  bool
	}{
		"on":  {[]string{"archival_per_attempt"}, true},
		"off": {nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			plugin := evm.NewPlugin(nil, evm.Config{})
			ctx := relay.NewContext(context.Background(), httptest.NewRequest(http.MethodPost, "/v1", nil), nil, nil)
			ctx.ServiceID = "poly"
			ctx.Plugin = plugin
			ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1010","0x10"]}`), domain.RPCTypeJSONRPC, "eth_getBalance")}
			send := relay.HandlerFunc(func(c *relay.Context) error {
				c.Endpoint = "pokt1a-https://rm01.pruned.example"
				c.Response = &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"historical state is not available"}}`)}
				return nil
			})
			_ = Heuristic(newFlags(tc.flags...), nil, HeuristicOptions{})(send).HandleRelay(ctx)

			archival, marked := plugin.ArchivalHosts()["rm01.pruned.example"]
			if marked != tc.want || archival {
				t.Fatalf("mark = %v (archival %v), want marked=%v and not archival", marked, archival, tc.want)
			}
		})
	}
}
