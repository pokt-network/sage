package middleware_test

import (
	"testing"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/relay/middleware"
	"github.com/pokt-network/sage/reputation"
)

// Mainnet base, 2026-09-26: numbered eth_getBalance two years back. A pruned
// full node and a proxy with no archival backend answered first on every pod,
// every time, and the archival node only ever served the retry. Observe sits
// outside Retry and sees the final attempt, so the endpoints that said the
// state was gone were never the ones observed.
func TestArchival_EveryAttemptThatSaysStateIsGoneIsRemembered(t *testing.T) {
	plugin := evm.NewPlugin(nil, evm.Config{SyncAllowance: 150})
	rep := reputation.NewService(reputation.NewMemoryStorage(), nil, reputation.ServiceConfig{})
	reg := qos.NewRegistry()
	if err := reg.Register("base", plugin); err != nil {
		t.Fatal(err)
	}
	pruned := domain.EndpointAddr("sA-https://pruned.full.test")
	archive := domain.EndpointAddr("sB-https://archive.full.test")
	proxy := domain.EndpointAddr("sC-https://proxy.full.test")
	pool := domain.EndpointAddrList{pruned, archive, proxy}
	for _, ep := range pool {
		plugin.UpdateBlockHeight(ep, 51_821_922)
	}
	var firstPick domain.EndpointAddr
	send := relay.HandlerFunc(func(c *relay.Context) error {
		if firstPick == "" {
			firstPick = c.Endpoint
		}
		body := `{"jsonrpc":"2.0","id":1,"result":"0x1"}`
		switch c.Endpoint {
		case pruned:
			body = `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"historical state is not available"}}`
		case proxy:
			body = `{"jsonrpc":"2.0","id":1,"error":{"code":4444,"message":"pruned history unavailable: requested 19922944, earliest available 51000000"}}`
		}
		c.Response = &domain.Response{HTTPStatusCode: 200, Body: []byte(body)}
		return nil
	})
	flags := newMockFlags(nil)
	cfgFn := func(domain.ServiceID) config.RetryConfig { return config.RetryConfig{Enabled: true, MaxRetries: 4} }
	chain := middleware.Retry(flags, cfgFn, nil, middleware.RetryOptions{Reputation: rep})(
		middleware.Score(flags, rep)(
			middleware.SelectEndpoint(rep, nil)(
				middleware.Heuristic(flags, reg, middleware.HeuristicOptions{})(send))))

	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x00000000000000000000000000000000000000aa","0x1300000"]}`)
	first := map[domain.EndpointAddr]int{}
	for i := 0; i < 50; i++ {
		firstPick = ""
		c := newCtx(newPOSTRequest("/v1", string(body)))
		c.ServiceID, c.RPCType, c.Plugin = "base", domain.RPCTypeJSONRPC, plugin
		c.Payloads = []domain.Payload{domain.NewPayload(body, domain.RPCTypeJSONRPC, "eth_getBalance")}
		c.Endpoints = append(domain.EndpointAddrList{}, pool...)
		_ = chain.HandleRelay(c)
		// What Observe, outside Retry, hands the pipeline: the final attempt.
		if c.Response != nil {
			_, _ = plugin.ExtractData(c.Endpoint, body, c.Response.Body)
		}
		if i >= 10 {
			first[firstPick]++
		}
	}
	if first[archive] != 40 {
		t.Fatalf("first attempts after the first ten requests = %v, want all 40 on the archival node", first)
	}
}
