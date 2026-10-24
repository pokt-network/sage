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
// state was gone were never the ones observed. Two archival parties here: with
// one, the plugin leaves the request unfiltered rather than pile every
// historical call onto it (evm manyParties).
func TestArchival_EveryAttemptThatSaysStateIsGoneIsRemembered(t *testing.T) {
	plugin := evm.NewPlugin(nil, evm.Config{SyncAllowance: 150})
	rep := reputation.NewService(reputation.NewMemoryStorage(), nil, reputation.ServiceConfig{})
	reg := qos.NewRegistry()
	if err := reg.Register("base", plugin); err != nil {
		t.Fatal(err)
	}
	pruned := domain.EndpointAddr("sA-https://pruned.one.test")
	archive := domain.EndpointAddr("sB-https://archive.two.test")
	archive2 := domain.EndpointAddr("sD-https://archive.four.test")
	proxy := domain.EndpointAddr("sC-https://proxy.three.test")
	pool := domain.EndpointAddrList{pruned, archive, archive2, proxy}
	for _, ep := range pool {
		plugin.UpdateBlockHeight(ep, 51_821_922)
	}
	var firstPick domain.EndpointAddr
	saidGone := map[domain.EndpointAddr]bool{}
	send := relay.HandlerFunc(func(c *relay.Context) error {
		if firstPick == "" {
			firstPick = c.Endpoint
		}
		body := `{"jsonrpc":"2.0","id":1,"result":"0x1"}`
		switch c.Endpoint {
		case pruned:
			body = `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"historical state is not available"}}`
			saidGone[pruned] = true
		case proxy:
			body = `{"jsonrpc":"2.0","id":1,"error":{"code":4444,"message":"pruned history unavailable: requested 19922944, earliest available 51000000"}}`
			saidGone[proxy] = true
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
	request := func() domain.EndpointAddr {
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
		return firstPick
	}
	// Until both have said the state is gone once. Which endpoint an attempt
	// reaches is random, so a fixed ten requests left one of them unasked in
	// a few runs in a hundred, and its first refusal fell in the count below.
	for i := 0; len(saidGone) < 2; i++ {
		if i == 500 {
			t.Fatalf("after %d requests only %v said the state was gone", i, saidGone)
		}
		request()
	}
	first := map[domain.EndpointAddr]int{}
	for range 40 {
		first[request()]++
	}
	if first[archive]+first[archive2] != 40 {
		t.Fatalf("first attempts after both said the state was gone = %v, want all 40 on the archival nodes", first)
	}
}
