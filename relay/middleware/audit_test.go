package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/qos/tron"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/responsecache"
)

// Regressions for the 2026-10-03 correctness audit. Each test asserts the
// correct behaviour; on the code the audit read they fail.

// evmPayload parses body the way Parse does for an EVM JSON-RPC request.
func evmPayload(t *testing.T, plugin *evm.Plugin, body string) domain.Payload {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	payloads, err := plugin.ParseRequest(context.Background(), req, []byte(body), domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatal(err)
	}
	return payloads[0]
}

// A null result is "not yet": a pending transaction's receipt, a block a
// lagging node has not seen. Cached, it is served for minutes after the
// answer exists. The second identical request must reach upstream.
func TestAudit_CacheStoresNullResult(t *testing.T) {
	plugin := evm.NewPlugin(nil, evm.Config{})
	nullAns := []byte(`{"jsonrpc":"2.0","id":1,"result":null}`)
	if r := heuristic.Analyze(nullAns, 200, domain.RPCTypeJSONRPC); !r.IsSuccess() {
		t.Fatalf("precondition: the analyzer passes a null result, got %+v", r)
	}

	for _, tc := range []struct{ name, body string }{
		{"pending receipt", `{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["0xabc"]}`},
		// Passes today, but only because Cache hands CacheTTL the whole
		// request object where the block rule expects the params array, so
		// no eth_getBlockByNumber is ever cached. The direct CacheTTL check
		// below is the policy itself.
		{"block a lagging node has not seen", `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x1312d01",false]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := evmPayload(t, plugin, tc.body)
			calls := 0
			h := Cache(&staticFlags{enabled: true}, responsecache.NewCache(100), nil)(relay.HandlerFunc(func(ctx *relay.Context) error {
				calls++
				ctx.Response = &domain.Response{Body: nullAns, HTTPStatusCode: 200}
				return nil
			}))
			var second *relay.Context
			for i := 0; i < 2; i++ {
				second = makeRelayCtx("eth", payload, plugin)
				if err := h.HandleRelay(second); err != nil {
					t.Fatal(err)
				}
			}
			if calls != 2 || second.Cached {
				t.Fatalf("a null result was cached: upstream calls = %d, second cached = %v; want 2 calls, not cached", calls, second.Cached)
			}
		})
	}

	// The block policy as written, with the params array it expects: 10
	// minutes for a null.
	if ttl := plugin.CacheTTL("eth_getBlockByNumber", []byte(`["0x1312d01",false]`), nullAns); ttl != 0 {
		t.Errorf("CacheTTL(eth_getBlockByNumber, hex block, null result) = %v, want 0", ttl)
	}
}

// Identical bytes are not an identical request when the RPC type, the HTTP
// path or the verb differ: a REST route and the JSON-RPC root answer
// different questions. They must not share a cache entry nor a coalesced
// relay.
func TestAudit_CacheAndCoalescingKeysIgnoreRoute(t *testing.T) {
	const body = `{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["0xabc"]}`
	plugin := tron.NewPlugin(nil, tron.Config{})
	parse := func(path string, rt domain.RPCType) domain.Payload {
		req, _ := http.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
		p, err := plugin.ParseRequest(context.Background(), req, []byte(body), rt)
		if err != nil {
			t.Fatal(err)
		}
		return p[0]
	}
	rest := parse("/wallet/getnowblock", domain.RPCTypeREST)
	rpc := parse("/", domain.RPCTypeJSONRPC)

	raw := []byte(`{"num":1}`)
	for _, tc := range []struct {
		name string
		a, b domain.Payload
	}{
		{"rest route vs json-rpc root", rest, rpc},
		{"different REST paths", domain.NewPayload(raw, domain.RPCTypeREST, "").WithHTTP("/wallet/getnowblock", http.MethodPost),
			domain.NewPayload(raw, domain.RPCTypeREST, "").WithHTTP("/wallet/getblockbynum", http.MethodPost)},
		{"different verbs", domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP("/eth/v1/node/health", http.MethodGet),
			domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP("/eth/v1/node/health", http.MethodHead)},
	} {
		// Singleflight keys on cacheKey too.
		if cacheKey("tron", []domain.Payload{tc.a}) == cacheKey("tron", []domain.Payload{tc.b}) {
			t.Errorf("%s: one cache and singleflight key", tc.name)
		}
	}

	// End to end: the REST route's answer must not be served to the
	// JSON-RPC client.
	h := Cache(&staticFlags{enabled: true}, responsecache.NewCache(100), nil)(relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{Body: []byte(`{"blockID":"000000000123abcd","block_header":{}}`), HTTPStatusCode: 200}
		return nil
	}))
	c1 := makeRelayCtx("tron", rest, plugin)
	c1.RPCType = domain.RPCTypeREST
	_ = h.HandleRelay(c1)
	c2 := makeRelayCtx("tron", rpc, plugin)
	c2.RPCType = domain.RPCTypeJSONRPC
	_ = h.HandleRelay(c2)
	if c2.Cached {
		t.Errorf("the JSON-RPC request was served the REST route's cached answer: %s", c2.Response.Body)
	}
}

// coalesce runs a leader and one follower through Singleflight. The first
// inner call is the leader's: it waits for the follower to join, then runs
// leader. Any later call (a follower running its own relay) runs rest.
func coalesce(t *testing.T, leaderCtx context.Context, leader, rest relay.HandlerFunc) (follower *relay.Context, followerErr error) {
	t.Helper()
	payload := domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}`), domain.RPCTypeJSONRPC, "eth_getBlockByNumber")
	var calls atomic.Int32
	ready, proceed := make(chan struct{}), make(chan struct{})
	h := Singleflight(&staticFlags{enabled: true}, nil)(relay.HandlerFunc(func(ctx *relay.Context) error {
		if calls.Add(1) == 1 {
			close(ready)
			<-proceed
			return leader(ctx)
		}
		return rest(ctx)
	}))

	ctx1 := newRelayContext("eth", &coalescablePlugin{}, payload)
	ctx1.Ctx = leaderCtx
	follower = newRelayContext("eth", &coalescablePlugin{}, payload)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = h.HandleRelay(ctx1) }()
	<-ready
	go func() { defer wg.Done(); followerErr = h.HandleRelay(follower) }()
	time.Sleep(20 * time.Millisecond) // let the follower join the flight
	close(proceed)
	wg.Wait()
	return follower, followerErr
}

// The leader's client hanging up is the leader's business. A follower whose
// client is still connected must get an answer, not context.Canceled (a
// 499 with a -32603 body).
func TestAudit_SingleflightFollowerInheritsLeaderCancel(t *testing.T) {
	leaderCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ok := []byte(`{"jsonrpc":"2.0","id":1,"result":{"number":"0x10"}}`)
	follower, err := coalesce(t, leaderCtx,
		func(ctx *relay.Context) error { cancel(); return context.Canceled },
		func(ctx *relay.Context) error {
			ctx.Response = &domain.Response{Body: ok, HTTPStatusCode: 200}
			return nil
		})
	if err != nil {
		t.Fatalf("follower with a live client got the leader's cancellation: %v", err)
	}
	if follower.Response == nil || !bytes.Equal(follower.Response.Body, ok) {
		t.Fatalf("follower response = %+v, want %s", follower.Response, ok)
	}
}

// A leader that ends on a retry verdict holding the node's answer delivers
// that answer to its own client (router.go). A follower must get the same
// answer, not a bare error the router turns into a gateway 500.
func TestAudit_SingleflightFollowerLosesNodeAnswer(t *testing.T) {
	answer := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"block not found"}}`)
	verdict := func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{Body: answer, HTTPStatusCode: 200}
		ctx.HeuristicResult = &heuristic.AnalysisResult{ShouldRetry: true, Attribution: heuristic.AttrBlockchain, Reason: "blockchain_error"}
		return domain.NewRelayError(domain.ErrEndpoint, "heuristic analysis suggests retry: blockchain_error", domain.ErrRetryVerdict, true)
	}
	follower, err := coalesce(t, context.Background(), verdict, verdict)
	if follower.Response == nil || !bytes.Equal(follower.Response.Body, answer) {
		t.Fatalf("follower got response %v and error %v; want the node's answer %s", follower.Response, err, answer)
	}
}

// A dial or TLS handshake cut short by OUR deadline, on an attempt given a
// sliver of its budget, says nothing about the host. Graded as
// transport_connect_failed it costs the host a critical penalty and a
// breaker vote; it must get the short-budget exemption a post-connect
// timeout gets.
func TestAudit_ConnectPhaseDeadlineBypassesShortBudget(t *testing.T) {
	ctx := baseContext()
	ctx.ServiceID = "sei"
	c, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	ctx.Ctx = c
	// What doTraced and SendRelay hand up when Client.Do fails before GotConn.
	connectErr := &domain.ConnectError{Cause: &url.Error{Op: "Post", URL: "https://node.example.com", Err: context.DeadlineExceeded}}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		<-ctx.Ctx.Done()
		return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", fmt.Errorf("sendHTTP: request failed: %w", connectErr), true)
	})
	mw := Heuristic(nil, nil, HeuristicOptions{AttemptTimeout: func(domain.ServiceID) time.Duration { return 5 * time.Second }})
	_ = mw(inner).HandleRelay(ctx)

	r := ctx.HeuristicResult
	if r == nil || r.ShouldCircuitBreak || r.PenaltySeverity == heuristic.SeverityCritical {
		t.Fatalf("50ms of a 5s budget, deadline hit while connecting: got %+v; want no breaker vote and no critical penalty", r)
	}
}

// The breaker keys on the address host. A REST face dialed on another host
// failing must not take the address host, and the JSON-RPC face on it, out
// of the pool.
func TestAudit_CircuitBreakerIgnoresDialedHost(t *testing.T) {
	breaker := firstErrorBreaker()
	eps := testEndpoints(2)
	provider := resolvingProvider{eps: eps} // eps[0]'s REST face is rest.other.example
	mw := CircuitBreak(breaker, provider, newFlags("circuit_breaker"), nil)

	restCtx := baseContext()
	restCtx.Endpoints = eps
	restCtx.RPCType = domain.RPCTypeREST
	_ = mw(relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{ShouldCircuitBreak: true, Attribution: heuristic.AttrSupplier, Reason: "transport_connect_failed"}
		return retryableErr("no connection to host")
	})).HandleRelay(restCtx)

	rpcCtx := baseContext()
	rpcCtx.Endpoints = eps
	rpcCtx.RPCType = domain.RPCTypeJSONRPC
	var seen domain.EndpointAddrList
	_ = mw(relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = append(seen, ctx.Endpoints...)
		return nil
	})).HandleRelay(rpcCtx)
	for _, ep := range seen {
		if ep == eps[0] {
			return
		}
	}
	t.Fatalf("json_rpc candidates = %v: %s was removed for a failure on its REST host", seen, eps[0])
}

// A batch item that ends on a retry verdict holding the node's answer gets
// that answer, as a single request does (router.go), not a gateway -32603.
func TestAudit_BatchReplacesNodeAnswerWithGatewayError(t *testing.T) {
	answer := `{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"block not found"}}`
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		id := string(ctx.Payloads[0].JSONRPCID())
		if id == "2" {
			ctx.Response = &domain.Response{Body: []byte(answer), HTTPStatusCode: 200}
			ctx.HeuristicResult = &heuristic.AnalysisResult{ShouldRetry: true, Attribution: heuristic.AttrBlockchain, Reason: "blockchain_error"}
			return domain.NewRelayError(domain.ErrEndpoint, "heuristic analysis suggests retry: blockchain_error", domain.ErrRetryVerdict, true)
		}
		ctx.Response = &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":` + id + `,"result":"0x1"}`), HTTPStatusCode: 200}
		return nil
	})
	ctx := makeMultiPayloadCtx([]domain.Payload{
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":2,"method":"eth_getBlockByNumber","params":["0x1312d01",false]}`), domain.RPCTypeJSONRPC, "eth_getBlockByNumber"),
	})
	if err := Batch(fixedLimits(4, 0), nil, nil, nil)(inner).HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(ctx.Response.Body, &arr); err != nil || len(arr) != 2 {
		t.Fatalf("batch body %s: %v", ctx.Response.Body, err)
	}
	if string(arr[1]) != answer {
		t.Fatalf("item 2 = %s, want the node's answer %s", arr[1], answer)
	}
}
