package traffic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/domain"
)

const svc domain.ServiceID = "eth"

// jsonRPCPayload builds a JSON-RPC payload. The inputs are always
// marshalable, so a marshal error here means the test itself is broken;
// panicking (rather than failing through *testing.T, which is unsafe to call
// from a non-test goroutine) surfaces that immediately even when called
// concurrently.
func jsonRPCPayload(_ *testing.T, method string, params any, id int) domain.Payload {
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      id,
	})
	if err != nil {
		panic(err)
	}
	return domain.NewPayload(body, domain.RPCTypeJSONRPC, method)
}

func restPayload(_ *testing.T, httpMethod, path string, body []byte) domain.Payload {
	return domain.NewPayload(body, domain.RPCTypeREST, "").WithHTTP(path, httpMethod)
}

// cometBFTPayload builds a CometBFT payload carrying a JSON-RPC envelope
// (method + params) — CometBFT bodies can be JSON-RPC-shaped, unlike REST or
// gRPC.
func cometBFTPayload(_ *testing.T, method string, params any, id int) domain.Payload {
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      id,
	})
	if err != nil {
		panic(err)
	}
	return domain.NewPayload(body, domain.RPCTypeCometBFT, method)
}

// grpcPayload builds a gRPC payload: its method comes from the URL path (as
// grpcMethodFromPath would produce), while its body is opaque bytes — never a
// JSON-RPC envelope.
func grpcPayload(_ *testing.T, method string, body []byte) domain.Payload {
	return domain.NewPayload(body, domain.RPCTypeGRPC, method).
		WithHTTP("/"+method, "POST").
		WithContentType("application/grpc")
}

func TestObserve_IdenticalRequestsModuloID_CollapseToOneFingerprint(t *testing.T) {
	s := New(WithRate(1))

	for i := 0; i < 100; i++ {
		s.Observe(svc, []domain.Payload{
			jsonRPCPayload(t, "eth_getBalance", []string{"0xabc", "latest"}, i),
		})
	}

	summary, ok := s.Summary(svc, false)
	require.True(t, ok)
	assert.Equal(t, 100, summary.Sampled)
	assert.Equal(t, 1, summary.Distinct)
	assert.Equal(t, 1.0, summary.Top1Share)
	assert.InDelta(t, 0.01, summary.DistinctRatio, 1e-9)
}

func TestObserve_DistinctAddresses_CountedSeparately(t *testing.T) {
	s := New(WithRate(1))

	for i := 0; i < 100; i++ {
		addr := fmt.Sprintf("0x%040x", i)
		s.Observe(svc, []domain.Payload{
			jsonRPCPayload(t, "eth_getBalance", []string{addr, "latest"}, 1),
		})
	}

	summary, ok := s.Summary(svc, false)
	require.True(t, ok)
	assert.Equal(t, 100, summary.Sampled)
	assert.Equal(t, 100, summary.Distinct)
	assert.InDelta(t, 1.0, summary.DistinctRatio, 1e-9)
	assert.InDelta(t, 0.01, summary.Top1Share, 1e-9)
}

// TestObserve_FingerprintMustIncludeParams is the revert-check: if the
// fingerprint were computed from the method alone (ignoring params), this
// test — unlike the "distinct addresses" test above — would immediately fail
// since all 100 distinct-address requests would collapse to one fingerprint.
func TestObserve_FingerprintMustIncludeParams(t *testing.T) {
	s := New(WithRate(1))

	s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_getBalance", []string{"0xaaa"}, 1)})
	s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_getBalance", []string{"0xbbb"}, 1)})

	summary, ok := s.Summary(svc, false)
	require.True(t, ok)
	assert.Equal(t, 2, summary.Distinct, "different params must produce different fingerprints")
}

func TestObserve_PerMethodStats(t *testing.T) {
	s := New(WithRate(1))

	for i := 0; i < 10; i++ {
		s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_blockNumber", []any{}, i)})
	}
	for i := 0; i < 5; i++ {
		addr := fmt.Sprintf("0x%040x", i)
		s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_getBalance", []string{addr}, 1)})
	}

	summary, ok := s.Summary(svc, false)
	require.True(t, ok)
	require.Len(t, summary.PerMethod, 2)

	bn := summary.PerMethod["eth_blockNumber"]
	assert.Equal(t, 10, bn.Sampled)
	assert.Equal(t, 1, bn.Distinct)
	assert.InDelta(t, 0.1, bn.DistinctRatio, 1e-9)

	gb := summary.PerMethod["eth_getBalance"]
	assert.Equal(t, 5, gb.Sampled)
	assert.Equal(t, 5, gb.Distinct)
	assert.InDelta(t, 1.0, gb.DistinctRatio, 1e-9)
}

func TestObserve_NonJSONRPC_FingerprintsOnVerbPathAndBody(t *testing.T) {
	s := New(WithRate(1))

	for i := 0; i < 3; i++ {
		s.Observe(svc, []domain.Payload{restPayload(t, "GET", "/status", nil)})
	}
	s.Observe(svc, []domain.Payload{restPayload(t, "GET", "/health", nil)})

	summary, ok := s.Summary(svc, false)
	require.True(t, ok)
	assert.Equal(t, 4, summary.Sampled)
	assert.Equal(t, 2, summary.Distinct)
	// Non-JSON-RPC payloads carry the empty raw method.
	require.Contains(t, summary.PerMethod, "")
	assert.Equal(t, 4, summary.PerMethod[""].Sampled)
	assert.Equal(t, 2, summary.PerMethod[""].Distinct)
}

func TestObserve_MaxFingerprints_BoundsDistinctAndCountsOverflow(t *testing.T) {
	s := New(WithRate(1), WithMaxFingerprints(10))

	for i := 0; i < 50; i++ {
		addr := fmt.Sprintf("0x%040x", i)
		s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_getBalance", []string{addr}, 1)})
	}

	summary, ok := s.Summary(svc, false)
	require.True(t, ok)
	assert.Equal(t, 50, summary.Sampled)
	assert.Equal(t, 10, summary.Distinct)
	assert.Equal(t, 40, summary.Overflow)
}

func TestObserve_WindowRoll_PreviousHoldsOldCounts(t *testing.T) {
	s := New(WithRate(1), WithWindow(20*time.Millisecond))

	for i := 0; i < 5; i++ {
		s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_blockNumber", []any{}, i)})
	}

	// No previous window exists yet.
	_, ok := s.Summary(svc, true)
	assert.False(t, ok)

	time.Sleep(30 * time.Millisecond)

	// The first Observe after WindowEnd rolls current into previous.
	s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_chainId", []any{}, 1)})

	prev, ok := s.Summary(svc, true)
	require.True(t, ok)
	assert.Equal(t, 5, prev.Sampled)
	assert.Equal(t, 1, prev.Distinct)

	cur, ok := s.Summary(svc, false)
	require.True(t, ok)
	assert.Equal(t, 1, cur.Sampled)
	assert.Equal(t, 1, cur.Distinct)
}

func TestObserve_Rate_SamplesAboutOneInN(t *testing.T) {
	s := New(WithRate(100))

	const n = 10000
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("0x%040x", i)
		s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_getBalance", []string{addr}, 1)})
	}

	summary, ok := s.Summary(svc, false)
	require.True(t, ok)
	assert.Equal(t, n/100, summary.Sampled)
}

func TestSummary_UnknownService_ReturnsFalse(t *testing.T) {
	s := New()
	_, ok := s.Summary("does-not-exist", false)
	assert.False(t, ok)
}

func TestTop_OrderedByCountDescending(t *testing.T) {
	s := New(WithRate(1))

	for i := 0; i < 5; i++ {
		s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_blockNumber", []any{}, i)})
	}
	for i := 0; i < 2; i++ {
		s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_chainId", []any{}, i)})
	}

	top := s.Top(svc, false, 1)
	require.Len(t, top, 1)
	assert.Equal(t, "eth_blockNumber", top[0].Method)
	assert.Equal(t, 5, top[0].Count)
	assert.InDelta(t, 5.0/7.0, top[0].Share, 1e-9)

	all := s.Top(svc, false, 0)
	assert.Len(t, all, 2)
}

func TestServices_ListsObservedServices(t *testing.T) {
	s := New(WithRate(1))
	s.Observe("eth", []domain.Payload{jsonRPCPayload(t, "eth_blockNumber", []any{}, 1)})
	s.Observe("poly", []domain.Payload{jsonRPCPayload(t, "eth_blockNumber", []any{}, 1)})

	ids := s.Services()
	assert.Equal(t, []domain.ServiceID{"eth", "poly"}, ids)
}

func TestObserve_Concurrent(t *testing.T) {
	s := New(WithRate(1))

	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				addr := fmt.Sprintf("0x%d-%d", g, i)
				s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_getBalance", []string{addr}, i)})
				_, _ = s.Summary(svc, false)
				_ = s.Top(svc, false, 5)
				_ = s.Services()
			}
		}(g)
	}
	wg.Wait()

	summary, ok := s.Summary(svc, false)
	require.True(t, ok)
	assert.Equal(t, 20*200, summary.Sampled)
}

func TestObserve_MethodTable_BoundedByMaxFingerprints(t *testing.T) {
	s := New(WithRate(1), WithMaxFingerprints(10))

	for i := 0; i < 50; i++ {
		method := fmt.Sprintf("custom_method_%d", i)
		s.Observe(svc, []domain.Payload{jsonRPCPayload(t, method, []any{}, 1)})
	}

	summary, ok := s.Summary(svc, false)
	require.True(t, ok)
	assert.Len(t, summary.PerMethod, 11, "10 real methods plus the shared _other bucket")
	assert.Equal(t, 40, summary.MethodOverflow)

	other, ok := summary.PerMethod["_other"]
	require.True(t, ok)
	assert.Equal(t, 40, other.Sampled)
}

func TestObserve_GRPC_FingerprintsByBodyNotJustMethod(t *testing.T) {
	s := New(WithRate(1))

	s.Observe(svc, []domain.Payload{grpcPayload(t, "pkg.Service/Method", []byte{0x01, 0x02, 0x03})})
	s.Observe(svc, []domain.Payload{grpcPayload(t, "pkg.Service/Method", []byte{0x04, 0x05, 0x06})})

	summary, ok := s.Summary(svc, false)
	require.True(t, ok)
	assert.Equal(t, 2, summary.Sampled)
	assert.Equal(t, 2, summary.Distinct, "same gRPC method, different bodies, must not collapse to one fingerprint")
}

func TestObserve_CometBFTJSONRPC_IdenticalModuloID_CollapsesToOne(t *testing.T) {
	s := New(WithRate(1))

	for i := 0; i < 10; i++ {
		s.Observe(svc, []domain.Payload{cometBFTPayload(t, "tx_search", map[string]any{"query": "tx.height=5"}, i)})
	}

	summary, ok := s.Summary(svc, false)
	require.True(t, ok)
	assert.Equal(t, 10, summary.Sampled)
	assert.Equal(t, 1, summary.Distinct)
}

// TestObserve_NonJSONRPC_HashIsCappedAtHashBytes pins the documented cost
// ceiling on the non-JSON-RPC fingerprint. A REST or gRPC body has no "params"
// member to reduce it to, so an untrusted client would otherwise choose how
// much work each sampled relay does. The price is stated rather than hidden:
// two such bodies identical in their first hashBytes share a fingerprint.
func TestObserve_NonJSONRPC_HashIsCappedAtHashBytes(t *testing.T) {
	s := New(WithRate(1))

	head := bytes.Repeat([]byte("a"), hashBytes)
	withTail := func(tail string) []byte {
		return append(append([]byte{}, head...), tail...)
	}

	s.Observe(svc, []domain.Payload{restPayload(t, "POST", "/tx", withTail("tail-one"))})
	s.Observe(svc, []domain.Payload{restPayload(t, "POST", "/tx", withTail("tail-two"))})

	summary, ok := s.Summary(svc, false)
	require.True(t, ok)
	assert.Equal(t, 2, summary.Sampled)
	assert.Equal(t, 1, summary.Distinct,
		"bodies identical in their first hashBytes must share one fingerprint — the documented cap")

	// The control: the cap must not be collapsing everything. A difference
	// inside the first hashBytes is still a distinct shape.
	inCap := append(append([]byte{}, head[:hashBytes-1]...), 'z')
	s.Observe(svc, []domain.Payload{restPayload(t, "POST", "/tx", inCap)})

	summary, ok = s.Summary(svc, false)
	require.True(t, ok)
	assert.Equal(t, 2, summary.Distinct, "a difference within the cap must still be seen")
}

// TestPreviousWindow_StopsReportingAStaleWindow covers the gauge lister's
// staleness rule. Windows roll when traffic arrives, so a service that goes
// quiet freezes with whatever its last two windows held — and a gauge read
// from that frozen window would go on describing traffic that stopped hours
// ago as if it were current. Absent is the honest answer.
func TestPreviousWindow_StopsReportingAStaleWindow(t *testing.T) {
	const window = 50 * time.Millisecond
	s := New(WithRate(1), WithWindow(window))

	if _, _, ok := s.PreviousWindow(svc); ok {
		t.Fatal("an unobserved service must have no previous window")
	}

	for i := 0; i < 4; i++ {
		s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_blockNumber", []any{}, i)})
	}
	if _, _, ok := s.PreviousWindow(svc); ok {
		t.Fatal("no window has rolled yet, so there is nothing complete to report")
	}

	time.Sleep(window + 5*time.Millisecond)
	s.Observe(svc, []domain.Payload{jsonRPCPayload(t, "eth_chainId", []any{}, 1)})

	distinctRatio, top1Share, ok := s.PreviousWindow(svc)
	require.True(t, ok, "the window that just rolled is fresh and must be reported")
	assert.InDelta(t, 0.25, distinctRatio, 1e-9)
	assert.InDelta(t, 1.0, top1Share, 1e-9)

	// Traffic stops. Past staleWindowFactor windows the series must go absent.
	time.Sleep(staleWindowFactor*window + 20*time.Millisecond)
	if _, _, ok := s.PreviousWindow(svc); ok {
		t.Fatal("a previous window older than the staleness bound must not be reported")
	}

	// It is the age that disqualifies it, not the data: the admin route still
	// serves the same window, with the timestamps that say how old it is.
	if _, ok := s.Summary(svc, true); !ok {
		t.Fatal("Summary must still serve the stale window for the admin route")
	}
}
