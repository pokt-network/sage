package middleware

import (
	"errors"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// fakeRecorder captures the arguments passed to RecordRelay.
type fakeRecorder struct {
	serviceID  domain.ServiceID
	endpoint   domain.EndpointAddr
	statusCode int
	latency    time.Duration
	err        error
	called     bool

	verdicts []fakeVerdict
}

type fakeVerdict struct {
	serviceID   domain.ServiceID
	rpcType     domain.RPCType
	reason      string
	attribution string
}

func (r *fakeRecorder) RecordUnclassified(domain.ServiceID, string, string) {}

func (r *fakeRecorder) RecordVerdict(serviceID domain.ServiceID, rpcType domain.RPCType, reason, attribution string) {
	r.verdicts = append(r.verdicts, fakeVerdict{serviceID, rpcType, reason, attribution})
}

func (r *fakeRecorder) RecordOperatorAttempt(domain.ServiceID, domain.RPCType, domain.EndpointAddr, string, string, string, time.Duration) {
}

func (r *fakeRecorder) RecordRelay(serviceID domain.ServiceID, endpoint domain.EndpointAddr, statusCode int, latency time.Duration, err error) {
	r.serviceID = serviceID
	r.endpoint = endpoint
	r.statusCode = statusCode
	r.latency = latency
	r.err = err
	r.called = true
}

func TestMetrics_RecordsSuccessFields(t *testing.T) {
	rec := &fakeRecorder{}
	endpoint := domain.EndpointAddr("supplierA-https://node.example.com")

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{
			HTTPStatusCode: http.StatusOK,
			EndpointAddr:   endpoint,
		}
		ctx.Endpoint = endpoint
		return nil
	})

	ctx := baseContext()
	ctx.ServiceID = "eth"

	mw := Metrics(rec)
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !rec.called {
		t.Fatal("RecordRelay was not called")
	}
	if rec.serviceID != "eth" {
		t.Errorf("serviceID: got %q, want %q", rec.serviceID, "eth")
	}
	if rec.endpoint != endpoint {
		t.Errorf("endpoint: got %q, want %q", rec.endpoint, endpoint)
	}
	if rec.statusCode != http.StatusOK {
		t.Errorf("statusCode: got %d, want %d", rec.statusCode, http.StatusOK)
	}
	if rec.latency < 0 {
		t.Errorf("latency should be non-negative, got %v", rec.latency)
	}
	if rec.err != nil {
		t.Errorf("err: got %v, want nil", rec.err)
	}
}

func TestMetrics_RecordsErrorFields(t *testing.T) {
	rec := &fakeRecorder{}
	sentErr := errors.New("relay failed")

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = "supplierA-https://node.example.com"
		return sentErr
	})

	ctx := baseContext()
	ctx.ServiceID = "poly"

	mw := Metrics(rec)
	err := mw(inner).HandleRelay(ctx)
	if !errors.Is(err, sentErr) {
		t.Fatalf("expected sentErr, got %v", err)
	}

	if !rec.called {
		t.Fatal("RecordRelay was not called on error")
	}
	if rec.statusCode != http.StatusBadGateway {
		t.Errorf("statusCode: got %d, want %d (bad gateway sentinel on relay error)",
			rec.statusCode, http.StatusBadGateway)
	}
	if rec.err == nil {
		t.Error("expected non-nil err in recorder")
	}
}

func TestMetrics_LatencyPositive(t *testing.T) {
	rec := &fakeRecorder{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK}
		return nil
	})

	ctx := baseContext()
	_ = Metrics(rec)(inner).HandleRelay(ctx)

	if rec.latency < 0 {
		t.Errorf("latency must be non-negative, got %v", rec.latency)
	}
}

func TestMetrics_StatusCodeFromResponse(t *testing.T) {
	rec := &fakeRecorder{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusTooManyRequests}
		return nil
	})

	ctx := baseContext()
	_ = Metrics(rec)(inner).HandleRelay(ctx)

	if rec.statusCode != http.StatusTooManyRequests {
		t.Errorf("statusCode: got %d, want %d", rec.statusCode, http.StatusTooManyRequests)
	}
}

// The verdict the heuristic left on the context for this attempt is recorded
// with the attempt; an attempt without one (flag off, no response) records
// nothing rather than a placeholder.
func TestMetrics_RecordsHeuristicVerdictPerAttempt(t *testing.T) {
	rec := &fakeRecorder{}
	verdict := heuristic.AnalysisResult{Reason: "internal_error", Attribution: heuristic.AttrBlockchain}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK}
		ctx.HeuristicResult = &verdict
		return nil
	})
	ctx := baseContext()
	ctx.ServiceID = "shentu"
	ctx.RPCType = domain.RPCTypeCometBFT
	if err := Metrics(rec)(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []fakeVerdict{{"shentu", domain.RPCTypeCometBFT, "internal_error", "blockchain"}}
	if len(rec.verdicts) != 1 || rec.verdicts[0] != want[0] {
		t.Fatalf("verdicts = %+v, want %+v", rec.verdicts, want)
	}

	// A success is exported with no attribution: internally it carries
	// AttrClient as "no action needed", which must not read as a client error.
	rec = &fakeRecorder{}
	ok := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK}
		res := heuristic.AnalysisResult{Reason: heuristic.ReasonSuccess, Attribution: heuristic.AttrClient}
		ctx.HeuristicResult = &res
		return nil
	})
	ctx = baseContext()
	ctx.ServiceID = "shentu"
	ctx.RPCType = domain.RPCTypeCometBFT
	if err := Metrics(rec)(ok).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rec.verdicts) != 1 || rec.verdicts[0].attribution != "none" || rec.verdicts[0].reason != heuristic.ReasonSuccess {
		t.Fatalf("verdicts = %+v, want one success with attribution none", rec.verdicts)
	}

	rec = &fakeRecorder{}
	quiet := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK}
		return nil
	})
	if err := Metrics(rec)(quiet).HandleRelay(baseContext()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rec.verdicts) != 0 {
		t.Fatalf("verdicts = %+v, want none when the heuristic left no result", rec.verdicts)
	}
}

// operatorRecorder is a fakeRecorder that also takes per-operator attempts.
type operatorRecorder struct {
	fakeRecorder
	mu       sync.Mutex
	attempts []string
	kinds    []string
}

func (r *operatorRecorder) RecordOperatorAttempt(_ domain.ServiceID, _ domain.RPCType, endpoint domain.EndpointAddr, attribution, kind, methodClass string, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts = append(r.attempts, endpoint.Operator()+":"+attribution+":"+kind+":"+methodClass)
	r.kinds = append(r.kinds, kind)
}

// RecordRelay and RecordVerdict lock too: hedge arms record concurrently.
func (r *operatorRecorder) RecordRelay(serviceID domain.ServiceID, endpoint domain.EndpointAddr, statusCode int, latency time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fakeRecorder.RecordRelay(serviceID, endpoint, statusCode, latency, err)
}

func (r *operatorRecorder) RecordUnclassified(domain.ServiceID, string, string) {}

func (r *operatorRecorder) RecordVerdict(serviceID domain.ServiceID, rpcType domain.RPCType, reason, attribution string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fakeRecorder.RecordVerdict(serviceID, rpcType, reason, attribution)
}

func (r *operatorRecorder) seenKinds() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.kinds...)
}

// Every attempt is counted against the operator that served it, with whose
// fault its outcome was (none for a good answer, the verdict's side for a
// graded one, unknown for a failure nothing graded), what kind of attempt it
// was, and what class of method it carried.
func TestMetrics_RecordsOperatorAttemptPerAttempt(t *testing.T) {
	rec := &operatorRecorder{}
	outcomes := []struct {
		verdict *heuristic.AnalysisResult
		err     error
		kind    string
		method  string
	}{
		{nil, nil, "", "eth_blockNumber"},
		{&heuristic.AnalysisResult{Reason: "blockchain_error", Attribution: heuristic.AttrBlockchain}, errors.New("retry"), relay.AttemptRetry, "eth_getLogs"},
		{nil, errors.New("dial"), relay.AttemptHedge, "eth_getBalance"},
	}
	for _, o := range outcomes {
		inner := relay.HandlerFunc(func(ctx *relay.Context) error {
			ctx.Endpoint = "supplierA-https://rm01.node.example.com"
			ctx.HeuristicResult = o.verdict
			return o.err
		})
		ctx := baseContext()
		ctx.ServiceID = "eth"
		ctx.AttemptKind = o.kind
		ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, o.method)}
		_ = Metrics(rec)(inner).HandleRelay(ctx)
	}
	want := []string{
		"example.com:none:first:light",
		"example.com:blockchain:retry:heavy",
		"example.com:unknown:hedge:standard",
	}
	if len(rec.attempts) != len(want) {
		t.Fatalf("attempts = %v, want %v", rec.attempts, want)
	}
	for i := range want {
		if rec.attempts[i] != want[i] {
			t.Fatalf("attempts = %v, want %v", rec.attempts, want)
		}
	}
}

// The attempt hook hears each attempt with what the counter keeps, plus the
// method as the service's plugin catalogues it, the key method blocks use.
func TestMetrics_AttemptHookCarriesTheCataloguedMethod(t *testing.T) {
	var got []string
	hook := WithAttemptHook(func(svc domain.ServiceID, rpc domain.RPCType, ep domain.EndpointAddr, attribution, kind, method string) {
		got = append(got, string(svc)+" "+string(rpc)+" "+ep.Operator()+" "+attribution+" "+kind+" "+method)
	})
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = "supplierA-https://rm01.node.example.com"
		ctx.HeuristicResult = &heuristic.AnalysisResult{Reason: "internal_error", Attribution: heuristic.AttrBlockchain}
		return nil
	})
	ctx := baseContext()
	ctx.RPCType = domain.RPCTypeJSONRPC
	ctx.Plugin = normPlugin{}
	ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "getSlot")}
	_ = Metrics(&operatorRecorder{}, hook)(inner).HandleRelay(ctx)

	if len(got) != 1 || got[0] != "eth json_rpc example.com blockchain first getSlot" {
		t.Fatalf("hook heard %q", got)
	}
}

// The attempt label is only a fair sample if the chain writes it: a first
// attempt reads first, the one after a failure reads retry, and the second arm
// of a fired hedge reads hedge.
func TestMetrics_AttemptKindWrittenByTheChain(t *testing.T) {
	rep := reputation.NewService(reputation.NewMemoryStorage(), nil, reputation.ServiceConfig{})
	flags := newFlags(featureflag.FlagRetry, featureflag.FlagHedge)
	chain := func(delay time.Duration, rec *operatorRecorder, send relay.HandlerFunc) relay.Handler {
		cfg := func(domain.ServiceID) config.RetryConfig {
			return config.RetryConfig{Enabled: true, MaxRetries: 1, HedgeDelay: delay}
		}
		return Retry(flags, cfg, nil, RetryOptions{})(Hedge(flags, cfg, nil, nil)(Metrics(rec)(SelectEndpoint(rep, nil)(send))))
	}

	// A failed first attempt, then a retry. The hedge delay is long enough
	// that no hedge fires.
	var calls atomic.Int32
	rec := &operatorRecorder{}
	_ = chain(time.Second, rec, func(c *relay.Context) error {
		if calls.Add(1) == 1 {
			return domain.NewRelayError(domain.ErrEndpoint, "first fails", nil, true)
		}
		c.Response = &domain.Response{HTTPStatusCode: 200, Body: []byte(`{}`)}
		return nil
	}).HandleRelay(baseContext())
	if got := rec.seenKinds(); len(got) != 2 || got[0] != relay.AttemptFirst || got[1] != relay.AttemptRetry {
		t.Fatalf("retry: kinds = %v, want [first retry]", got)
	}

	// A slow primary and a fired hedge.
	rec = &operatorRecorder{}
	var arm atomic.Int32
	_ = chain(10*time.Millisecond, rec, func(c *relay.Context) error {
		if arm.Add(1) == 1 {
			time.Sleep(100 * time.Millisecond)
		}
		c.Response = &domain.Response{HTTPStatusCode: 200, Body: []byte(`{}`)}
		return nil
	}).HandleRelay(baseContext())
	deadline := time.Now().Add(2 * time.Second)
	for len(rec.seenKinds()) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	got := rec.seenKinds()
	sort.Strings(got)
	if len(got) != 2 || got[0] != relay.AttemptFirst || got[1] != relay.AttemptHedge {
		t.Fatalf("hedge: kinds = %v, want first and hedge", got)
	}
}
