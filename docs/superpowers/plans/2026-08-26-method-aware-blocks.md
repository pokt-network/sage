# Method-Aware Blocks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A host that cannot answer method M stops receiving M for a bounded time and keeps receiving every other method; transport errors are graded so a dead host reaches the circuit breaker and a client hang-up penalises nobody.

**Architecture:** A transport-error classifier in `heuristic/` gives every failed attempt an `AnalysisResult`; a `MethodBlocking` flag on that result (timeouts, `-32601`, "api not supported") feeds a new chain-agnostic `methodblock.Store` (host × method × expiry, escalation to host-level at 3 methods); a new `method_blocks` middleware inside Retry/Hedge prunes `ctx.Endpoints` before selection and marks after the attempt; plugins contribute only a bounded method vocabulary through the optional `qos.MethodNormalizer` interface.

**Tech Stack:** Go (version per `go.mod`), stdlib + testify only, `tidwall/gjson`, Prometheus client. All tests run under `-short -race`.

**Spec:** `docs/superpowers/specs/2026-08-26-method-aware-blocks-design.md` — read it first; every task below argues from it.

## Global Constraints

- Never write a bare `go` statement — `internal/safego` (`safego.Go`, `safego.Call`, `safego.Run`).
- Config fields are value types, zero = default, negative = off. Every new config field needs a doc comment (docgen golden test) AND an entry in `config/testdata/path_config.yaml` and `config/testdata/path_config_unified.yaml` (`TestConfigFixtureIsExhaustive`).
- A feature flag is defined once in `featureflag.DefaultFlags` and referenced by its `Flag*` constant, never a string literal.
- Every exported symbol has a doc comment starting with its own name; every new package has a package comment (`revive`).
- `relay.Context.Clone()` is shallow: never mutate a slice/map field in place from an arm; build a new slice.
- Metric labels are bounded by value SET (plugin catalogue, configured service IDs), not by a sanitizer.
- Run `make docs` after touching config structs, metric constructors, or admin routes; the golden test in `internal/docgen` fails otherwise.
- Every behaviour change is revert-checked: disable the change, the new test must fail, re-enable.
- Commit after every task. Commit messages in normal prose, subject ≤ 72 chars, body says why.
- Canonical checks before any "done" claim: `go build ./... && go vet ./... && gofmt -l . && make test_unit && make go_lint`.

---

## File map

| Path | Responsibility |
|---|---|
| `heuristic/transport.go` (new) | `AnalyzeTransportError` — classify the error `SendRelay` returned |
| `heuristic/transport_test.go` (new) | Real `net/http` failures against local listeners, classified |
| `heuristic/result.go` | `AnalysisResult.MethodBlocking` field |
| `heuristic/protocol.go`, `heuristic/indicators.go` | Set `MethodBlocking` on `-32601` and the unsupported-API wordings |
| `relay/middleware/heuristic.go` | Call the classifier where it returns early on error today |
| `relay/middleware/observe.go` | `AttrClient` + error ⇒ no signal |
| `qos/plugin.go` | `MethodNormalizer` interface, `MethodOther` |
| `qos/evm/methods.go` (new), `qos/solana/methods.go` (new), `qos/cosmos/methods.go` (new) | Per-plugin catalogue + `NormalizeMethod` |
| `methodblock/store.go`, `methodblock/doc.go` (new package) | Host × method marks, TTL, escalation, sweep |
| `config/service.go` | `MethodBlocksConfig` on `GatewayConfig` |
| `featureflag/defaults.go` | `FlagMethodBlocks` |
| `relay/chain_order.go` | `MWMethodBlocks`, default order, two `mustPrecede` rules |
| `relay/middleware/endpointfilter.go` (new) | Copy-on-filter helper shared by CircuitBreak and MethodBlocks |
| `relay/middleware/methodblocks.go` (new) | The middleware |
| `metrics/methodblock.go` (new) | `MethodBlockCollector` gauge; `RecordMethodBlockEvent` counter on `Recorder` |
| `router/admin.go` | `GET /admin/method-blocks/{serviceID}`, `POST /admin/method-blocks/clear/{serviceID}` |
| `cmd/sagegw/wire.go` | Construct store, register middleware, collector, admin |
| `ARCHITECTURE.md`, `docs/*.md` (generated) | Chain diagram line; regenerated references |

---

### Task 1: Transport error classifier

**Files:**
- Create: `heuristic/transport.go`
- Create: `heuristic/transport_test.go`
- Modify: `heuristic/result.go:45-63` (add field)

**Interfaces:**
- Consumes: `domain.RelayError` (`domain/errors.go`), `heuristic.AnalysisResult`, severity constants `SeverityMinor/Major/Critical`, attribution constants `AttrClient/AttrSupplier/AttrUnknown`.
- Produces: `func AnalyzeTransportError(err error, requestCtxErr error) AnalysisResult` and the new field `AnalysisResult.MethodBlocking bool`. Reason codes: `"client_cancelled"`, `"transport_connect_failed"`, `"transport_timeout"`, `"transport_error"`.

- [ ] **Step 1: Add the field**

In `heuristic/result.go`, inside `AnalysisResult` after `Details`:

```go
	// MethodBlocking indicates the endpoint could not serve THIS METHOD — it
	// timed out after accepting the connection, or answered that the method
	// is not available on it — and should not receive that method again for
	// a while. It is deliberately not set for missing historical state (per
	// block, owned by archival tri-state) or Solana's per-key index exclusion
	// (per program), because those are not "cannot do this method".
	MethodBlocking bool
```

- [ ] **Step 2: Write the failing tests with REAL error shapes**

Create `heuristic/transport_test.go`. The errors are produced by real `net/http` calls against local listeners and wrapped exactly as `protocol/shannon/relayer.go` wraps them (`domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", err, true)`), so the classifier is tested on what production hands it.

```go
package heuristic

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// relayerWrap mirrors protocol/shannon/relayer.go: every transport failure
// reaches the chain as a retryable ErrTransport RelayError wrapping the cause.
func relayerWrap(err error) error {
	return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", err, true)
}

// refusedError: a port nothing listens on.
func refusedError(t *testing.T) error {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	_, err = (&http.Client{Timeout: time.Second}).Post("http://"+addr, "application/json", nil)
	if err == nil {
		t.Fatal("expected a dial error")
	}
	return relayerWrap(err)
}

// hangError: a server that accepts and never answers; the CLIENT timeout fires.
func hangError(t *testing.T) error {
	t.Helper()
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-block }))
	t.Cleanup(func() { close(block); srv.Close() })
	_, err := (&http.Client{Timeout: 50 * time.Millisecond}).Post(srv.URL, "application/json", nil)
	if err == nil {
		t.Fatal("expected a timeout")
	}
	return relayerWrap(err)
}

// deadlineError: same hang, but the REQUEST context's deadline fires.
func deadlineError(t *testing.T) (error, error) {
	t.Helper()
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-block }))
	t.Cleanup(func() { close(block); srv.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, nil)
	_, err := http.DefaultClient.Do(req)
	if err == nil {
		t.Fatal("expected a deadline error")
	}
	return relayerWrap(err), ctx.Err()
}

// cancelError: the client hangs up mid-flight.
func cancelError(t *testing.T) (error, error) {
	t.Helper()
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-block }))
	t.Cleanup(func() { close(block); srv.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, nil)
	_, err := http.DefaultClient.Do(req)
	if err == nil {
		t.Fatal("expected a cancel error")
	}
	return relayerWrap(err), ctx.Err()
}

// dnsError: a name that cannot resolve. .invalid is reserved (RFC 2606).
func dnsError(t *testing.T) error {
	t.Helper()
	_, err := (&http.Client{Timeout: 2 * time.Second}).Post("http://nonexistent.invalid:1/", "application/json", nil)
	if err == nil {
		t.Fatal("expected a DNS error")
	}
	return relayerWrap(err)
}

func TestAnalyzeTransportError_ConnectRefusedIsHostDead(t *testing.T) {
	r := AnalyzeTransportError(refusedError(t), nil)
	if r.Attribution != AttrSupplier || !r.ShouldCircuitBreak || r.PenaltySeverity != SeverityCritical {
		t.Fatalf("refused: %+v", r)
	}
	if r.MethodBlocking {
		t.Fatal("a dead host is not a method problem")
	}
	if r.Reason != "transport_connect_failed" {
		t.Fatalf("reason = %q", r.Reason)
	}
}

func TestAnalyzeTransportError_DNSIsHostDead(t *testing.T) {
	r := AnalyzeTransportError(dnsError(t), nil)
	if !r.ShouldCircuitBreak || r.MethodBlocking {
		t.Fatalf("dns: %+v", r)
	}
}

func TestAnalyzeTransportError_TimeoutAfterConnectBlocksTheMethod(t *testing.T) {
	r := AnalyzeTransportError(hangError(t), nil)
	if !r.MethodBlocking || r.ShouldCircuitBreak {
		t.Fatalf("hang: %+v", r)
	}
	if r.Attribution != AttrSupplier || r.PenaltySeverity != SeverityMajor || !r.ShouldRetry || !r.ShouldPenalize {
		t.Fatalf("hang grading: %+v", r)
	}
	if r.Reason != "transport_timeout" {
		t.Fatalf("reason = %q", r.Reason)
	}
}

func TestAnalyzeTransportError_RequestDeadlineMidAttemptIsATimeout(t *testing.T) {
	err, ctxErr := deadlineError(t)
	r := AnalyzeTransportError(err, ctxErr)
	if !r.MethodBlocking || r.ShouldCircuitBreak {
		t.Fatalf("deadline: %+v", r)
	}
}

func TestAnalyzeTransportError_ClientCancelPenalisesNobody(t *testing.T) {
	err, ctxErr := cancelError(t)
	r := AnalyzeTransportError(err, ctxErr)
	if r.Attribution != AttrClient || r.ShouldPenalize || r.ShouldRetry || r.ShouldCircuitBreak || r.MethodBlocking {
		t.Fatalf("cancel: %+v", r)
	}
	if r.Reason != "client_cancelled" {
		t.Fatalf("reason = %q", r.Reason)
	}
}

// A cancel and a timeout can coincide on an unhedged attempt. The cancel wins:
// whatever the host was doing, nobody is waiting for the answer.
func TestAnalyzeTransportError_CancelWinsOverTimeout(t *testing.T) {
	r := AnalyzeTransportError(hangError(t), context.Canceled)
	if r.Attribution != AttrClient || r.MethodBlocking {
		t.Fatalf("cancel+timeout: %+v", r)
	}
}

func TestAnalyzeTransportError_OtherStaysMinorUnknown(t *testing.T) {
	other := domain.NewRelayError(domain.ErrProtocol, "failed to sign relay request", errors.New("boom"), false)
	r := AnalyzeTransportError(other, nil)
	if r.Attribution != AttrUnknown || r.PenaltySeverity != SeverityMinor || r.ShouldCircuitBreak || r.MethodBlocking {
		t.Fatalf("other: %+v", r)
	}
	if r.ShouldRetry {
		t.Fatal("ShouldRetry must follow domain.IsRetryable for the other bucket")
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./heuristic/ -run TestAnalyzeTransportError -count=1 -v`
Expected: build failure, `undefined: AnalyzeTransportError`.

- [ ] **Step 4: Implement**

Create `heuristic/transport.go`:

```go
package heuristic

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"

	"github.com/pokt-network/sage/domain"
)

// AnalyzeTransportError grades an attempt that failed before any response
// body existed — the branch the Heuristic middleware used to return from
// without a verdict, which is why a hanging host never reached the circuit
// breaker and a client hang-up was scored against whichever supplier held
// the relay.
//
// requestCtxErr is the request context's Err() at the time of the failure.
// It is passed in because the relayer cannot tell a client hang-up from
// anything else: the same "context canceled" reaches it either way.
//
// Verdicts, in the order they are checked:
//
//   - client cancelled: requestCtxErr is context.Canceled. Nobody is waiting
//     for the answer, so nobody is at fault. No retry, no penalty, and
//     Observe records no signal for AttrClient with an error.
//   - connect-level: the dial itself failed (refused, DNS, TLS handshake).
//     The host is not serving anything: critical, ShouldCircuitBreak.
//   - timeout after connect: the host accepted the connection and did not
//     answer in time. The one failure that says "cannot do THIS": major,
//     retry elsewhere, MethodBlocking.
//   - other: session fetch, signing, relay-miner validation, unknown. Today's
//     grading — minor, AttrUnknown, retryable per the error itself.
func AnalyzeTransportError(err error, requestCtxErr error) AnalysisResult {
	if errors.Is(requestCtxErr, context.Canceled) {
		return AnalysisResult{
			Attribution: AttrClient,
			Confidence:  0.95,
			Reason:      "client_cancelled",
			Details:     "client hung up before the endpoint answered",
		}
	}

	if isConnectFailure(err) {
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: true,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityCritical,
			Attribution:        AttrSupplier,
			Confidence:         0.90,
			Reason:             "transport_connect_failed",
			Details:            "could not connect to endpoint: " + err.Error(),
		}
	}

	if isTimeout(err) || errors.Is(requestCtxErr, context.DeadlineExceeded) {
		return AnalysisResult{
			ShouldRetry:     true,
			ShouldPenalize:  true,
			PenaltySeverity: SeverityMajor,
			Attribution:     AttrSupplier,
			Confidence:      0.85,
			Reason:          "transport_timeout",
			Details:         "endpoint accepted the connection and did not answer in time",
			MethodBlocking:  true,
		}
	}

	return AnalysisResult{
		ShouldRetry:     domain.IsRetryable(err),
		ShouldPenalize:  true,
		PenaltySeverity: SeverityMinor,
		Attribution:     AttrUnknown,
		Confidence:      0.50,
		Reason:          "transport_error",
		Details:         err.Error(),
	}
}

// isConnectFailure reports whether the error happened before any byte was
// exchanged with the host: a dial that was refused or timed out, a name that
// did not resolve, a TLS handshake that failed.
//
// A dial timeout is a connect failure, not a method timeout — the check for
// Op == "dial" runs before the generic Timeout() check on purpose.
func isConnectFailure(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var recordErr tls.RecordHeaderError
	if errors.As(err, &recordErr) {
		return true
	}
	var certErr x509.CertificateInvalidError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	if errors.As(err, &certErr) || errors.As(err, &unknownAuthority) || errors.As(err, &hostnameErr) {
		return true
	}
	var tlsAlert tls.AlertError
	return errors.As(err, &tlsAlert)
}

// isTimeout reports whether the error is a deadline that fired after the
// connection was established: net/http's Client.Timeout, or a context
// deadline the transport surfaced.
func isTimeout(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}
```

- [ ] **Step 5: Run to verify pass**

Run: `go test ./heuristic/ -run TestAnalyzeTransportError -race -count=1 -v`
Expected: all PASS. If `TestAnalyzeTransportError_DNSIsHostDead` fails because the sandbox resolves `.invalid` (some resolvers do), replace the URL host with `"[::1]:1"` — no, that is a refused dial; instead assert `isConnectFailure` on a hand-built `&net.DNSError{IsNotFound: true}` wrapped in `*url.Error` and keep the live test skipped with `t.Skip` when `net.LookupHost("nonexistent.invalid")` succeeds.

- [ ] **Step 6: Revert-check**

Temporarily change `isConnectFailure` to `return false` → `ConnectRefused` and `DNS` tests must fail. Temporarily make `isTimeout` return false → `TimeoutAfterConnect` must fail. Restore both. Run again: PASS.

- [ ] **Step 7: Lint and commit**

Run: `gofmt -l heuristic/ && go vet ./heuristic/ && golangci-lint run ./heuristic/`

```bash
git add heuristic/transport.go heuristic/transport_test.go heuristic/result.go
git commit -m "feat(heuristic): grade transport errors

A relay that failed before any body existed had no verdict: the Heuristic
middleware returned early, so a hanging host never reached the circuit
breaker and a client hang-up was scored against whichever supplier held
the relay. AnalyzeTransportError gives those attempts one: connect-level
failures are a dead host (critical, ShouldCircuitBreak), a timeout after
connect says the host cannot do THIS method (major, MethodBlocking), and a
client cancel is nobody's fault.

Tests drive real net/http failures against local listeners and wrap them
the way protocol/shannon does, so the classifier is tested on the shapes
production hands it."
```

---

### Task 2: Wire the classifier into the chain

**Files:**
- Modify: `relay/middleware/heuristic.go:17-22`
- Modify: `relay/middleware/observe.go:52-68`
- Test: `relay/middleware/heuristic_test.go`, `relay/middleware/observe_test.go`, `relay/middleware/circuitbreak_test.go`

**Interfaces:**
- Consumes: `heuristic.AnalyzeTransportError` (Task 1), `ctx.HeuristicResult`, `reputation.Signal` (zero value = nothing recorded).
- Produces: after this task, every failed attempt has `ctx.HeuristicResult != nil`; `Observe` records nothing when `HeuristicResult.Attribution == AttrClient` and the relay errored.

- [ ] **Step 1: Failing tests — Heuristic middleware sets a result on transport error**

Append to `relay/middleware/heuristic_test.go`:

```go
// A transport failure used to leave the chain with no verdict at all. Now
// the attempt is graded: the inner error is still returned (retry needs it),
// and ctx.HeuristicResult carries what the failure meant.
func TestHeuristic_TransportErrorIsGraded(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", context.DeadlineExceeded, true)
	})
	h := Heuristic(newFlags("heuristic"))(inner)

	ctx := baseContext()
	err := h.HandleRelay(ctx)
	if err == nil {
		t.Fatal("the transport error must still propagate")
	}
	if ctx.HeuristicResult == nil {
		t.Fatal("transport error left no HeuristicResult")
	}
	if ctx.HeuristicResult.Reason != "transport_timeout" {
		t.Fatalf("reason = %q, want transport_timeout", ctx.HeuristicResult.Reason)
	}
}

// The classifier needs the request context's own error to tell a client
// hang-up from a supplier hang; the middleware must pass it.
func TestHeuristic_ClientCancelIsAttributedToClient(t *testing.T) {
	goCtx, cancel := context.WithCancel(context.Background())
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		cancel()
		return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", context.Canceled, true)
	})
	h := Heuristic(newFlags("heuristic"))(inner)

	ctx := baseContext()
	ctx.Ctx = goCtx
	_ = h.HandleRelay(ctx)
	if ctx.HeuristicResult == nil || ctx.HeuristicResult.Attribution != heuristic.AttrClient {
		t.Fatalf("result = %+v, want AttrClient", ctx.HeuristicResult)
	}
}
```

Add imports `context`, `github.com/pokt-network/sage/domain`, `github.com/pokt-network/sage/heuristic` to the test file if missing.

- [ ] **Step 2: Failing tests — Observe records nothing for a client cancel**

Append to `relay/middleware/observe_test.go`:

```go
// A client hang-up is nobody's fault. The old fallback turned every relay
// error into a MinorError against whichever supplier held the relay — PATH's
// A/B showed those cancels track the slowest operator's tail latency, a
// latency signal misfiled as a fault.
func TestObserve_ClientCancelRecordsNoSignal(t *testing.T) {
	repSvc := &trackingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = ctx.Endpoints[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{Attribution: heuristic.AttrClient, Reason: "client_cancelled"}
		return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", context.Canceled, true)
	})
	mw := Observe(newFlags(), nil, repSvc)
	ctx := baseContext()
	_ = mw(inner).HandleRelay(ctx)

	repSvc.mu.Lock()
	defer repSvc.mu.Unlock()
	if repSvc.called {
		t.Fatalf("a client cancel recorded a %q signal against the supplier", repSvc.last.Type)
	}
}

// The control: a transport timeout graded major must reach reputation as
// major, not as the old undifferentiated minor.
func TestObserve_TransportTimeoutIsMajor(t *testing.T) {
	repSvc := &trackingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = ctx.Endpoints[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			Attribution: heuristic.AttrSupplier, ShouldPenalize: true, PenaltySeverity: heuristic.SeverityMajor, Reason: "transport_timeout",
		}
		return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", context.DeadlineExceeded, true)
	})
	mw := Observe(newFlags(), nil, repSvc)
	_ = mw(inner).HandleRelay(baseContext())

	repSvc.mu.Lock()
	defer repSvc.mu.Unlock()
	if !repSvc.called || repSvc.last.Type != reputation.SignalMajorError {
		t.Fatalf("signal = %v (called=%v), want major_error", repSvc.last.Type, repSvc.called)
	}
}
```

- [ ] **Step 3: Failing test — CircuitBreak sees a connect failure**

Append to `relay/middleware/circuitbreak_test.go`:

```go
// A dead host used to be invisible to the breaker: a transport error carried
// no HeuristicResult, so neither branch of the post-relay switch fired. With
// transport grading, a connect failure is a break candidate like a 5xx.
func TestCircuitBreak_ConnectFailureFeedsTheBreaker(t *testing.T) {
	breaker := circuitbreaker.New(circuitbreaker.WithFailureRateGate(time.Minute, 1, 0))
	eps := testEndpoints(1)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed",
			&net.OpError{Op: "dial", Err: errors.New("connection refused")}, true)
	})
	// Heuristic sits inside CircuitBreak in the real chain; compose them so the
	// verdict is produced by the production classifier, not stubbed.
	h := CircuitBreak(breaker, nil, newFlags("circuit_breaker"), nil)(Heuristic(newFlags("heuristic"))(inner))

	ctx := baseContext()
	ctx.Endpoints = eps
	_ = h.HandleRelay(ctx)

	if !breaker.IsBroken("eth", eps[0].Domain()) {
		t.Fatal("a refused connection must reach the breaker")
	}
}
```

Add imports `errors`, `net`, `time` as needed.

- [ ] **Step 4: Run to verify failure**

Run: `go test ./relay/middleware/ -run 'TestHeuristic_TransportError|TestHeuristic_ClientCancel|TestObserve_ClientCancel|TestObserve_TransportTimeout|TestCircuitBreak_ConnectFailure' -count=1`
Expected: FAIL (nil HeuristicResult; signal recorded; domain not broken).

- [ ] **Step 5: Implement — Heuristic middleware**

In `relay/middleware/heuristic.go` replace:

```go
			// Run the inner chain first.
			if err := next.HandleRelay(ctx); err != nil {
				return err
			}
```

with:

```go
			// Run the inner chain first.
			if err := next.HandleRelay(ctx); err != nil {
				// No body to analyse, but the failure itself is evidence: a
				// dead host, a host that cannot do this method, or a client
				// that hung up. Without a verdict here none of that reached
				// the breaker, the method blocks, or reputation correctly.
				// The flag gate below is deliberately not applied: grading a
				// transport error is attribution, not response analysis.
				result := heuristic.AnalyzeTransportError(err, ctx.Ctx.Err())
				ctx.HeuristicResult = &result
				return err
			}
```

- [ ] **Step 6: Implement — Observe**

In `relay/middleware/observe.go`, at the top of `buildSignal`:

```go
	// A failure the client caused is nobody's signal. successResult also
	// carries AttrClient, but with no error, so key on both.
	if relayErr != nil && ctx.HeuristicResult != nil && ctx.HeuristicResult.Attribution == heuristic.AttrClient {
		return reputation.Signal{}
	}
```

and in the middleware body change the recording block to skip a zero signal:

```go
			if ctx.Endpoint != "" {
				sig := buildSignal(ctx, err, latency)
				if sig.Type != "" {
					_ = repSvc.RecordSignal(context.Background(), ctx.ServiceID, ctx.Endpoint, ctx.RPCType, sig)
				}
			}
```

- [ ] **Step 7: Run to verify pass, then the whole middleware package**

Run: `go test ./relay/middleware/ -race -count=1`
Expected: PASS. If an existing test asserted a MinorError on a bare transport error with no `HeuristicResult`, it still passes: the fallback path is unchanged when `HeuristicResult` is nil.

- [ ] **Step 8: Revert-check**

Revert Step 5 (restore the plain `return err`) → the two Heuristic tests and the CircuitBreak test must fail. Revert Step 6 → `TestObserve_ClientCancelRecordsNoSignal` must fail. Restore both.

- [ ] **Step 9: Commit**

```bash
git add relay/middleware/heuristic.go relay/middleware/observe.go relay/middleware/*_test.go
git commit -m "fix(middleware): give transport failures a verdict

The Heuristic middleware returned early on a transport error, so a refused
connection never reached the circuit breaker and every relay error — a
client hang-up included — became a minor reputation error against the
supplier holding the relay. Transport errors are now graded on the way out,
and Observe records nothing for a failure attributed to the client."
```

---

### Task 3: `MethodBlocking` on method-level JSON-RPC errors

**Files:**
- Modify: `heuristic/protocol.go:95-107` (`-32601`), `heuristic/protocol.go:227-245` (server-error patterns)
- Modify: `heuristic/indicators.go` (two entries)
- Test: `heuristic/method_unsupported_test.go` (new)

**Interfaces:**
- Consumes: `classifyJSONRPCError`, `classifyServerError`, `indicators` table.
- Produces: `MethodBlocking == true` for `-32601` and for the wordings `"api is not supported"`, `"lite fullnode"`, `"method not supported"`, `"does not exist/is not available"`. `MethodBlocking == false` for every `capabilityLimitationPatterns` entry that is historical-state and for `"excluded from account secondary indexes"`.

- [ ] **Step 1: Failing test**

Create `heuristic/method_unsupported_test.go`:

```go
package heuristic

import (
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// An endpoint answering "I do not serve this method" is the structured form
// of a method timeout: not a fault, but this host should not get that method
// again for a while. Historical-state wordings are per BLOCK and Solana's
// index exclusion is per PROGRAM; marking on those would exclude a host from
// every eth_getBalance for one honest pruned-state answer.
func TestAnalyze_MethodBlocking(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{"-32601 method not found", `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method debug_traceTransaction does not exist/is not available"}}`, true},
		{"tron lite fullnode (tier 2)", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"this api is not supported on lite fullnode"}}`, true},
		{"api not supported (tier 3, unparsed)", `{"note":"upstream said: api is not supported"}`, true},
		{"pruned state is per block, not per method", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"missing trie node abc"}}`, false},
		{"pbss pruned state", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"metadata is not found, 0x14a5f1c"}}`, false},
		{"solana index exclusion is per program", `{"jsonrpc":"2.0","id":1,"error":{"code":-32010,"message":"Tokenkeg excluded from account secondary indexes; this RPC method unavailable for key"}}`, false},
		{"plain success", `{"jsonrpc":"2.0","id":1,"result":"0x1"}`, false},
		{"5xx", ``, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := http.StatusOK
			if tt.name == "5xx" {
				status = http.StatusBadGateway
			}
			r := Analyze([]byte(tt.body), status, domain.RPCTypeJSONRPC)
			if r.MethodBlocking != tt.want {
				t.Fatalf("MethodBlocking = %v, want %v (%+v)", r.MethodBlocking, tt.want, r)
			}
		})
	}
}

// -32601 keeps its client attribution and no-retry: a bogus method name must
// not bounce across every host in the pool. The mark is what changes — the
// NEXT request for that method goes elsewhere.
func TestAnalyze_MethodNotFoundStillDoesNotRetry(t *testing.T) {
	r := Analyze([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`), http.StatusOK, domain.RPCTypeJSONRPC)
	if r.ShouldRetry || r.ShouldPenalize || r.Attribution != AttrClient {
		t.Fatalf("-32601 grading changed: %+v", r)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./heuristic/ -run 'TestAnalyze_MethodBlocking|TestAnalyze_MethodNotFoundStillDoesNotRetry' -count=1`
Expected: FAIL on the three `want: true` rows.

- [ ] **Step 3: Implement**

In `heuristic/protocol.go`:

1. In the `-32601` case add `MethodBlocking: true,` to the returned `AnalysisResult`.
2. Above `capabilityLimitationPatterns` add:

```go
// methodUnsupportedPatterns are wordings in which an endpoint reports that it
// does not serve the METHOD asked for — a namespace it was started without,
// an API a lite node does not expose. Unlike the historical-state wordings
// below, which are about one block, these are about the method, so they set
// MethodBlocking: the host should not receive that method again for a while.
var methodUnsupportedPatterns = []string{
	"api is not supported",
	"lite fullnode",
	"method not supported",
	"does not exist/is not available",
	"is not available on this node",
}

// reportsMethodUnsupported reports whether message is one of the
// methodUnsupportedPatterns wordings. Case-insensitive on the caller's behalf.
func reportsMethodUnsupported(lowerMsg string) bool {
	for _, pattern := range methodUnsupportedPatterns {
		if strings.Contains(lowerMsg, pattern) {
			return true
		}
	}
	return false
}
```

3. In `classifyServerError`, inside the `blockchainErrorPatterns` match, build the result into a variable and set `result.MethodBlocking = reportsMethodUnsupported(lowerMsg)` before returning it. Do the same in `classifyInternalError` if it has a blockchain-pattern branch; if it does not, leave it.

In `heuristic/indicators.go`, add a field `methodBlocking bool` to `indicator`, set it `true` on the `"lite fullnode"` and `"api is not supported"` entries, and in `matchIndicator` copy it: `MethodBlocking: ind.methodBlocking,`.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./heuristic/ -race -count=1`
Expected: PASS, including the existing PBSS and Solana index tests.

- [ ] **Step 5: Revert-check**

Make `reportsMethodUnsupported` return `false` and remove `MethodBlocking: true` from the `-32601` case → three rows fail. Restore.

- [ ] **Step 6: Commit**

```bash
git add heuristic/
git commit -m "feat(heuristic): mark method-level unsupported errors as MethodBlocking

-32601 and the \"api is not supported\" / lite-fullnode wordings mean the
host does not serve this METHOD. They now set MethodBlocking so the method
blocks can route the next such request elsewhere. Historical-state wordings
(per block) and Solana's index exclusion (per program) deliberately do not."
```

---

### Task 4: `qos.MethodNormalizer` + EVM catalogue

**Files:**
- Modify: `qos/plugin.go` (after `DataExtractor`)
- Create: `qos/evm/methods.go`, `qos/evm/methods_test.go`
- Modify: `qos/evm/plugin.go:44-62` (fold `coalescableMethods` into the catalogue by reference)

**Interfaces:**
- Produces:
  ```go
  const MethodOther = "_other"
  type MethodNormalizer interface { NormalizeMethod(payload domain.Payload) string }
  ```
  and `(*evm.Plugin).NormalizeMethod`.

- [ ] **Step 1: Add the interface**

In `qos/plugin.go` after the `DataExtractor` block:

```go
// MethodOther is the bucket NormalizeMethod returns for a method the plugin
// does not catalogue. One bucket, so unknown methods cost one key, not one
// per client-chosen string.
const MethodOther = "_other"

// MethodNormalizer is implemented by plugins that can name a payload's method
// from a bounded set. The returned string is a key in method-aware state and
// a metric label, so it must come from the plugin's own catalogue — never
// from the request verbatim. Only a label's value SET bounds it; a sanitizer
// bounds shape, not set.
type MethodNormalizer interface {
	// NormalizeMethod returns the catalogued name, MethodOther for a method
	// the plugin does not list, or "" when the payload has no method notion
	// at all (a raw body under a plugin that does not parse it).
	NormalizeMethod(payload domain.Payload) string
}
```

- [ ] **Step 2: Failing tests**

Create `qos/evm/methods_test.go`:

```go
package evm

import (
	"sort"
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

func TestNormalizeMethod(t *testing.T) {
	p := NewPlugin(nil, Config{})
	cases := []struct{ method, want string }{
		{"eth_getLogs", "eth_getLogs"},
		{"eth_call", "eth_call"},
		{"debug_traceTransaction", "debug_traceTransaction"},
		{"eth_definitelyNotAMethod", qos.MethodOther},
		{"", ""},
	}
	for _, tc := range cases {
		got := p.NormalizeMethod(domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, tc.method))
		if got != tc.want {
			t.Errorf("NormalizeMethod(%q) = %q, want %q", tc.method, got, tc.want)
		}
	}
}

// Every method the plugin reasons about elsewhere must be in the catalogue,
// or a coalescable/archival method would normalise to _other and share one
// block with every unknown method.
func TestKnownMethods_CoverOtherLists(t *testing.T) {
	for m := range coalescableMethods {
		if !knownMethods[m] {
			t.Errorf("coalescable %q missing from knownMethods", m)
		}
	}
	for m := range methodsWithBlockParam {
		if !knownMethods[m] {
			t.Errorf("block-param %q missing from knownMethods", m)
		}
	}
}

// Golden: the catalogue is a label value set. Growth must be a diff someone
// reads, not a runtime surprise.
func TestKnownMethods_Golden(t *testing.T) {
	names := make([]string, 0, len(knownMethods))
	for m := range knownMethods {
		names = append(names, m)
	}
	sort.Strings(names)
	got := strings.Join(names, "\n")
	if got != knownMethodsGolden {
		t.Fatalf("knownMethods changed; update knownMethodsGolden in this test if intended.\n--- got ---\n%s", got)
	}
}
```

Set `knownMethodsGolden` after Step 4 by copying the test's `got` output into a `const knownMethodsGolden = \`...\`` at the bottom of the test file.

- [ ] **Step 3: Run to verify failure**

Run: `go test ./qos/evm/ -run 'TestNormalizeMethod|TestKnownMethods' -count=1`
Expected: build failure, `undefined: knownMethods`.

- [ ] **Step 4: Implement**

Create `qos/evm/methods.go`:

```go
package evm

import (
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// knownMethods is the EVM method catalogue: every JSON-RPC method the plugin
// will name in method-aware state and metric labels. A method not listed
// normalises to qos.MethodOther. The other per-method sets in this package
// (coalescableMethods, methodsWithBlockParam) are subsets, checked by test.
var knownMethods = map[string]bool{
	// eth namespace — standard read set
	"eth_blockNumber": true, "eth_chainId": true, "eth_gasPrice": true,
	"eth_maxPriorityFeePerGas": true, "eth_feeHistory": true, "eth_syncing": true,
	"eth_getBalance": true, "eth_getCode": true, "eth_getStorageAt": true,
	"eth_getTransactionCount": true, "eth_call": true, "eth_estimateGas": true,
	"eth_getBlockByNumber": true, "eth_getBlockByHash": true,
	"eth_getBlockTransactionCountByNumber": true, "eth_getBlockTransactionCountByHash": true,
	"eth_getUncleCountByBlockNumber": true, "eth_getUncleCountByBlockHash": true,
	"eth_getUncleByBlockNumberAndIndex": true, "eth_getUncleByBlockHashAndIndex": true,
	"eth_getTransactionByHash": true, "eth_getTransactionByBlockNumberAndIndex": true,
	"eth_getTransactionByBlockHashAndIndex": true, "eth_getTransactionReceipt": true,
	"eth_getBlockReceipts": true, "eth_getLogs": true, "eth_getProof": true,
	"eth_sendRawTransaction": true, "eth_createAccessList": true, "eth_blobBaseFee": true,
	"eth_newFilter": true, "eth_newBlockFilter": true, "eth_getFilterChanges": true,
	"eth_getFilterLogs": true, "eth_uninstallFilter": true, "eth_subscribe": true, "eth_unsubscribe": true,
	// net / web3
	"net_version": true, "net_listening": true, "net_peerCount": true,
	"web3_clientVersion": true, "web3_sha3": true,
	// debug / trace — the namespaces most often absent on a host
	"debug_traceTransaction": true, "debug_traceBlockByNumber": true, "debug_traceBlockByHash": true,
	"debug_traceCall": true, "debug_getRawReceipts": true,
	"trace_block": true, "trace_transaction": true, "trace_call": true, "trace_filter": true,
	"trace_replayTransaction": true, "trace_replayBlockTransactions": true,
}

// NormalizeMethod implements qos.MethodNormalizer.
func (p *Plugin) NormalizeMethod(payload domain.Payload) string {
	m := payload.Method()
	if m == "" {
		return ""
	}
	if knownMethods[m] {
		return m
	}
	return qos.MethodOther
}
```

Then run the golden test once, copy its printed list into `const knownMethodsGolden` in the test file.

- [ ] **Step 5: Run to verify pass**

Run: `go test ./qos/evm/ -race -count=1`
Expected: PASS.

- [ ] **Step 6: Add `qos.MethodNormalizer` to the interface list in the `Plugin` doc comment** in `qos/evm/plugin.go` (the comment enumerates implemented interfaces) and commit.

```bash
git add qos/plugin.go qos/evm/methods.go qos/evm/methods_test.go qos/evm/plugin.go
git commit -m "feat(qos): MethodNormalizer, with the EVM catalogue

Method-aware state and metric labels need a bounded method vocabulary that
SAGE owns; the client's string is unbounded. Plugins that can name a
payload's method implement MethodNormalizer; unknown methods share one
_other bucket. A golden test pins the EVM list so growth is a reviewed
diff."
```

---

### Task 5: Solana catalogue

**Files:**
- Create: `qos/solana/methods.go`, `qos/solana/methods_test.go`

**Interfaces:**
- Consumes: `qos.MethodNormalizer`, `qos.MethodOther`, `domain.Payload.Method()`.
- Produces: `(*solana.Plugin).NormalizeMethod`.

- [ ] **Step 1: Failing tests** — same three tests as Task 4 (`TestNormalizeMethod`, `TestKnownMethods_CoverOtherLists` over `coalescableMethods` only, `TestKnownMethods_Golden`), constructed with the package's `NewPlugin` (check its signature in `qos/solana/plugin.go` and pass zero-value config + `nil` logger). Cases: `getProgramAccounts`, `getSlot`, `getMultipleAccounts` → themselves; `getSomethingFake` → `_other`; `""` → `""`.

- [ ] **Step 2: Run to verify failure** — `go test ./qos/solana/ -run 'TestNormalizeMethod|TestKnownMethods' -count=1` → `undefined: knownMethods`.

- [ ] **Step 3: Implement** `qos/solana/methods.go`:

```go
package solana

import (
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// knownMethods is the Solana method catalogue; see qos.MethodNormalizer.
// coalescableMethods is a subset, checked by test.
var knownMethods = map[string]bool{
	"getAccountInfo": true, "getBalance": true, "getBlock": true, "getBlockHeight": true,
	"getBlockProduction": true, "getBlockCommitment": true, "getBlocks": true, "getBlocksWithLimit": true,
	"getBlockTime": true, "getClusterNodes": true, "getEpochInfo": true, "getEpochSchedule": true,
	"getFeeForMessage": true, "getFirstAvailableBlock": true, "getGenesisHash": true, "getHealth": true,
	"getHighestSnapshotSlot": true, "getIdentity": true, "getInflationGovernor": true, "getInflationRate": true,
	"getInflationReward": true, "getLargestAccounts": true, "getLatestBlockhash": true, "getLeaderSchedule": true,
	"getMaxRetransmitSlot": true, "getMaxShredInsertSlot": true, "getMinimumBalanceForRentExemption": true,
	"getMultipleAccounts": true, "getProgramAccounts": true, "getRecentBlockhash": true,
	"getRecentPerformanceSamples": true, "getRecentPrioritizationFees": true, "getSignatureStatuses": true,
	"getSignaturesForAddress": true, "getSlot": true, "getSlotLeader": true, "getSlotLeaders": true,
	"getStakeMinimumDelegation": true, "getSupply": true, "getTokenAccountBalance": true,
	"getTokenAccountsByDelegate": true, "getTokenAccountsByOwner": true, "getTokenLargestAccounts": true,
	"getTokenSupply": true, "getTransaction": true, "getTransactionCount": true, "getVersion": true,
	"getVoteAccounts": true, "isBlockhashValid": true, "minimumLedgerSlot": true, "requestAirdrop": true,
	"sendTransaction": true, "simulateTransaction": true,
}

// NormalizeMethod implements qos.MethodNormalizer.
func (p *Plugin) NormalizeMethod(payload domain.Payload) string {
	m := payload.Method()
	if m == "" {
		return ""
	}
	if knownMethods[m] {
		return m
	}
	return qos.MethodOther
}
```

- [ ] **Step 4: Run, fill the golden, run again** — `go test ./qos/solana/ -race -count=1` → PASS.

- [ ] **Step 5: Commit**

```bash
git add qos/solana/methods.go qos/solana/methods_test.go
git commit -m "feat(qos/solana): method catalogue for MethodNormalizer"
```

---

### Task 6: Cosmos catalogue with REST path templating

**Files:**
- Create: `qos/cosmos/methods.go`, `qos/cosmos/methods_test.go`

**Interfaces:**
- Consumes: `cometBFTMethods` (`qos/cosmos/parser.go`), `domain.Payload.Method()`, `domain.Payload.Path()`, `domain.Payload.RPCType()`, `domain.RPCTypeCometBFT`, `domain.RPCTypeREST` (confirm exact constant names in `domain/rpctype.go`).
- Produces: `(*cosmos.Plugin).NormalizeMethod`, `templatePath(path string) string`.

- [ ] **Step 1: Failing tests**

```go
package cosmos

import (
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

func TestTemplatePath(t *testing.T) {
	cases := []struct{ in, want string }{
		{"/cosmos/tx/v1beta1/txs/block/12345", "/cosmos/tx/v1beta1/txs/block/:var"},
		{"/cosmos/bank/v1beta1/balances/cosmos1abcdefghijklmnopqrstuvwxyz0123456789", "/cosmos/bank/v1beta1/balances/:var"},
		{"/cosmos/tx/v1beta1/txs/0xDEADBEEF", "/cosmos/tx/v1beta1/txs/:var"},
		{"/cosmos/tx/v1beta1/txs/A1B2C3D4E5F60718293A4B5C6D7E8F90A1B2C3D4E5F60718293A4B5C6D7E8F90", "/cosmos/tx/v1beta1/txs/:var"},
		{"/cosmos/base/tendermint/v1beta1/blocks/latest", "/cosmos/base/tendermint/v1beta1/blocks/latest"},
		{"/status?height=5", "/status"},
	}
	for _, tc := range cases {
		if got := templatePath(tc.in); got != tc.want {
			t.Errorf("templatePath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestNormalizeMethod(t *testing.T) {
	p := NewPlugin(nil, Config{}) // adjust to the real constructor signature
	jsonrpc := func(m string) domain.Payload { return domain.NewPayload([]byte(`{}`), domain.RPCTypeCometBFT, m) }
	rest := func(path string) domain.Payload {
		return domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP(path, "GET")
	}
	cases := []struct {
		name string
		p    domain.Payload
		want string
	}{
		{"cometbft method", jsonrpc("block_results"), "block_results"},
		{"unknown cometbft method", jsonrpc("nope"), qos.MethodOther},
		{"catalogued rest template", rest("/cosmos/tx/v1beta1/txs/block/77"), "/cosmos/tx/v1beta1/txs/block/:var"},
		{"unlisted rest path", rest("/osmosis/gamm/v1beta1/pools/1"), qos.MethodOther},
		{"no method, no path", domain.NewPayload(nil, domain.RPCTypeREST, ""), ""},
	}
	for _, tc := range cases {
		if got := p.NormalizeMethod(tc.p); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
```

- [ ] **Step 2: Run to verify failure** — `undefined: templatePath`.

- [ ] **Step 3: Implement** `qos/cosmos/methods.go`:

```go
package cosmos

import (
	"strings"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// knownRESTTemplates is the catalogued set of gRPC-gateway paths, with every
// variable segment written as :var. A request path is templated (see
// templatePath) and then looked up here; anything unlisted is MethodOther.
var knownRESTTemplates = map[string]bool{
	"/cosmos/base/tendermint/v1beta1/blocks/latest":       true,
	"/cosmos/base/tendermint/v1beta1/blocks/:var":         true,
	"/cosmos/base/tendermint/v1beta1/node_info":           true,
	"/cosmos/base/tendermint/v1beta1/syncing":             true,
	"/cosmos/base/tendermint/v1beta1/validatorsets/latest": true,
	"/cosmos/base/tendermint/v1beta1/validatorsets/:var":  true,
	"/cosmos/bank/v1beta1/balances/:var":                  true,
	"/cosmos/bank/v1beta1/balances/:var/by_denom":         true,
	"/cosmos/bank/v1beta1/supply":                         true,
	"/cosmos/auth/v1beta1/accounts/:var":                  true,
	"/cosmos/auth/v1beta1/account_info/:var":              true,
	"/cosmos/tx/v1beta1/txs":                              true,
	"/cosmos/tx/v1beta1/txs/:var":                         true,
	"/cosmos/tx/v1beta1/txs/block/:var":                   true,
	"/cosmos/tx/v1beta1/simulate":                         true,
	"/cosmos/staking/v1beta1/validators":                  true,
	"/cosmos/staking/v1beta1/validators/:var":             true,
	"/cosmos/staking/v1beta1/delegations/:var":            true,
	"/cosmos/distribution/v1beta1/delegators/:var/rewards": true,
	"/cosmos/gov/v1/proposals":                            true,
	"/cosmos/gov/v1/proposals/:var":                       true,
	"/cosmos/gov/v1beta1/proposals":                       true,
	"/cosmos/gov/v1beta1/proposals/:var":                  true,
	"/ibc/core/channel/v1/channels":                       true,
	"/ibc/core/client/v1/client_states":                   true,
	"/ibc/apps/transfer/v1/denom_traces":                  true,
	"/ibc/apps/transfer/v1/denom_traces/:var":             true,
}

// templatePath replaces every path segment that is a number, a hex string
// (optionally 0x-prefixed, at least 8 hex digits), or a bech32 address
// (a lowercase prefix followed by '1' and 38 or more chars) with :var, and
// drops the query string. What survives is the route, which is ours to
// catalogue; what is removed is the client's, which is not.
func templatePath(path string) string {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	segs := strings.Split(path, "/")
	for i, s := range segs {
		if isVariableSegment(s) {
			segs[i] = ":var"
		}
	}
	return strings.Join(segs, "/")
}

func isVariableSegment(s string) bool {
	if s == "" {
		return false
	}
	if isDigits(s) {
		return true
	}
	h := strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	if len(h) >= 8 && isHex(h) {
		return true
	}
	// bech32: hrp + '1' + data, lowercase, long. Cosmos addresses are ≥ 39.
	if i := strings.LastIndexByte(s, '1'); i > 0 && len(s)-i >= 39 && s == strings.ToLower(s) {
		return true
	}
	return false
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

// NormalizeMethod implements qos.MethodNormalizer. CometBFT JSON-RPC methods
// come from cometBFTMethods verbatim; REST paths are templated and matched
// against knownRESTTemplates.
func (p *Plugin) NormalizeMethod(payload domain.Payload) string {
	if m := payload.Method(); m != "" {
		if cometBFTMethods[m] {
			return m
		}
		return qos.MethodOther
	}
	path := payload.Path()
	if path == "" {
		return ""
	}
	tpl := templatePath(path)
	if knownRESTTemplates[tpl] {
		return tpl
	}
	if cometBFTPath := strings.TrimPrefix(tpl, "/"); cometBFTMethods[cometBFTPath] {
		return cometBFTPath // GET /status is the same method as {"method":"status"}
	}
	return qos.MethodOther
}
```

Check how `parseRequest` in `qos/cosmos/parser.go` sets `Method()` for CometBFT GET-path requests; if it already sets the method name from the path, the `cometBFTPath` branch is redundant — keep whichever the parser does not already do, and add a test row for `GET /status` either way.

- [ ] **Step 4: Run to verify pass** — `go test ./qos/cosmos/ -race -count=1` → PASS. Add a golden test for `knownRESTTemplates` as in Task 4.

- [ ] **Step 5: Commit**

```bash
git add qos/cosmos/methods.go qos/cosmos/methods_test.go
git commit -m "feat(qos/cosmos): method catalogue with REST path templating

CometBFT methods are named verbatim; gRPC-gateway paths are templated by
replacing numeric, hex and bech32 segments with :var and matched against a
listed set, so the client's identifiers never become a key."
```

---

### Task 7: `methodblock.Store`

**Files:**
- Create: `methodblock/doc.go`, `methodblock/store.go`, `methodblock/store_test.go`

**Interfaces:**
- Produces:
  ```go
  type Block struct { Host, Method string; Expiry time.Time } // Method == "" is a host-level block
  type Option func(*Store)
  func WithTTL(d time.Duration) Option
  func WithEscalation(n int) Option
  func WithLogger(l *slog.Logger) Option
  func New(opts ...Option) *Store
  func (s *Store) Blocked(service, host, method string) bool
  func (s *Store) Mark(service, host, method string) (escalated bool)
  func (s *Store) Clear(service string) int
  func (s *Store) Active(service string) []Block
  func (s *Store) StartSweep(ctx context.Context)
  const DefaultTTL = 5 * time.Minute
  const DefaultEscalation = 3
  ```
  TTL ≤ 0 disables marking (`Mark` is a no-op, `Blocked` always false). Escalation ≤ 0 never escalates.

- [ ] **Step 1: Failing tests**

Create `methodblock/store_test.go`:

```go
package methodblock

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestStore_MarkBlocksOnlyThatMethod(t *testing.T) {
	s := New()
	s.Mark("solana", "slow.example.com", "getProgramAccounts")

	if !s.Blocked("solana", "slow.example.com", "getProgramAccounts") {
		t.Fatal("marked method must be blocked")
	}
	if s.Blocked("solana", "slow.example.com", "getSlot") {
		t.Fatal("a different method on the same host must not be blocked")
	}
	if s.Blocked("solana", "other.example.com", "getProgramAccounts") {
		t.Fatal("a different host must not be blocked")
	}
	if s.Blocked("eth", "slow.example.com", "getProgramAccounts") {
		t.Fatal("a different service must not be blocked")
	}
}

func TestStore_MarkExpires(t *testing.T) {
	s := New(WithTTL(20 * time.Millisecond))
	s.Mark("eth", "h.example.com", "eth_getLogs")
	time.Sleep(30 * time.Millisecond)
	if s.Blocked("eth", "h.example.com", "eth_getLogs") {
		t.Fatal("mark must expire after the TTL")
	}
}

// A re-mark refreshes to one TTL from now; it never extends past that. A
// method mark is cheap to be wrong about, so it must not accumulate.
func TestStore_RemarkRefreshesDoesNotExtend(t *testing.T) {
	s := New(WithTTL(time.Hour))
	s.Mark("eth", "h.example.com", "eth_getLogs")
	first := s.Active("eth")[0].Expiry
	time.Sleep(5 * time.Millisecond)
	s.Mark("eth", "h.example.com", "eth_getLogs")
	second := s.Active("eth")[0].Expiry
	if !second.After(first) {
		t.Fatal("re-mark must refresh the expiry")
	}
	if second.Sub(time.Now()) > time.Hour+time.Second {
		t.Fatal("re-mark must not extend past one TTL from now")
	}
	if len(s.Active("eth")) != 1 {
		t.Fatalf("re-mark created a second block: %+v", s.Active("eth"))
	}
}

func TestStore_ThirdDistinctMethodEscalatesToHost(t *testing.T) {
	s := New()
	if s.Mark("eth", "dead.example.com", "eth_getLogs") {
		t.Fatal("first mark must not escalate")
	}
	if s.Mark("eth", "dead.example.com", "eth_call") {
		t.Fatal("second mark must not escalate")
	}
	if !s.Mark("eth", "dead.example.com", "eth_getBalance") {
		t.Fatal("third distinct method must escalate")
	}
	if !s.Blocked("eth", "dead.example.com", "eth_blockNumber") {
		t.Fatal("host block must cover a method that was never marked")
	}
	active := s.Active("eth")
	if len(active) != 1 || active[0].Method != "" {
		t.Fatalf("escalation must collapse the method marks into one host block, got %+v", active)
	}
}

// Three re-marks of ONE method are not three methods.
func TestStore_RemarksOfOneMethodDoNotEscalate(t *testing.T) {
	s := New()
	for i := 0; i < 5; i++ {
		if s.Mark("eth", "h.example.com", "eth_getLogs") {
			t.Fatalf("re-mark %d escalated", i)
		}
	}
	if s.Blocked("eth", "h.example.com", "eth_call") {
		t.Fatal("host must not be blocked")
	}
}

func TestStore_EscalationThresholdZeroNeverEscalates(t *testing.T) {
	s := New(WithEscalation(0))
	for _, m := range []string{"a", "b", "c", "d"} {
		if s.Mark("eth", "h.example.com", m) {
			t.Fatal("escalation disabled must never escalate")
		}
	}
	if s.Blocked("eth", "h.example.com", "e") {
		t.Fatal("host must not be blocked")
	}
}

func TestStore_TTLZeroDisablesMarking(t *testing.T) {
	s := New(WithTTL(0))
	s.Mark("eth", "h.example.com", "eth_getLogs")
	if s.Blocked("eth", "h.example.com", "eth_getLogs") {
		t.Fatal("TTL <= 0 must disable marking entirely")
	}
}

func TestStore_ClearDropsMarksAndEscalationState(t *testing.T) {
	s := New()
	s.Mark("eth", "h.example.com", "a")
	s.Mark("eth", "h.example.com", "b")
	if n := s.Clear("eth"); n != 2 {
		t.Fatalf("Clear returned %d, want 2", n)
	}
	if s.Blocked("eth", "h.example.com", "a") {
		t.Fatal("cleared mark still blocks")
	}
	if s.Mark("eth", "h.example.com", "c") {
		t.Fatal("a mark after Clear must count as the first, not the third")
	}
}

func TestStore_EmptyHostOrMethodIsNoop(t *testing.T) {
	s := New()
	s.Mark("eth", "", "a")
	s.Mark("eth", "h.example.com", "")
	if len(s.Active("eth")) != 0 {
		t.Fatalf("empty key produced a block: %+v", s.Active("eth"))
	}
}

func TestStore_SweepDropsExpiredHosts(t *testing.T) {
	s := New(WithTTL(10 * time.Millisecond))
	s.Mark("eth", "h.example.com", "a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.sweepInterval = 5 * time.Millisecond
	s.StartSweep(ctx)
	time.Sleep(40 * time.Millisecond)
	s.mu.RLock()
	_, present := s.byService["eth"]["h.example.com"]
	s.mu.RUnlock()
	if present {
		t.Fatal("sweep must remove a host whose every mark expired")
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				s.Mark("eth", "h.example.com", string(rune('a'+i)))
				s.Blocked("eth", "h.example.com", "z")
				s.Active("eth")
			}
		}(i)
	}
	wg.Wait()
}
```

(`go` in a `_test.go` is fine: the no-bare-`go` rule is for production code.)

- [ ] **Step 2: Run to verify failure** — `go test ./methodblock/ -count=1` → `undefined: New`.

- [ ] **Step 3: Implement**

`methodblock/doc.go`:

```go
// Package methodblock remembers that a host could not answer a method
// recently, so selection can route that method elsewhere while the host
// keeps serving everything else.
//
// A mark is host × method × expiry. Marks come from the method_blocks
// middleware after an attempt whose verdict was MethodBlocking (a timeout
// after connect, or an endpoint saying it does not serve the method). A host
// that accumulates escalation-many distinct method marks inside one TTL is
// not slow on something, it is dead: it is blocked for every method for one
// TTL, and the method marks are folded into that.
//
// Local memory only. One mark is one relay_timeout of evidence, not worth a
// Redis round trip, and the gateway must run without Redis anyway.
package methodblock
```

`methodblock/store.go`:

```go
package methodblock

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/pokt-network/sage/internal/safego"
)

// Defaults. TTL is short on purpose: a method mark is unverified and cheap to
// be wrong about, and a host re-proves itself with one relay when it lapses.
const (
	DefaultTTL        = 5 * time.Minute
	DefaultEscalation = 3
)

// Block is one active block, for the collector and the admin API. Method is
// "" for a host-level block.
type Block struct {
	Host   string    `json:"host"`
	Method string    `json:"method,omitempty"`
	Expiry time.Time `json:"expiry"`
}

// hostState is one host's marks within one service.
type hostState struct {
	hostUntil time.Time            // host-level block; zero = none
	methods   map[string]time.Time // method -> expiry
}

// Store holds method blocks for every service in the process.
type Store struct {
	ttl           time.Duration
	escalation    int
	logger        *slog.Logger
	sweepInterval time.Duration

	mu        sync.RWMutex
	byService map[string]map[string]*hostState // service -> host -> state
}

// Option configures a Store.
type Option func(*Store)

// WithTTL sets how long a mark lasts. Zero or negative disables marking.
func WithTTL(d time.Duration) Option { return func(s *Store) { s.ttl = d } }

// WithEscalation sets how many distinct methods must be marked on one host
// inside one TTL before the host is blocked for every method. Zero or
// negative never escalates.
func WithEscalation(n int) Option { return func(s *Store) { s.escalation = n } }

// WithLogger sets the logger used by the sweep goroutine.
func WithLogger(l *slog.Logger) Option { return func(s *Store) { s.logger = l } }

// New returns a Store with the defaults, then the options applied.
func New(opts ...Option) *Store {
	s := &Store{
		ttl:           DefaultTTL,
		escalation:    DefaultEscalation,
		logger:        slog.Default(),
		sweepInterval: DefaultTTL,
		byService:     make(map[string]map[string]*hostState),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Blocked reports whether host must not receive method for service right
// now: a live host-level block, or a live mark on exactly this method.
// Called per candidate per attempt, so it takes only the read lock and
// allocates nothing.
func (s *Store) Blocked(service, host, method string) bool {
	if s.ttl <= 0 {
		return false
	}
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	h := s.byService[service][host]
	if h == nil {
		return false
	}
	if now.Before(h.hostUntil) {
		return true
	}
	return now.Before(h.methods[method])
}

// Mark records that host could not answer method. It returns true when this
// mark escalated the host to a host-level block. A re-mark refreshes the
// expiry to one TTL from now and never extends past that.
func (s *Store) Mark(service, host, method string) (escalated bool) {
	if s.ttl <= 0 || host == "" || method == "" {
		return false
	}
	now := time.Now()
	expiry := now.Add(s.ttl)

	s.mu.Lock()
	defer s.mu.Unlock()

	hosts := s.byService[service]
	if hosts == nil {
		hosts = make(map[string]*hostState)
		s.byService[service] = hosts
	}
	h := hosts[host]
	if h == nil {
		h = &hostState{methods: make(map[string]time.Time)}
		hosts[host] = h
	}
	if now.Before(h.hostUntil) {
		// Already blocked wholesale; a method mark adds nothing.
		return false
	}
	h.methods[method] = expiry

	// Count LIVE distinct methods. Expired ones are dropped here rather than
	// counted, so a host that was marked on three methods over an hour is not
	// escalated for it.
	live := 0
	for m, exp := range h.methods {
		if now.Before(exp) {
			live++
		} else {
			delete(h.methods, m)
		}
	}
	if s.escalation > 0 && live >= s.escalation {
		h.hostUntil = expiry
		h.methods = make(map[string]time.Time)
		return true
	}
	return false
}

// Clear drops every block for a service and returns how many were live. It
// exists so an operator can undo a false positive; the escalation count goes
// with the marks, so the next mark is a first mark.
func (s *Store) Clear(service string) int {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, h := range s.byService[service] {
		if now.Before(h.hostUntil) {
			n++
		}
		for _, exp := range h.methods {
			if now.Before(exp) {
				n++
			}
		}
	}
	delete(s.byService, service)
	return n
}

// Active lists the live blocks for a service, host-level ones with an empty
// Method. Read at scrape time and by the admin API; not on the relay path.
func (s *Store) Active(service string) []Block {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Block
	for host, h := range s.byService[service] {
		if now.Before(h.hostUntil) {
			out = append(out, Block{Host: host, Expiry: h.hostUntil})
			continue
		}
		for m, exp := range h.methods {
			if now.Before(exp) {
				out = append(out, Block{Host: host, Method: m, Expiry: exp})
			}
		}
	}
	return out
}

// StartSweep runs a background pass every sweep interval (one TTL) that drops
// hosts with no live marks, so the map does not grow with every host that
// was ever slow. Expiry itself is lazy on read; this is only memory hygiene.
func (s *Store) StartSweep(ctx context.Context) {
	safego.GoCtx(ctx, s.logger, "methodblock.sweep", func(ctx context.Context) {
		t := time.NewTicker(s.sweepInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				safego.Run(s.logger, "methodblock.sweep.tick", s.sweep)
			}
		}
	})
}

func (s *Store) sweep() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for service, hosts := range s.byService {
		for host, h := range hosts {
			for m, exp := range h.methods {
				if !now.Before(exp) {
					delete(h.methods, m)
				}
			}
			if len(h.methods) == 0 && !now.Before(h.hostUntil) {
				delete(hosts, host)
			}
		}
		if len(hosts) == 0 {
			delete(s.byService, service)
		}
	}
}
```

Check `safego.GoCtx`'s exact signature in `internal/safego/safego.go:126` and `safego.Run` at `:56`; adjust the calls to match.

- [ ] **Step 4: Run to verify pass** — `go test ./methodblock/ -race -count=1` → PASS.

- [ ] **Step 5: Revert-check** — set `live >= s.escalation` to `false` → escalation test fails; skip the `delete` of expired methods in `Mark` and add a test that three marks spaced past the TTL do not escalate (use `WithTTL(10ms)`, sleep 15ms between marks) → it must fail without the delete. Restore.

- [ ] **Step 6: Lint + commit**

```bash
gofmt -l methodblock/ && go vet ./methodblock/ && golangci-lint run ./methodblock/
git add methodblock/
git commit -m "feat(methodblock): host x method block store

Remembers that a host could not answer a method for one TTL, escalates to a
host-level block at three distinct methods inside a TTL, and forgets on
Clear. Local memory only; expiry is lazy on read with a sweep for hygiene."
```

---

### Task 8: Config, flag, chain-order constant

**Files:**
- Modify: `config/service.go` (`GatewayConfig` + new type)
- Modify: `config/testdata/path_config.yaml`, `config/testdata/path_config_unified.yaml`
- Modify: `featureflag/defaults.go`
- Modify: `relay/chain_order.go`, `relay/chain_order_test.go`
- Test: `config/methodblocks_test.go` (new)

**Interfaces:**
- Produces:
  ```go
  type MethodBlocksConfig struct { TTL time.Duration `yaml:"ttl"`; EscalationThreshold int `yaml:"escalation_threshold"` }
  func (m MethodBlocksConfig) EffectiveTTL() time.Duration       // 0 → 5m; <0 → 0 (off)
  func (m MethodBlocksConfig) EffectiveEscalation() int           // 0 → 3; <0 → 0 (never)
  GatewayConfig.MethodBlocks MethodBlocksConfig `yaml:"method_blocks"`
  featureflag.FlagMethodBlocks = "method_blocks" (default true)
  relay.MWMethodBlocks = "method_blocks"
  ```

- [ ] **Step 1: Failing config test**

Create `config/methodblocks_test.go`:

```go
package config

import (
	"testing"
	"time"
)

func TestMethodBlocksConfig_Effective(t *testing.T) {
	cases := []struct {
		cfg     MethodBlocksConfig
		wantTTL time.Duration
		wantEsc int
	}{
		{MethodBlocksConfig{}, 5 * time.Minute, 3},
		{MethodBlocksConfig{TTL: time.Minute, EscalationThreshold: 5}, time.Minute, 5},
		{MethodBlocksConfig{TTL: -1, EscalationThreshold: -1}, 0, 0},
	}
	for _, tc := range cases {
		if got := tc.cfg.EffectiveTTL(); got != tc.wantTTL {
			t.Errorf("%+v: EffectiveTTL = %v, want %v", tc.cfg, got, tc.wantTTL)
		}
		if got := tc.cfg.EffectiveEscalation(); got != tc.wantEsc {
			t.Errorf("%+v: EffectiveEscalation = %d, want %d", tc.cfg, got, tc.wantEsc)
		}
	}
}
```

- [ ] **Step 2: Run to verify failure** — `go test ./config/ -run TestMethodBlocksConfig -count=1` → undefined.

- [ ] **Step 3: Implement config**

In `config/service.go`, add to `GatewayConfig` after `BlockedDomains`:

```go
	// MethodBlocks tunes the per-host, per-method memory consulted at
	// selection: a host that timed out on a method, or said it does not
	// serve it, stops receiving that method for a while and keeps receiving
	// everything else. See MethodBlocksConfig.
	MethodBlocks MethodBlocksConfig `yaml:"method_blocks"`
```

and the type (near `BlockedDomain`):

```go
// MethodBlocksConfig tunes method-aware blocks. Both fields follow the
// zero-is-default, negative-is-off convention.
type MethodBlocksConfig struct {
	// TTL is how long one mark keeps a method away from a host. Zero means
	// 5m; negative disables marking entirely (the middleware still runs and
	// passes everything through). Short on purpose — a mark is one timeout
	// of evidence and a host re-proves itself with one relay when it lapses.
	TTL time.Duration `yaml:"ttl"`
	// EscalationThreshold is how many distinct methods must be marked on one
	// host inside one TTL before the host is blocked for every method. Zero
	// means 3; negative never escalates.
	EscalationThreshold int `yaml:"escalation_threshold"`
}

// EffectiveTTL resolves TTL: zero to the default, negative to off.
func (m MethodBlocksConfig) EffectiveTTL() time.Duration {
	switch {
	case m.TTL < 0:
		return 0
	case m.TTL == 0:
		return 5 * time.Minute
	}
	return m.TTL
}

// EffectiveEscalation resolves EscalationThreshold: zero to the default,
// negative to never.
func (m MethodBlocksConfig) EffectiveEscalation() int {
	switch {
	case m.EscalationThreshold < 0:
		return 0
	case m.EscalationThreshold == 0:
		return 3
	}
	return m.EscalationThreshold
}
```

Add to both fixtures, directly under the `blocked_domains:` block, at the same indentation as `blocked_domains`:

```yaml
  method_blocks:
    ttl: 5m
    escalation_threshold: 3
```

- [ ] **Step 4: Flag and chain constant**

`featureflag/defaults.go`: add `FlagMethodBlocks = "method_blocks"` next to `FlagOperatorAwareSelection`, and `FlagMethodBlocks: true,` in `DefaultFlags`.

`relay/chain_order.go`: add `MWMethodBlocks = "method_blocks"` to the constants; insert `MWMethodBlocks,` between `MWCircuitBreak,` and `MWSelectEndpoint,` in `DefaultChainOrder`; append two `mustPrecede` rules:

```go
		{MWHedge, MWMethodBlocks, "each hedge arm must honour and feed method blocks"},
		{MWMethodBlocks, MWSelectEndpoint, "method_blocks prunes before selection"},
```

Append to `relay/chain_order_test.go`:

```go
func TestValidateChainOrder_MethodBlocksAfterSelectEndpoint(t *testing.T) {
	order := []string{MWParse, MWHedge, MWSelectEndpoint, MWMethodBlocks, MWSendRelay}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "method_blocks") {
		t.Errorf("expected method_blocks→select_endpoint violation, got %v", err)
	}
}

func TestValidateChainOrder_MethodBlocksOutsideHedge(t *testing.T) {
	order := []string{MWParse, MWMethodBlocks, MWHedge, MWSelectEndpoint, MWSendRelay}
	err := ValidateChainOrder(order)
	if err == nil || !strings.Contains(err.Error(), "method_blocks") {
		t.Errorf("expected hedge→method_blocks violation, got %v", err)
	}
}
```

If `canonicalOrder` in the test file is a literal list, add `MWMethodBlocks` to it in the same position.

- [ ] **Step 5: Run** — `go test ./config/ ./featureflag/ ./relay/ -race -count=1` → PASS (including `TestConfigFixtureIsExhaustive` and the canonical-order test).

- [ ] **Step 6: Regenerate docs, commit**

```bash
make docs && git add config/ featureflag/ relay/ docs/configuration.md
git commit -m "feat(config): method_blocks knobs, flag and chain slot

Config carries ttl and escalation_threshold (zero = default, negative = off),
the method_blocks flag ships on in DefaultFlags, and the chain gains a
method_blocks slot between circuit_break and select_endpoint with ordering
rules that keep it inside hedge and ahead of selection."
```

---

### Task 9: Copy-on-filter helper, applied to CircuitBreak

**Files:**
- Create: `relay/middleware/endpointfilter.go`, `relay/middleware/endpointfilter_test.go`
- Modify: `relay/middleware/circuitbreak.go:62-71`

**Interfaces:**
- Produces: `func filterEndpoints(eps domain.EndpointAddrList, keep func(domain.EndpointAddr) bool) domain.EndpointAddrList` — returns `eps` itself when nothing is removed, otherwise a NEW slice. Never writes into `eps`.

- [ ] **Step 1: Failing test**

```go
package middleware

import (
	"testing"

	"github.com/pokt-network/sage/domain"
)

// Hedge's Clone is shallow: the primary arm and the parent share one backing
// array. A filter that compacts in place is a write on one arm racing the
// other's read. The helper must leave the input untouched.
func TestFilterEndpoints_NeverMutatesInput(t *testing.T) {
	eps := testEndpoints(4)
	before := append(domain.EndpointAddrList(nil), eps...)

	out := filterEndpoints(eps, func(ep domain.EndpointAddr) bool { return ep != eps[1] })

	for i := range before {
		if eps[i] != before[i] {
			t.Fatalf("input mutated at %d: %v -> %v", i, before[i], eps[i])
		}
	}
	if len(out) != 3 || out[0] != eps[0] || out[1] != eps[2] || out[2] != eps[3] {
		t.Fatalf("filtered = %v", out)
	}
}

func TestFilterEndpoints_NoRemovalReturnsSameSlice(t *testing.T) {
	eps := testEndpoints(3)
	out := filterEndpoints(eps, func(domain.EndpointAddr) bool { return true })
	if &out[0] != &eps[0] {
		t.Fatal("nothing removed must not allocate")
	}
}
```

- [ ] **Step 2: Run to verify failure** — undefined.

- [ ] **Step 3: Implement**

```go
package middleware

import "github.com/pokt-network/sage/domain"

// filterEndpoints returns the endpoints keep accepts. It returns eps itself
// when nothing is removed — the common case, and free — and a fresh slice
// otherwise. It never writes into eps: relay.Context.Clone is shallow, so a
// hedge arm and its parent share one backing array, and an in-place compaction
// on one is a data race with the other's read.
func filterEndpoints(eps domain.EndpointAddrList, keep func(domain.EndpointAddr) bool) domain.EndpointAddrList {
	for i, ep := range eps {
		if keep(ep) {
			continue
		}
		// First removal: copy what survived so far, then continue filtering.
		out := make(domain.EndpointAddrList, 0, len(eps)-1)
		out = append(out, eps[:i]...)
		for _, rest := range eps[i+1:] {
			if keep(rest) {
				out = append(out, rest)
			}
		}
		return out
	}
	return eps
}
```

Replace in `circuitbreak.go`:

```go
			if len(ctx.Endpoints) > 0 {
				filtered := ctx.Endpoints[:0:len(ctx.Endpoints)]
				filtered = filtered[:0]
				for _, ep := range ctx.Endpoints {
					if !breaker.IsBroken(serviceID, ep.Domain()) {
						filtered = append(filtered, ep)
					}
				}
				ctx.Endpoints = filtered
			}
```

with:

```go
			if len(ctx.Endpoints) > 0 {
				ctx.Endpoints = filterEndpoints(ctx.Endpoints, func(ep domain.EndpointAddr) bool {
					return !breaker.IsBroken(serviceID, ep.Domain())
				})
			}
```

- [ ] **Step 4: Run** — `go test ./relay/middleware/ -race -count=1` → PASS.

- [ ] **Step 5: Commit**

```bash
git add relay/middleware/endpointfilter.go relay/middleware/endpointfilter_test.go relay/middleware/circuitbreak.go
git commit -m "fix(middleware): filter endpoints by copy, not in place

circuit_break compacted ctx.Endpoints in place. Hedge's Clone is shallow,
so the primary arm and its parent share the backing array, and that
compaction is a write racing the hedge arm's read. The helper copies only
when something is removed; method_blocks uses it too."
```

---

### Task 10: The `method_blocks` middleware

**Files:**
- Create: `relay/middleware/methodblocks.go`, `relay/middleware/methodblocks_test.go`

**Interfaces:**
- Consumes: `methodblock.Store` (Task 7), `qos.Registry.Get`, `qos.MethodNormalizer` (Task 4), `featureflag.FlagMethodBlocks` (Task 8), `filterEndpoints` (Task 9), `ctx.HeuristicResult.MethodBlocking` (Tasks 1, 3).
- Produces:
  ```go
  type MethodBlockRecorder interface { RecordMethodBlockEvent(serviceID domain.ServiceID, method, event string) }
  const ( MethodBlockEventMark = "mark"; MethodBlockEventEscalate = "escalate"; MethodBlockEventBypass = "bypass" )
  func MethodBlocks(store *methodblock.Store, registry *qos.Registry, flags featureflag.FlagStore, events MethodBlockRecorder) relay.Middleware
  ```

- [ ] **Step 1: Failing tests**

Create `relay/middleware/methodblocks_test.go`:

```go
package middleware

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/methodblock"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
)

// normPlugin is a qos.Plugin that names every payload's method verbatim,
// except "" which it reports as no method.
type normPlugin struct{}

func (normPlugin) ParseRequest(context.Context, *http.Request, []byte, domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}
func (normPlugin) SelectEndpoints(eps domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return eps, nil
}
func (normPlugin) NormalizeMethod(p domain.Payload) string { return p.Method() }

type spyEvents struct {
	mu     sync.Mutex
	events []string
}

func (s *spyEvents) RecordMethodBlockEvent(_ domain.ServiceID, method, event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event+":"+method)
}

func methodCtx(method string, eps domain.EndpointAddrList) *relay.Context {
	ctx := baseContext()
	ctx.Endpoints = eps
	ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, method)}
	return ctx
}

func registryWith(t *testing.T) *qos.Registry {
	t.Helper()
	reg := qos.NewRegistry()
	if err := reg.Register("eth", normPlugin{}); err != nil {
		t.Fatal(err)
	}
	return reg
}

// A timeout on eth_getLogs marks the host for eth_getLogs and nothing else.
func TestMethodBlocks_TimeoutMarksOnlyThatMethod(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{MethodBlocking: true, Reason: "transport_timeout"}
		return retryableErr("timeout")
	})
	h := MethodBlocks(store, registryWith(t), newFlags("method_blocks"), nil)(inner)
	_ = h.HandleRelay(methodCtx("eth_getLogs", eps))

	if !store.Blocked("eth", eps[0].Domain(), "eth_getLogs") {
		t.Fatal("timed-out method not marked")
	}
	if store.Blocked("eth", eps[0].Domain(), "eth_call") {
		t.Fatal("another method was blocked")
	}
}

// The filter must remove exactly the blocked host for the blocked method —
// built so ONE endpoint survives, because filter-all and filter-none look the
// same once selection falls back to the unfiltered list.
func TestMethodBlocks_FiltersBlockedHostForThatMethodOnly(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	store.Mark("eth", eps[0].Domain(), "eth_getLogs")

	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.Endpoints
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})
	h := MethodBlocks(store, registryWith(t), newFlags("method_blocks"), nil)(inner)

	_ = h.HandleRelay(methodCtx("eth_getLogs", eps))
	if len(seen) != 1 || seen[0] != eps[1] {
		t.Fatalf("eth_getLogs saw %v, want only %v", seen, eps[1])
	}

	_ = h.HandleRelay(methodCtx("eth_call", eps))
	if len(seen) != 2 {
		t.Fatalf("eth_call saw %v, want both hosts", seen)
	}
}

// A block must never empty a pool. Every host blocked ⇒ degrade and serve
// the unfiltered list, and say so.
func TestMethodBlocks_EveryHostBlockedDegradesInsteadOfEmptying(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	for _, ep := range eps {
		store.Mark("eth", ep.Domain(), "eth_getLogs")
	}
	events := &spyEvents{}
	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.Endpoints
		return nil
	})
	h := MethodBlocks(store, registryWith(t), newFlags("method_blocks"), events)(inner)

	ctx := methodCtx("eth_getLogs", eps)
	_ = h.HandleRelay(ctx)
	if len(seen) != 2 {
		t.Fatalf("pool emptied: %v", seen)
	}
	if !ctx.Degraded {
		t.Fatal("bypass must mark the relay degraded")
	}
	if len(events.events) != 1 || events.events[0] != "bypass:eth_getLogs" {
		t.Fatalf("events = %v", events.events)
	}
}

func TestMethodBlocks_ThirdMethodEscalatesAndIsCounted(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(1)
	events := &spyEvents{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{MethodBlocking: true}
		return retryableErr("timeout")
	})
	h := MethodBlocks(store, registryWith(t), newFlags("method_blocks"), events)(inner)
	for _, m := range []string{"a", "b", "c"} {
		_ = h.HandleRelay(methodCtx(m, eps))
	}
	if !store.Blocked("eth", eps[0].Domain(), "anything") {
		t.Fatal("host not escalated")
	}
	want := []string{"mark:a", "mark:b", "escalate:c"}
	if len(events.events) != 3 {
		t.Fatalf("events = %v", events.events)
	}
	for i := range want {
		if events.events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events.events, want)
		}
	}
}

func TestMethodBlocks_NoNormalizerPassesThrough(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(1)
	store.Mark("eth", eps[0].Domain(), "eth_getLogs")
	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.Endpoints
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{MethodBlocking: true}
		return retryableErr("timeout")
	})
	// Registry with no plugin for "eth".
	h := MethodBlocks(store, qos.NewRegistry(), newFlags("method_blocks"), nil)(inner)
	_ = h.HandleRelay(methodCtx("eth_getLogs", eps))
	if len(seen) != 1 {
		t.Fatal("without a normalizer nothing may be filtered")
	}
	if len(store.Active("eth")) != 1 {
		t.Fatal("without a normalizer nothing may be marked")
	}
}

func TestMethodBlocks_FlagOffPassesThrough(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(1)
	store.Mark("eth", eps[0].Domain(), "eth_getLogs")
	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error { seen = ctx.Endpoints; return nil })
	h := MethodBlocks(store, registryWith(t), newFlags(), nil)(inner)
	_ = h.HandleRelay(methodCtx("eth_getLogs", eps))
	if len(seen) != 1 {
		t.Fatal("flag off must not filter")
	}
}

// The reason this middleware sits INSIDE Hedge: the losing arm's timeout is
// the whole incident, and Observe never sees a loser. Through the real
// Hedge, a slow primary must mark its host, and the next request's hedge
// must not land there for that method.
func TestMethodBlocks_LosingHedgeArmMarksAndNextHedgeAvoids(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	slow := eps[0]

	var mu sync.Mutex
	var attempts []domain.EndpointAddr
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		// "select": first available endpoint.
		ctx.Endpoint = ctx.Endpoints[0]
		if ctx.SelectedEndpoint != nil {
			ep := ctx.Endpoint
			ctx.SelectedEndpoint.Store(&ep)
		}
		mu.Lock()
		attempts = append(attempts, ctx.Endpoint)
		mu.Unlock()
		if ctx.Endpoint == slow {
			time.Sleep(30 * time.Millisecond)
			ctx.HeuristicResult = &heuristic.AnalysisResult{MethodBlocking: true, Reason: "transport_timeout"}
			return retryableErr("timeout")
		}
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})
	cfg := func(domain.ServiceID) config.RetryConfig { return config.RetryConfig{HedgeDelay: 5 * time.Millisecond} }
	chain := Hedge(newFlags("hedge", "method_blocks"), cfg)(
		MethodBlocks(store, registryWith(t), newFlags("hedge", "method_blocks"), nil)(inner))

	// Request 1: primary picks slow, hedge picks the other and wins; the slow
	// arm finishes later and marks its host.
	if err := chain.HandleRelay(methodCtx("eth_getLogs", eps)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for !store.Blocked("eth", slow.Domain(), "eth_getLogs") {
		if time.Now().After(deadline) {
			t.Fatal("losing arm's timeout never marked the host")
		}
		time.Sleep(time.Millisecond)
	}

	// Request 2: the slow host must not be tried for eth_getLogs at all.
	mu.Lock()
	attempts = nil
	mu.Unlock()
	if err := chain.HandleRelay(methodCtx("eth_getLogs", eps)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // let any straggling arm record
	mu.Lock()
	defer mu.Unlock()
	for _, a := range attempts {
		if a == slow {
			t.Fatalf("blocked host was attempted again: %v", attempts)
		}
	}
}
```

Add `"net/http"` to imports (for `normPlugin.ParseRequest`).

- [ ] **Step 2: Run to verify failure** — `go test ./relay/middleware/ -run TestMethodBlocks -count=1` → undefined `MethodBlocks`.

- [ ] **Step 3: Implement**

Create `relay/middleware/methodblocks.go`:

```go
package middleware

import (
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/methodblock"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
)

// MethodBlockRecorder is told about method-block events. metrics.Recorder
// satisfies it; nil disables recording.
type MethodBlockRecorder interface {
	RecordMethodBlockEvent(serviceID domain.ServiceID, method, event string)
}

// Method-block event names, a closed set used as a metric label.
const (
	MethodBlockEventMark     = "mark"
	MethodBlockEventEscalate = "escalate"
	MethodBlockEventBypass   = "bypass"
)

// MethodBlocks returns a middleware that keeps a method away from a host that
// could not answer it recently, while the host keeps receiving everything
// else.
//
//  1. Pre-relay: removes from ctx.Endpoints every host the store blocks for
//     this request's method. If that leaves nothing, the relay is marked
//     degraded and the unfiltered list is used — a block must never be able
//     to empty a pool.
//  2. Post-relay: if the attempt's verdict is MethodBlocking (a timeout after
//     connect, or the endpoint saying it does not serve the method), marks
//     the attempt's host for that method.
//
// It sits inside Retry and Hedge so every arm and every attempt both honours
// and feeds the store — the losing hedge arm's timeout is the case that
// matters, and nothing outside Hedge ever sees a loser.
//
// The method is the plugin's normalised name (qos.MethodNormalizer): a
// bounded catalogue, never the client's string. A service whose plugin has
// no normaliser, or a payload with no method, passes through untouched.
func MethodBlocks(
	store *methodblock.Store,
	registry *qos.Registry,
	flags featureflag.FlagStore,
	events MethodBlockRecorder,
) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if store == nil || !flags.IsEnabled(ctx.Ctx, featureflag.FlagMethodBlocks, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}
			method := normalizedMethod(registry, ctx)
			if method == "" {
				return next.HandleRelay(ctx)
			}
			serviceID := string(ctx.ServiceID)

			if len(ctx.Endpoints) > 0 {
				filtered := filterEndpoints(ctx.Endpoints, func(ep domain.EndpointAddr) bool {
					return !store.Blocked(serviceID, ep.Domain(), method)
				})
				if len(filtered) == 0 {
					ctx.Degraded = true
					if events != nil {
						events.RecordMethodBlockEvent(ctx.ServiceID, method, MethodBlockEventBypass)
					}
				} else {
					ctx.Endpoints = filtered
				}
			}

			err := next.HandleRelay(ctx)

			if ctx.Endpoint != "" && ctx.HeuristicResult != nil && ctx.HeuristicResult.MethodBlocking {
				event := MethodBlockEventMark
				if store.Mark(serviceID, ctx.Endpoint.Domain(), method) {
					event = MethodBlockEventEscalate
				}
				if events != nil {
					events.RecordMethodBlockEvent(ctx.ServiceID, method, event)
				}
			}
			return err
		})
	}
}

// normalizedMethod asks the service's plugin to name the request's method.
// "" means "nothing to key on" for any reason: no plugin, no normaliser, no
// payload, or a payload without a method notion.
func normalizedMethod(registry *qos.Registry, ctx *relay.Context) string {
	if registry == nil || len(ctx.Payloads) == 0 {
		return ""
	}
	plugin := ctx.Plugin
	if plugin == nil {
		plugin = registry.Get(ctx.ServiceID)
	}
	normalizer, ok := plugin.(qos.MethodNormalizer)
	if !ok {
		return ""
	}
	return normalizer.NormalizeMethod(ctx.Payloads[0])
}
```

- [ ] **Step 4: Run to verify pass** — `go test ./relay/middleware/ -run TestMethodBlocks -race -count=1 -v` → PASS. Then the whole package with `-race`.

- [ ] **Step 5: Revert-checks**

- Remove the pre-relay filter block → `FiltersBlockedHost…` and `LosingHedgeArm…` fail.
- Replace `if len(filtered) == 0 { … } else { ctx.Endpoints = filtered }` with an unconditional `ctx.Endpoints = filtered` → `EveryHostBlockedDegrades…` fails.
- Remove the post-relay mark → `TimeoutMarksOnlyThatMethod`, `ThirdMethodEscalates…` fail.
Restore each.

- [ ] **Step 6: Commit**

```bash
git add relay/middleware/methodblocks.go relay/middleware/methodblocks_test.go
git commit -m "feat(middleware): method_blocks

Prunes hosts blocked for this request's method before selection and marks
the attempt's host when the verdict is MethodBlocking. Inside Retry and
Hedge, so a losing hedge arm's timeout — the case PATH measured — is what
feeds it. Every host blocked degrades to the unfiltered list rather than
emptying the pool."
```

---

### Task 11: Metrics — collector and event counter

**Files:**
- Create: `metrics/methodblock.go`, `metrics/methodblock_test.go`
- Modify: `metrics/prometheus.go` (`Recorder` field, constructor, `MustRegister`, method), `metrics/prometheus_test.go` (`newIsolatedRecorder`)

**Interfaces:**
- Consumes: `methodblock.Block` shape via a local interface (do not import `methodblock` into `metrics`; mirror `BrokenDomainLister`).
- Produces:
  ```go
  type MethodBlockLister interface { Active(serviceID string) []MethodBlock }   // see note
  func NewMethodBlockCollector(lister MethodBlockLister, services []domain.ServiceID) *MethodBlockCollector
  func (r *Recorder) RecordMethodBlockEvent(serviceID domain.ServiceID, method, event string)
  ```
  Note: to avoid the import, define in `metrics`:
  ```go
  // MethodBlock is the subset of methodblock.Block the collector reads.
  type MethodBlock struct { Host, Method string }
  type MethodBlockLister interface { ActiveMethodBlocks(serviceID string) []MethodBlock }
  ```
  and have `wire.go` adapt with a tiny struct that calls `store.Active` and maps fields. (Alternative: give `methodblock.Store` a method `ActiveMethodBlocks` returning `[]metrics.MethodBlock` — rejected, that would make `methodblock` import `metrics`.)

- [ ] **Step 1: Failing tests**

`metrics/methodblock_test.go`, modelled on `metrics/breaker_test.go` (`scrape` helper exists there):

```go
package metrics

import (
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
)

type stubMethodBlocks struct{ blocks map[string][]MethodBlock }

func (s *stubMethodBlocks) ActiveMethodBlocks(serviceID string) []MethodBlock {
	return s.blocks[serviceID]
}

func TestMethodBlockCollector_ReportsActiveBlocks(t *testing.T) {
	lister := &stubMethodBlocks{blocks: map[string][]MethodBlock{
		"eth":    {{Host: "slow.example.com", Method: "eth_getLogs"}},
		"solana": {{Host: "dead.example.com"}}, // host-level: empty method label
	}}
	out := scrape(t, NewMethodBlockCollector(lister, []domain.ServiceID{"eth", "solana", "poly"}))
	for _, want := range []string{
		`sage_method_blocks{domain="slow.example.com",method="eth_getLogs",service_id="eth"} 1`,
		`sage_method_blocks{domain="dead.example.com",method="",service_id="solana"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in:\n%s", want, out)
		}
	}
	if strings.Contains(out, `service_id="poly"`) {
		t.Error("a service with no blocks must be absent, not 0")
	}
}
```

And append to `metrics/prometheus_test.go`:

```go
func TestRecordMethodBlockEvent(t *testing.T) {
	r := newIsolatedRecorder(t)
	r.RecordMethodBlockEvent("eth", "eth_getLogs", "mark")
}
```

- [ ] **Step 2: Run to verify failure** — undefined.

- [ ] **Step 3: Implement**

`metrics/methodblock.go`:

```go
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
)

// MethodBlock is one active method block as the collector reads it. Method is
// "" for a host-level block.
type MethodBlock struct {
	Host   string
	Method string
}

// MethodBlockLister reports the live method blocks for a service.
// methodblock.Store satisfies it through an adapter in wire.go.
type MethodBlockLister interface {
	ActiveMethodBlocks(serviceID string) []MethodBlock
}

// MethodBlockCollector exposes method blocks as a gauge:
//
//	sage_method_blocks{service_id, domain, method} 1
//
// Derived at scrape time, like BreakerCollector: blocks expire lazily, so a
// pushed gauge would never clear. Absent means no block. method is the
// plugin's catalogued name (bounded) or "" for a host-level block.
type MethodBlockCollector struct {
	lister   MethodBlockLister
	services []domain.ServiceID
	desc     *prometheus.Desc
}

// NewMethodBlockCollector returns a collector for the given services. It does
// not register itself.
func NewMethodBlockCollector(lister MethodBlockLister, services []domain.ServiceID) *MethodBlockCollector {
	return &MethodBlockCollector{
		lister:   lister,
		services: services,
		desc: prometheus.NewDesc(
			"sage_method_blocks",
			"1 while a host is blocked from receiving a method for this service (method empty = blocked for every method). Absent when nothing is blocked.",
			[]string{"service_id", "domain", "method"},
			nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *MethodBlockCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

// Collect implements prometheus.Collector. Called on scrape, not on the hot path.
func (c *MethodBlockCollector) Collect(ch chan<- prometheus.Metric) {
	if c.lister == nil {
		return
	}
	for _, serviceID := range c.services {
		for _, b := range c.lister.ActiveMethodBlocks(string(serviceID)) {
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, 1,
				sanitizeLabel(string(serviceID)), sanitizeLabel(b.Host), b.Method)
		}
	}
}
```

In `metrics/prometheus.go`: add field `methodBlockEvents *prometheus.CounterVec`; construct it in `NewRecorder`:

```go
		// No domain label on purpose: the gauge above names the host, and a
		// counter keyed on host is the series growth PATH's cardinality
		// incident was about. method is the plugin catalogue; event is a
		// closed set from relay/middleware.
		methodBlockEvents: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "method_block_events_total",
				Help:      "Method-block events by service and method. event is mark (a host was blocked for a method), escalate (a host was blocked for every method), or bypass (every host was blocked for the method, so the unfiltered pool was used).",
			},
			[]string{"service_id", "method", "event"},
		),
```

add it to `MustRegister`, add the same construction (namespace `sage_test`) to `newIsolatedRecorder` in the test file, and the method:

```go
// RecordMethodBlockEvent counts one method-block event. method comes from
// the plugin's bounded catalogue and event from a closed set, so neither
// needs bounding here.
func (r *Recorder) RecordMethodBlockEvent(serviceID domain.ServiceID, method, event string) {
	r.methodBlockEvents.WithLabelValues(r.services.serviceValue(serviceID), method, event).Inc()
}
```

- [ ] **Step 4: Run** — `go test ./metrics/ -race -count=1` → PASS.

- [ ] **Step 5: Docs + commit**

```bash
make docs
git add metrics/ docs/metrics.md
git commit -m "feat(metrics): method block gauge and event counter

sage_method_blocks is derived at scrape time like the breaker gauge; the
event counter carries service and catalogued method only, never the host."
```

---

### Task 12: Admin routes

**Files:**
- Modify: `router/admin.go` (struct field, constructor parameter, two routes, two handlers)
- Modify: `cmd/sagegw/wire.go:464` (constructor call — done for real in Task 13; here only make it compile by passing `nil`)
- Test: `router/admin_methodblocks_test.go` (new)

**Interfaces:**
- Consumes: `methodblock.Store.Active`, `.Clear`.
- Produces: `GET /admin/method-blocks/{serviceID}` → `[]methodblock.Block` (empty array, never null); `POST /admin/method-blocks/clear/{serviceID}` → `{"service_id","cleared","message"}`. `NewAdminAPI` gains a `blocks *methodblock.Store` parameter after `breaker`.

- [ ] **Step 1: Failing test**

Look at how existing admin tests build an `AdminAPI` and issue requests (grep `NewAdminAPI(` in `router/*_test.go`, note the bearer-token setup if the tests wrap the mux in auth). Then:

```go
package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pokt-network/sage/methodblock"
)

func TestAdmin_MethodBlocks_GetAndClear(t *testing.T) {
	store := methodblock.New()
	store.Mark("eth", "slow.example.com", "eth_getLogs")
	api := newTestAdminAPIWithBlocks(t, store) // helper: same as the existing admin test constructor, plus the store
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/method-blocks/eth", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status %d: %s", rec.Code, rec.Body)
	}
	var blocks []methodblock.Block
	if err := json.Unmarshal(rec.Body.Bytes(), &blocks); err != nil || len(blocks) != 1 || blocks[0].Method != "eth_getLogs" {
		t.Fatalf("GET body %s (%v)", rec.Body, err)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/method-blocks/clear/eth", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST status %d: %s", rec.Code, rec.Body)
	}
	if store.Blocked("eth", "slow.example.com", "eth_getLogs") {
		t.Fatal("clear did not clear")
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/method-blocks/eth", nil))
	if rec.Body.String() != "[]" {
		t.Fatalf("empty list must be [], got %s", rec.Body)
	}
}
```

- [ ] **Step 2: Run to verify failure** — 404 / undefined helper.

- [ ] **Step 3: Implement**

In `router/admin.go`: add `blocks *methodblock.Store` to the struct and a `blocks *methodblock.Store` parameter to `NewAdminAPI` after `breaker`; register:

```go
	// Method blocks
	mux.HandleFunc("POST /admin/method-blocks/clear/{serviceID}", a.handleClearMethodBlocks)
	mux.HandleFunc("GET /admin/method-blocks/{serviceID}", a.handleGetMethodBlocks)
```

handlers:

```go
// handleGetMethodBlocks lists the hosts currently blocked from receiving a
// method for a service, with each block's expiry. A block with an empty
// method is a host-level block (every method). An empty array means nothing
// is blocked. The same state is exported as the sage_method_blocks metric.
func (a *AdminAPI) handleGetMethodBlocks(w http.ResponseWriter, req *http.Request) {
	serviceID := req.PathValue("serviceID")
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}
	blocks := []methodblock.Block{}
	if a.blocks != nil {
		if active := a.blocks.Active(serviceID); active != nil {
			blocks = active
		}
	}
	writeJSON(w, http.StatusOK, blocks)
}

// handleClearMethodBlocks drops every method block for a service. It exists
// so an operator can undo a false positive; the escalation count goes with
// the marks, so the next mark is a first mark.
func (a *AdminAPI) handleClearMethodBlocks(w http.ResponseWriter, req *http.Request) {
	serviceID := req.PathValue("serviceID")
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}
	cleared := 0
	if a.blocks != nil {
		cleared = a.blocks.Clear(serviceID)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service_id": serviceID,
		"cleared":    cleared,
		"message":    "method blocks cleared",
	})
}
```

Update every existing `NewAdminAPI(` call in tests and in `cmd/sagegw/wire.go` (pass `nil` for now in wire; Task 13 replaces it).

- [ ] **Step 4: Run** — `go build ./... && go test ./router/ -race -count=1` → PASS.

- [ ] **Step 5: Docs + commit**

```bash
make docs
git add router/ cmd/sagegw/wire.go docs/admin-api.md
git commit -m "feat(admin): list and clear method blocks"
```

---

### Task 13: Wiring

**Files:**
- Modify: `cmd/sagegw/wire.go` (store construction near the breaker at `:219`, collector registration near `:225`, middleware registration near `:355`, admin constructor at `:464`)
- Modify: `ARCHITECTURE.md:47` (chain diagram)

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Construct the store next to the breaker**

After `cb := circuitbreaker.New(...)`:

```go
	// 6b. Method blocks: per-host, per-method memory consulted at selection.
	// Local memory only — see the methodblock package doc.
	blocks := methodblock.New(
		methodblock.WithTTL(cfg.Gateway.MethodBlocks.EffectiveTTL()),
		methodblock.WithEscalation(cfg.Gateway.MethodBlocks.EffectiveEscalation()),
		methodblock.WithLogger(logger),
	)
	blocks.StartSweep(ctx)
	prometheus.MustRegister(metrics.NewMethodBlockCollector(methodBlockLister{blocks}, serviceIDsFrom(cfg)))
```

and at file scope:

```go
// methodBlockLister adapts methodblock.Store to metrics.MethodBlockLister so
// metrics does not import methodblock (nor the reverse).
type methodBlockLister struct{ store *methodblock.Store }

func (l methodBlockLister) ActiveMethodBlocks(serviceID string) []metrics.MethodBlock {
	active := l.store.Active(serviceID)
	out := make([]metrics.MethodBlock, len(active))
	for i, b := range active {
		out[i] = metrics.MethodBlock{Host: b.Host, Method: b.Method}
	}
	return out
}
```

- [ ] **Step 2: Register the middleware** between `MWCircuitBreak` and `MWSelectEndpoint`:

```go
	mwReg.Register(relay.MWMethodBlocks, func() relay.Middleware {
		return middleware.MethodBlocks(blocks, qosReg, flags, recorder)
	})
```

- [ ] **Step 3: Admin** — replace the `nil` from Task 12 with `blocks` in `router.NewAdminAPI(...)`.

- [ ] **Step 4: ARCHITECTURE.md** — add a line under `[CircuitBreak]` in the chain diagram:

```
                                  [MethodBlocks] — skip hosts blocked for this method
```

and shift the lines below it by two spaces to keep the staircase. In the feature-flag table near line 224 add `| method_blocks | on | Per-host, per-method memory: a host that timed out on a method stops receiving it for a TTL |`.

- [ ] **Step 5: Build, full suite, lint, docs check**

Run:
```bash
go build ./... && go vet ./... && gofmt -l . && make test_unit && make go_lint && make docs && git diff --stat docs/
```
Expected: clean, `docs/` unchanged after regeneration (already regenerated in earlier tasks) or only the ARCHITECTURE line.

- [ ] **Step 6: Boot check against the mock backend**

Run the gateway with a mock-backend config (see `Makefile` targets and `local/` for an existing mock config; if none, `make sage_run CONFIG_PATH=config/testdata/path_config.yaml` and confirm it starts and logs the middleware chain including `method_blocks`, then `curl -H 'Authorization: Bearer …' localhost:<admin>/admin/method-blocks/eth` → `[]`). Record the exact command that worked in the commit body.

- [ ] **Step 7: Commit**

```bash
git add cmd/sagegw/wire.go ARCHITECTURE.md docs/
git commit -m "feat: wire method-aware blocks

Store, sweep, collector, middleware slot and admin routes, behind the
method_blocks flag (on). A host that times out on a method now stops
receiving that method for one TTL and keeps receiving everything else;
three methods inside a TTL block the host outright."
```

---

### Task 14: Memory and hand-off notes

**Files:**
- Modify: `docs/superpowers/specs/2026-08-26-method-aware-blocks-design.md` (status line → implemented, commit range)
- Modify (outside repo): the memory files `project_sage_catchup_pending.md` and `design_method_scoped_exclusion.md` — mark implemented, list what the beta validation must show (spec §8).

- [ ] **Step 1: Update the spec status line and commit** (`docs: mark method-aware blocks spec implemented`).
- [ ] **Step 2: Update the two memory files** with: commit range, "not validated on beta", and the four rollout checks from spec §8 as the next action.

---

## Self-review against the spec

- §5.1 transport grading → Tasks 1–2. Cancel-wins ordering → Task 1 test. `buildSignal` AttrClient rule → Task 2.
- §5.1 `MethodBlocking` on `-32601` / unsupported wordings, not on archival or per-key → Task 3.
- §5.2 `MethodNormalizer`, per-plugin catalogues, cosmos templating, golden tests → Tasks 4–6.
- §5.3 store rules (refresh-not-extend, escalation on distinct live methods, host block, Clear, sweep, local-only, defaults) → Task 7.
- §5.4 middleware, chain slot, `mustPrecede`, flag default on, bypass-on-empty, copy-on-filter + the circuit_break fix → Tasks 8–10.
- §5.5 config with zero/negative convention, fixtures, docgen → Task 8.
- §5.6 gauge collector, event counter without domain label, admin GET/clear, `make docs` → Tasks 11–12.
- §6 error handling: no normaliser / flag off / TTL off pass-through → Task 10 tests + Task 7 `TTL <= 0`.
- §7 testing: real-chain Hedge test, one-survivor filter tests, revert-checks → Tasks 1, 2, 3, 7, 10.
- §8 rollout → Task 14 hands the checks to the beta-validation memory.
- Type consistency: `MethodBlocking` (Tasks 1, 3, 10); `NormalizeMethod(domain.Payload) string` (Tasks 4–6, 10); `Store.Blocked/Mark/Clear/Active/StartSweep` (Tasks 7, 10, 12, 13); `MethodBlockRecorder.RecordMethodBlockEvent(domain.ServiceID, string, string)` (Tasks 10, 11, 13); `metrics.MethodBlockLister.ActiveMethodBlocks` (Tasks 11, 13); `NewAdminAPI(..., breaker, blocks, qosReg, ...)` (Tasks 12, 13).
