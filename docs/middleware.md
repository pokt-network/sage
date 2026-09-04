# Adding a middleware

A middleware is one file in `relay/middleware/` that wraps `next.HandleRelay(ctx)`
with a single concern. It is the seam for extending the gateway without touching
protocol code — most new behaviour belongs here rather than anywhere else.

Most existing middleware run well under 200 lines. If yours is heading past
that, it is probably two concerns — the exceptions in the tree (parse, batch,
retry, hedge) are each one concern that happens to be a fan-out or a
rejection point.

## The six steps

Say you are adding a `rate_limit`.

### 1. The file

`relay/middleware/ratelimit.go`. The constructor returns a `relay.Middleware`
and captures its dependencies in the closure — look at any existing middleware
for the shape. If it should be runtime-toggleable, gate it on a feature flag and
bail to `next` when off:

```go
func RateLimit(flags featureflag.FlagStore, /* deps */) relay.Middleware {
    return func(next relay.Handler) relay.Handler {
        return relay.HandlerFunc(func(ctx *relay.Context) error {
            if flags == nil || !flags.IsEnabled(ctx.Ctx, featureflag.FlagRateLimit, ctx.ServiceID) {
                return next.HandleRelay(ctx)
            }
            // reject over-limit here; otherwise fall through
            return next.HandleRelay(ctx)
        })
    }
}
```

The `flags == nil` check is not defensive noise — it is what lets the middleware
be constructed in a test without a flag store.

### 2. The name

Add an `MW…` constant to `relay/chain_order.go` and place it in
`DefaultChainOrder()` where it should run. **First in the list is outermost and
runs first.**

If the position is load-bearing — it must run before or after something else —
add a `mustPrecede` rule in the same file. This matters more than it looks: a
name the ordering rules do not recognise is allowed *anywhere* in the chain and
gets **no** protection. A rule is the only thing that enforces "before X". The
default order is not a guarantee; an operator can override it in YAML.

### 3. Register it

In `cmd/sagegw.Build` (`wire.go`), next to the others:

```go
mwReg.Register(relay.MWRateLimit, func() relay.Middleware {
    return middleware.RateLimit(flags /* … */)
})
```

The registry takes no arguments by design: dependencies are captured at
registration, where they are already in scope and typed.

Failure modes are deliberate. A name in the chain that is not registered is a
**startup error** listing what is registered. A registered name absent from the
chain **warns**. A `Build` test fails if a `DefaultChainOrder` name was never
registered — that is the drift guard.

### 4. The flag

If you added one, add its name to `featureflag.DefaultFlags` with a default and
an exported `Flag…` constant **next to it, in the same file**
(`featureflag/defaults.go`). That is the only place a flag is defined; config
picks it up as a `map[string]bool` override automatically, and unset means "use
the default".

A name absent from `DefaultFlags` resolves to `false`. A flag you forgot to add
there does not error — it silently never runs.

### 5. Config

If the module needs settings, add fields to the relevant config struct with
yaml tags and **value types** (no `*bool`, no `*int`). Choose the zero value so
that unconfigured is the safe state.

Write a doc comment on every field. The configuration reference is generated
from them, and `make docs` plus its golden test will fail the build if a key
arrives without one.

Wire the field through to something that reads it. A field SAGE parses and never
reads is the exact bug this repo keeps re-learning: it does not even earn the
"unknown key" warning an undeclared key gets, so the operator sets it and hears
nothing. The generated reference will list it under "Parsed but not
implemented", which is honest but not a substitute for wiring it.

### 6. Test

`ratelimit_test.go`, next to the file. The canonical run is `-short -race`.

## Things that will bite you

**`Context.Clone()` is a shallow copy.** Hedge racers and batch sub-relays each
run on a clone, so every pointer, slice and map field on `relay.Context` is
shared across the whole request tree. A field that looks per-request is
per-request-*tree*. Scalars are safe to write; anything else needs an atomic
(see how `Degraded` is merged in `middleware/batch.go`) or must be treated as
read-only. The race detector only catches the mistake if a test actually fans
out — write that test.

**Each `Context` field has exactly one writer.** Set by one middleware, read by
others. Keep that discipline when adding a field. `SelectedEndpoint` is the
illustrative exception: it is an `*atomic.Pointer` that exists only because
Hedge needs to read the primary arm's pick from another goroutine, and reading
`ctx.Endpoint` for that is a data race.

**Read `ctx.ClientIP`, not `HTTPRequest.RemoteAddr`,** for anything per-client.
The former is set by the `client_ip` middleware and is trusted-proxy aware;
behind a proxy the latter is the proxy.

**Redis is optional.** Fail open when it is nil. Nothing on the hot path may
hard-require it.

**`RPCType` is immutable** once `Parse` has set it. Do not mutate it later.

**`ShouldRetry` and `ShouldCircuitBreak` are independent.** Retrying must never
escalate into a domain-wide lockout on its own — that split is a reproduction of
a production bug from PATH, and it is load-bearing.

## Where the pieces live

| Concern | File |
|---|---|
| Middleware implementations | `relay/middleware/*.go` |
| Names, default order, ordering rules | `relay/chain_order.go` |
| Registry and chain building | `relay/registry.go` |
| Per-request state | `relay/context.go` |
| Wiring | `cmd/sagegw/wire.go` |
| Flag definitions | `featureflag/defaults.go` |
