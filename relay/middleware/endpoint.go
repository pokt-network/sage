package middleware

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// SelectEndpoint returns a middleware that selects the best endpoint for the
// relay. It applies chain-specific QoS filtering via the plugin, falls back
// gracefully to the full endpoint list when QoS filtering produces no
// candidates, and then delegates final selection to the reputation service.
func SelectEndpoint(repSvc reputation.Service, endpointProvider protocol.EndpointProvider) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			// Fetch available endpoints from the protocol layer if not already set.
			if len(ctx.Endpoints) == 0 && endpointProvider != nil {
				eps, err := endpointProvider.AvailableEndpoints(ctx.Ctx, ctx.ServiceID, ctx.RPCType)
				if err != nil {
					return err
				}
				ctx.Endpoints = eps
			}

			candidates := ctx.Endpoints

			// Apply chain-specific filtering via QoS plugin.
			if ctx.Plugin != nil {
				filtered, err := ctx.Plugin.SelectEndpoints(ctx.Endpoints, ctx.Payloads)
				switch {
				case err == nil && len(filtered) > 0 && len(filtered) < len(ctx.Endpoints) &&
					!anyVouched(repSvc, ctx, filtered) && anyVouched(repSvc, ctx, ctx.Endpoints):
					// Never into junk: the plugin's filters (block height, the
					// archival filter) left only endpoints reputation does not
					// vouch for, while it removed ones it does. On mainnet base
					// (2026-09-14) the archival filter excluded every full node
					// known to be pruned and left two relay miners that never
					// answer — so never marked — to serve the request with a
					// 503 or a 408. The healthy node's own answer, even "state
					// pruned", is the better outcome. Same guard as MethodBlocks.
					ctx.Degraded = true
				case err == nil && len(filtered) > 0:
					candidates = filtered
				default:
					// Graceful degraded fallback: use original endpoints.
					ctx.Degraded = true
				}
			}

			// If there are still no endpoints, degrade and use the original list.
			if len(candidates) == 0 {
				ctx.Degraded = true
				candidates = ctx.Endpoints
			}
			// Nothing at all to send to. Say so: selecting from an empty list
			// used to hand SendRelay an empty address, which it reported as a
			// session rollover — retryable — so every request burned its
			// retries on a pool that was simply empty (mainnet persistence and
			// shentu, 2026-09-15, for the 15 minutes a blacklist emptied them).
			if len(candidates) == 0 {
				return domain.NewRelayError(domain.ErrProtocol, "no endpoint available for service", nil, false)
			}

			// A hedge is a second path to an answer while the primary is
			// still running. When every endpoint left for it is ruled out,
			// the pool-collapse guard would serve the least-bad of them: a
			// guard against reputation emptying a pool into an outage, which
			// a hedge never is. On mainnet poly and poly-zkevm (2026-10-06)
			// every collapse pick, ~5,400 an hour, was a hedge arm sent to an
			// operator whose backend answered 503 to all of them.
			if ctx.AttemptKind == relay.AttemptHedge && allRuledOut(repSvc, ctx, candidates) {
				return errHedgeRuledOut
			}

			// Select the best endpoint by reputation.
			ctx.Endpoint = repSvc.SelectBest(ctx.Ctx, ctx.ServiceID, candidates, ctx.RPCType)

			// Publish the choice for any goroutine watching this relay from
			// outside it — today only Hedge, which needs the primary arm's
			// endpoint while that arm is still running. Nil for every
			// unhedged relay, which is nearly all of them.
			if ctx.SelectedEndpoint != nil {
				ep := ctx.Endpoint
				ctx.SelectedEndpoint.Store(&ep)
			}

			// A probation pick is sent first on a share of relays so it can
			// earn its score back, and it is there because it failed. Held to
			// the attempt's full deadline, a host demoted for timing out
			// spends the client's budget timing out again: on mainnet base
			// (2026-09-26) that was every client 504. A quarter of what
			// remains is ample for a host that has recovered and leaves Retry
			// and Hedge the rest.
			if checker, ok := repSvc.(reputation.ProbationChecker); ok &&
				checker.OnProbation(ctx.Ctx, ctx.ServiceID, ctx.Endpoint, ctx.RPCType) {
				if ctx.AttemptKind == "" {
					ctx.AttemptKind = relay.AttemptProbation
				}
				if dl, has := ctx.Ctx.Deadline(); has {
					if remaining := time.Until(dl); remaining > 0 {
						saved := ctx.Ctx
						short, cancel := context.WithTimeout(saved, remaining/probationBudgetShare)
						ctx.Ctx = short
						defer func() {
							cancel()
							ctx.Ctx = saved
						}()
					}
				}
			}

			return next.HandleRelay(ctx)
		})
	}
}

// errHedgeRuledOut is what SelectEndpoint answers a hedge arm whose every
// candidate is ruled out. No relay was sent; Hedge waits on the primary.
var errHedgeRuledOut = errors.New("hedge: every endpoint left for it is ruled out")

// allRuledOut reports whether every endpoint in eps is ranked out by
// reputation. False when the service cannot say, and for an endpoint with no
// score yet: an unproven host is not a dead one.
func allRuledOut(repSvc reputation.Service, ctx *relay.Context, eps domain.EndpointAddrList) bool {
	c, ok := repSvc.(reputation.RuledOutChecker)
	return ok && !slices.ContainsFunc(eps, func(ep domain.EndpointAddr) bool {
		return !c.RuledOut(ctx.ServiceID, ep, ctx.RPCType)
	})
}

// narrowsIntoStale reports whether narrowing full to narrowed leaves only
// endpoints the service's plugin knows to be far behind the chain head while
// full still held one that is not (qos.StaleChecker).
func narrowsIntoStale(ctx *relay.Context, narrowed, full domain.EndpointAddrList) bool {
	c, ok := ctx.Plugin.(qos.StaleChecker)
	return ok && c.AllStale(narrowed) && !c.AllStale(full)
}

// fillEndpoints fetches the service's endpoints onto ctx when nothing has yet.
//
// Retry and Hedge steer by the pool (the endpoints tried, the primary arm's
// pick), but the pool is fetched by SelectEndpoint, inside the arm, onto the
// arm's clone. On a first attempt the parent's list was therefore empty: a
// hedge excluded its primary from nothing and could land on the same host,
// and a hedged first attempt the deadline ended left Retry no pool to retry
// from. Fetched here once, the arms inherit it and SelectEndpoint skips its
// own fetch. A failed fetch leaves the list empty for SelectEndpoint to fail
// on, as before.
func fillEndpoints(ctx *relay.Context, p protocol.EndpointProvider) {
	if len(ctx.Endpoints) > 0 || p == nil {
		return
	}
	if eps, err := p.AvailableEndpoints(ctx.Ctx, ctx.ServiceID, ctx.RPCType); err == nil {
		ctx.Endpoints = eps
	}
}

// probationBudgetShare is the fraction (1/n) of an attempt's remaining
// deadline a probation first try may use.
const probationBudgetShare = 4
