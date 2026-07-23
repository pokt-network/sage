package middleware

import (
	"crypto/sha256"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/responsecache"
)

// Cache returns a middleware that serves repeated identical relay requests
// from an in-memory response cache, bypassing the upstream relay entirely on
// a cache hit.
//
// Caching is only applied when:
//   - The "cache" feature flag is enabled for the service.
//   - The QoS plugin for the service implements CachePolicy and returns a
//     positive TTL for the method + params + response combination.
func Cache(flags featureflag.FlagStore, cache *responsecache.Cache) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if !flags.IsEnabled(ctx.Ctx, featureflag.FlagCache, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}

			// Without a CachePolicy the service can never cache — skip the
			// SHA-256 key computation and cache probe entirely.
			cp, hasPolicy := ctx.Plugin.(qos.CachePolicy)
			if !hasPolicy {
				return next.HandleRelay(ctx)
			}

			key := cacheKey(ctx.ServiceID, ctx.Payloads)

			// Cache hit: serve from cache, skip inner chain.
			if resp, ok := cache.Get(key); ok {
				ctx.Response = resp
				ctx.Cached = true
				return nil
			}

			// Cache miss: run the inner chain.
			if err := next.HandleRelay(ctx); err != nil {
				return err
			}

			// Store the response if the plugin defines a positive TTL.
			if ctx.Response != nil {
				var params []byte
				var method string
				if len(ctx.Payloads) == 1 {
					params = ctx.Payloads[0].Bytes()
					method = ctx.Payloads[0].Method()
				}
				ttl := cp.CacheTTL(method, params, ctx.Response.Body)
				if ttl > 0 {
					cache.Set(key, ctx.Response, ttl)
				}
			}

			return nil
		})
	}
}

// cacheKey builds a deterministic string key for the response cache:
// sha256(serviceID + method_0 + bytes_0 + ...) as raw bytes. The key is only
// ever a map key (never displayed), so hex encoding would just double its
// size and add an allocation per request.
func cacheKey(serviceID domain.ServiceID, payloads []domain.Payload) string {
	h := sha256.New()
	_, _ = h.Write([]byte(serviceID))
	for _, p := range payloads {
		_, _ = h.Write([]byte(p.Method()))
		_, _ = h.Write(p.Bytes())
	}
	var sum [sha256.Size]byte
	return string(h.Sum(sum[:0]))
}
