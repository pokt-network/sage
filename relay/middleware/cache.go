package middleware

import (
	"crypto/sha256"
	"hash"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/responsecache"
)

// CacheRecorder is notified of cache hits and misses. metrics.Recorder
// satisfies it. Nil disables recording.
type CacheRecorder interface {
	RecordCacheHit(serviceID domain.ServiceID)
	RecordCacheMiss(serviceID domain.ServiceID)
}

// Cache returns a middleware that serves repeated identical relay requests
// from an in-memory response cache, bypassing the upstream relay entirely on
// a cache hit, recording sage_cache_hits_total / sage_cache_misses_total when
// rec is non-nil.
//
// Caching is only applied when the "cache" feature flag is enabled for the
// service and the QoS plugin implements CachePolicy with a positive TTL.
func Cache(flags featureflag.FlagStore, cache *responsecache.Cache, rec CacheRecorder) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if ctx.QuorumArm || !flags.IsEnabled(ctx.Ctx, featureflag.FlagCache, ctx.ServiceID) {
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
				if rec != nil {
					rec.RecordCacheHit(ctx.ServiceID)
				}
				return nil
			}

			// Cache miss: run the inner chain.
			if rec != nil {
				rec.RecordCacheMiss(ctx.ServiceID)
			}
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

// cacheKey builds a deterministic string key for the response cache: the
// raw sha256 of the service and every payload's request identity
// (writeRequestKey). The key is only ever a map key (never displayed), so hex
// encoding would just double its size and add an allocation per request.
func cacheKey(serviceID domain.ServiceID, payloads []domain.Payload) string {
	h := sha256.New()
	_, _ = h.Write([]byte(serviceID))
	for _, p := range payloads {
		writeRequestKey(h, p)
	}
	var sum [sha256.Size]byte
	return string(h.Sum(sum[:0]))
}

// writeRequestKey writes what makes two payloads the same request: their
// bytes and the route they are sent on. Identical bytes are not an identical
// request on another RPC type, path or verb: a REST route and the JSON-RPC
// root answer different questions, and keyed on method and bytes alone a
// client could put one route's answer in front of every client of the other
// for as long as the entry lived. Each field ends in a NUL so no two splits
// of the same characters collide.
func writeRequestKey(h hash.Hash, p domain.Payload) {
	for _, field := range []string{string(p.RPCType()), p.HTTPMethod(), p.Path(), p.Method()} {
		_, _ = h.Write([]byte(field))
		_, _ = h.Write([]byte{0})
	}
	_, _ = h.Write(p.Bytes())
}
