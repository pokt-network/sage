package middleware

import (
	"crypto/sha256"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// Review of 5fd0d96..c3a3a7a: the cost per request of the route-aware cache
// and singleflight keys (b3146b2). The pre-change bodies are copied here as
// the baseline. Both keys are in-process only (responsecache is an in-memory
// LRU, singleflight a local group), so the format change costs nothing on
// the roll: only this, per request.

func coalescingKeyBefore(serviceID domain.ServiceID, p domain.Payload) string {
	sum := sha256.Sum256(p.Bytes())
	return string(serviceID) + ":" + p.Method() + ":" + string(sum[:])
}

func cacheKeyBefore(serviceID domain.ServiceID, payloads []domain.Payload) string {
	h := sha256.New()
	_, _ = h.Write([]byte(serviceID))
	for _, p := range payloads {
		_, _ = h.Write([]byte(p.Method()))
		_, _ = h.Write(p.Bytes())
	}
	var sum [sha256.Size]byte
	return string(h.Sum(sum[:0]))
}

var reviewPayload = domain.NewPayload(
	[]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x1234567",false]}`),
	domain.RPCTypeJSONRPC, "eth_getBlockByNumber")

var reviewSink string

func BenchmarkReviewLoad_CoalescingKeyBefore(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		reviewSink = coalescingKeyBefore("eth", reviewPayload)
	}
}

func BenchmarkReviewLoad_CoalescingKeyAfter(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		reviewSink = coalescingKey("eth", reviewPayload)
	}
}

func BenchmarkReviewLoad_CacheKeyBefore(b *testing.B) {
	ps := []domain.Payload{reviewPayload}
	b.ReportAllocs()
	for b.Loop() {
		reviewSink = cacheKeyBefore("eth", ps)
	}
}

func BenchmarkReviewLoad_CacheKeyAfter(b *testing.B) {
	ps := []domain.Payload{reviewPayload}
	b.ReportAllocs()
	for b.Loop() {
		reviewSink = cacheKey("eth", ps)
	}
}
