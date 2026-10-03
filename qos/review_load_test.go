package qos

import (
	"testing"
	"time"
)

// Review of 5fd0d96..c3a3a7a, ef6ac52 (RateSampleGap): a perceived-height move
// within 2s of the last rate sample updates that sample's height but keeps
// its time. On a chain whose head moves more often than every 2s, every
// sample is stamped at the start of its 2s bucket and holds the height at its
// end. Simulated here on a 0.4s chain (2.5 blocks/s), with the clock driven
// by hand through recordRateSampleLocked, exactly as AddObservation calls it.

const (
	fastBlock = 400 * time.Millisecond
	fastRate  = 2.5 // blocks per second at fastBlock
)

// moveFast publishes heights from..to, one per fastBlock starting at base
// (height from at base), and returns when the last one moved.
func moveFast(bc *BlockConsensus, base time.Time, from, to uint64) time.Time {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	var at time.Time
	for h := from; h <= to; h++ {
		at = base.Add(time.Duration(h-from) * fastBlock)
		bc.perceived.Store(h)
		bc.recordRateSampleLocked(h, at)
	}
	return at
}

// The first rate after a boot or Reset. The first bucket's sample holds the
// height reached 1.6s after its time, so the moment the second sample lands
// (t=2.0s) the rate is one block over two seconds: 0.5 blocks/s against a
// true 2.5, and the stale tolerance max(2, rate x 10s) is 5 blocks instead of
// 25. An answer 2.4s behind the head (6 blocks) grades stale: a major
// penalty and a retry. Before ef6ac52 each move was a sample and the rate
// read 2.5 from the second move on.
func TestReviewLoad_FastChainRateAfterBootIsLow(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	base := time.Now()
	last := moveFast(bc, base, 1000, 1005) // t = 0 .. 2.0s

	rate, ok := bc.BlockRate()
	if !ok {
		t.Fatal("precondition: two samples give a rate")
	}
	lag, stale, _ := bc.AnswerLag(999, last.Add(100*time.Millisecond))
	if rate < fastRate/2 || stale {
		t.Fatalf("2.0s after boot on a 0.4s chain: rate %.2f blocks/s (true %.1f); an answer 6 blocks (2.4s) behind graded stale=%v with lag %d",
			rate, fastRate, stale, lag)
	}
}

// Steady state. The rate is right, but Projection's headAt is the bucket's
// start while perceived is the height of the last move, up to 1.6s later.
// AnswerLag projects that far past the real head: an answer naming the
// head the instant it moved reads 4 blocks behind, shrinking the 10s
// tolerance to 8.4s. Project, the height filter's projection, stops 1.6s
// short the same way: a reading taken 0.4s into the bucket is left 3 blocks
// below the head it had caught up with.
func TestReviewLoad_FastChainHeadAtIsTheBucketStart(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	base := time.Now()
	// 25 buckets of five moves; stop on the last move of a bucket.
	const first, last = uint64(1000), uint64(1000 + 5*25 + 4)
	lastAt := moveFast(bc, base, first, last)

	rate, _ := bc.BlockRate()
	lag, _, _ := bc.AnswerLag(last, lastAt)
	if lag > 1 {
		t.Errorf("an answer naming the head (%d) at the instant it moved reads %d blocks behind (rate %.2f, true %.1f); want 0",
			last, lag, rate, fastRate)
	}

	p := bc.Projection()
	readAt := lastAt.Add(-3 * fastBlock) // 0.4s into the bucket
	read := last - 3
	if got := p.Project(read, readAt); got+1 < last {
		t.Errorf("a reading of %d taken %v before the head reached %d projects to %d; want within 1 of the head",
			read, 3*fastBlock, last, got)
	}
}
