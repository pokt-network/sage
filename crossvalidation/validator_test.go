package crossvalidation

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func newTestValidator() *Validator {
	v := NewValidator(slog.Default())
	v.minQuorum = 3
	v.windowSize = 10
	v.sweepInterval = 100 * time.Millisecond
	return v
}

func TestValidator_RecordAndCheckConsensus_NoOutliers(t *testing.T) {
	v := newTestValidator()

	body := []byte(`{"result":"0x1"}`)
	v.RecordDigest("eth", "ep1", "eth_blockNumber", body)
	v.RecordDigest("eth", "ep2", "eth_blockNumber", body)
	v.RecordDigest("eth", "ep3", "eth_blockNumber", body)

	outliers := v.CheckConsensus("eth", "eth_blockNumber")
	if len(outliers) != 0 {
		t.Errorf("expected no outliers when all agree, got %v", outliers)
	}
}

func TestValidator_RecordAndCheckConsensus_FindsOutlier(t *testing.T) {
	v := newTestValidator()

	majority := []byte(`{"result":"0x1"}`)
	minority := []byte(`{"result":"0xdeadbeef"}`)

	v.RecordDigest("eth", "ep1", "eth_blockNumber", majority)
	v.RecordDigest("eth", "ep2", "eth_blockNumber", majority)
	v.RecordDigest("eth", "ep3", "eth_blockNumber", majority)
	v.RecordDigest("eth", "ep4", "eth_blockNumber", minority)

	outliers := v.CheckConsensus("eth", "eth_blockNumber")
	if len(outliers) != 1 {
		t.Fatalf("expected 1 outlier, got %d: %v", len(outliers), outliers)
	}
	if outliers[0].Endpoint != "ep4" {
		t.Errorf("expected ep4 as outlier, got %q", outliers[0].Endpoint)
	}

	want := sha256.Sum256(minority)
	if outliers[0].Hash != want {
		t.Errorf("outlier hash mismatch: got %x, want %x", outliers[0].Hash, want)
	}
}

func TestValidator_CheckConsensus_QuorumNotMet(t *testing.T) {
	v := newTestValidator()

	// Only 2 digests recorded (minQuorum=3).
	v.RecordDigest("eth", "ep1", "eth_blockNumber", []byte(`{"result":"0x1"}`))
	v.RecordDigest("eth", "ep2", "eth_blockNumber", []byte(`{"result":"0x2"}`))

	outliers := v.CheckConsensus("eth", "eth_blockNumber")
	if outliers != nil {
		t.Errorf("expected nil when quorum not met, got %v", outliers)
	}
}

func TestValidator_CheckConsensus_UnknownKey(t *testing.T) {
	v := newTestValidator()
	outliers := v.CheckConsensus("unknown", "unknown_method")
	if outliers != nil {
		t.Errorf("expected nil for unknown key, got %v", outliers)
	}
}

func TestValidator_WindowEviction(t *testing.T) {
	v := newTestValidator()
	v.windowSize = 5

	body := []byte(`{"result":"0x1"}`)
	// Fill window beyond capacity.
	for i := 0; i < 10; i++ {
		v.RecordDigest("eth", domain.EndpointAddr("ep1"), "m", body)
	}

	v.mu.RLock()
	w := v.windows[windowKey{ServiceID: "eth", Method: "m"}]
	size := len(w.digests)
	v.mu.RUnlock()

	if size > v.windowSize {
		t.Errorf("window size exceeded maxSize: got %d, max %d", size, v.windowSize)
	}
}

func TestValidator_SeparatesWindowsByMethod(t *testing.T) {
	v := newTestValidator()

	majority := []byte(`{"result":"0x1"}`)
	minority := []byte(`{"result":"0xdead"}`)

	// For method "m1": majority agrees.
	v.RecordDigest("eth", "ep1", "m1", majority)
	v.RecordDigest("eth", "ep2", "m1", majority)
	v.RecordDigest("eth", "ep3", "m1", majority)

	// For method "m2": ep4 disagrees.
	v.RecordDigest("eth", "ep1", "m2", majority)
	v.RecordDigest("eth", "ep2", "m2", majority)
	v.RecordDigest("eth", "ep3", "m2", majority)
	v.RecordDigest("eth", "ep4", "m2", minority)

	if len(v.CheckConsensus("eth", "m1")) != 0 {
		t.Error("expected no outliers for m1")
	}
	if len(v.CheckConsensus("eth", "m2")) != 1 {
		t.Errorf("expected 1 outlier for m2, got %v", v.CheckConsensus("eth", "m2"))
	}
}

func TestValidator_SeparatesWindowsByService(t *testing.T) {
	v := newTestValidator()

	majority := []byte(`{"result":"0x1"}`)
	minority := []byte(`{"result":"0xdead"}`)

	// eth: all agree.
	v.RecordDigest("eth", "ep1", "method", majority)
	v.RecordDigest("eth", "ep2", "method", majority)
	v.RecordDigest("eth", "ep3", "method", majority)

	// poly: ep4 disagrees.
	v.RecordDigest("poly", "ep1", "method", majority)
	v.RecordDigest("poly", "ep2", "method", majority)
	v.RecordDigest("poly", "ep3", "method", majority)
	v.RecordDigest("poly", "ep4", "method", minority)

	if len(v.CheckConsensus("eth", "method")) != 0 {
		t.Error("expected no outliers for eth")
	}
	if len(v.CheckConsensus("poly", "method")) != 1 {
		t.Error("expected 1 outlier for poly")
	}
}

func TestValidator_Start_RunsBackgroundSweep(t *testing.T) {
	v := newTestValidator()
	v.sweepInterval = 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	v.Start(ctx)

	// Add some digests.
	body := []byte(`{"result":"0x1"}`)
	for i := 0; i < 5; i++ {
		v.RecordDigest("eth", "ep1", "method", body)
	}

	// Let the background goroutine sweep at least once.
	<-ctx.Done()
	// If no panic occurred, the background goroutine ran cleanly.
}

func TestValidator_ConcurrentRecordDigest(t *testing.T) {
	v := newTestValidator()

	done := make(chan struct{})
	body := []byte(`{"result":"0x1"}`)

	for i := 0; i < 50; i++ {
		go func() {
			v.RecordDigest("eth", "ep1", "method", body)
			done <- struct{}{}
		}()
	}

	for i := 0; i < 50; i++ {
		<-done
	}

	// No race detector errors = pass.
}
