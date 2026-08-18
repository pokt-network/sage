package crossvalidation

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
)

const (
	// defaultWindowSize is the maximum number of digests kept per (service, method)
	// window before the oldest are dropped.
	defaultWindowSize = 100

	// defaultMinQuorum is the minimum number of digests required before outliers
	// are flagged. Prevents false positives when only one endpoint has been seen.
	defaultMinQuorum = 3

	// defaultSweepInterval is how often the background goroutine checks for
	// consensus violations.
	defaultSweepInterval = 30 * time.Second
)

// windowKey uniquely identifies a sliding digest window.
type windowKey struct {
	ServiceID domain.ServiceID
	Method    string
}

// responseDigest is a single recorded response hash from one endpoint.
type responseDigest struct {
	Endpoint     domain.EndpointAddr
	ResponseHash [32]byte
	Timestamp    time.Time
}

// digestWindow is a fixed-size sliding window of response digests.
type digestWindow struct {
	digests []responseDigest
	maxSize int
}

// add appends a new digest, dropping the oldest entry when the window is full.
func (w *digestWindow) add(d responseDigest) {
	if len(w.digests) >= w.maxSize {
		// Shift left: drop oldest.
		copy(w.digests, w.digests[1:])
		w.digests[len(w.digests)-1] = d
	} else {
		w.digests = append(w.digests, d)
	}
}

// snapshot returns a copy of the current digests so callers can read without
// holding the lock.
func (w *digestWindow) snapshot() []responseDigest {
	if len(w.digests) == 0 {
		return nil
	}
	out := make([]responseDigest, len(w.digests))
	copy(out, w.digests)
	return out
}

// Validator records response digests and periodically checks for consensus
// violations. It is safe for concurrent use.
type Validator struct {
	mu            sync.RWMutex
	windows       map[windowKey]*digestWindow
	logger        *slog.Logger
	minQuorum     int
	windowSize    int
	sweepInterval time.Duration
}

// NewValidator creates a new Validator with sensible defaults.
// logger is used for warning output when outliers are detected.
func NewValidator(logger *slog.Logger) *Validator {
	return &Validator{
		windows:       make(map[windowKey]*digestWindow),
		logger:        logger,
		minQuorum:     defaultMinQuorum,
		windowSize:    defaultWindowSize,
		sweepInterval: defaultSweepInterval,
	}
}

// RecordDigest hashes responseBody and records it in the window for the given
// (serviceID, method) pair associated with the endpoint. This method is safe
// for concurrent use and is designed to be called from the hot relay path.
func (v *Validator) RecordDigest(serviceID domain.ServiceID, endpoint domain.EndpointAddr, method string, responseBody []byte) {
	hash := sha256.Sum256(responseBody)

	digest := responseDigest{
		Endpoint:     endpoint,
		ResponseHash: hash,
		Timestamp:    time.Now(),
	}

	key := windowKey{ServiceID: serviceID, Method: method}

	v.mu.Lock()
	w, ok := v.windows[key]
	if !ok {
		w = &digestWindow{
			digests: make([]responseDigest, 0, v.windowSize),
			maxSize: v.windowSize,
		}
		v.windows[key] = w
	}
	w.add(digest)
	v.mu.Unlock()
}

// CheckConsensus analyses the current digest window for (serviceID, method) and
// returns any outlier endpoints. Returns nil when there are fewer than
// minQuorum digests or when all endpoints agree.
func (v *Validator) CheckConsensus(serviceID domain.ServiceID, method string) []Outlier {
	key := windowKey{ServiceID: serviceID, Method: method}

	v.mu.RLock()
	w, ok := v.windows[key]
	if !ok {
		v.mu.RUnlock()
		return nil
	}
	digests := w.snapshot()
	v.mu.RUnlock()

	return findOutliers(digests, v.minQuorum)
}

// Start launches a background goroutine that sweeps all windows for consensus
// violations at regular intervals. The goroutine exits when ctx is cancelled.
func (v *Validator) Start(ctx context.Context) {
	safego.GoCtx(ctx, v.logger, "crossvalidation.sweep", v.sweepLoop)
}

// sweepLoop runs CheckConsensus on every known window periodically.
func (v *Validator) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(v.sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			v.sweep()
		}
	}
}

// sweep iterates all known windows and logs any outliers detected.
func (v *Validator) sweep() {
	// Collect keys under a read lock so we don't hold it during analysis.
	v.mu.RLock()
	keys := make([]windowKey, 0, len(v.windows))
	for k := range v.windows {
		keys = append(keys, k)
	}
	v.mu.RUnlock()

	for _, key := range keys {
		outliers := v.CheckConsensus(key.ServiceID, key.Method)
		for _, o := range outliers {
			v.logger.Warn("cross_validation outlier detected",
				slog.String("service_id", string(key.ServiceID)),
				slog.String("method", key.Method),
				slog.String("endpoint", string(o.Endpoint)),
				slog.Int("count", o.Count),
			)
		}
	}
}
