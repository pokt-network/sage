package healthcheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/pokt-network/sage/domain"
)

// HC-1. A probe whose endpoint left the session between ProbeEndpoints and
// SendRelay comes back as the relayer's ErrEndpointsStale: the gateway's
// session rolled over, the supplier did nothing. The relay path already
// exempts it (observe.go, score.go, retry.go); the probe path must too.
func TestAudit2_StaleSessionProbeRecordsNoSignal(t *testing.T) {
	staleErr := domain.NewRelayError(domain.ErrTransport,
		"endpoint supplierA-https://node1.example.com not in current session after rollover",
		domain.ErrEndpointsStale, true)
	rep := &stubRepService{}
	exe := newTestExecutor(&stubRelayer{err: staleErr},
		&stubEndpointProvider{endpoints: domain.EndpointAddrList{"supplierA-https://node1.example.com"}},
		&stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}}},
		probeableRegistry(t, "eth"), rep)
	exe.runOnce(context.Background())
	exe.wg.Wait()

	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.signals) != 0 {
		s := rep.signals[0]
		t.Fatalf("a stale-session probe recorded %q (reason %q) against %s; the session rollover is the gateway's, not the supplier's",
			s.signal.Type, s.signal.Reason, s.endpoint)
	}
}

// HC-2. A peer's transport failure says something about the peer's path to
// the backend (its session, its signer, its network), not necessarily about
// the backend. Applied to a registration this instance also holds, it scores
// a supplier for a failure this instance never saw.
func TestAudit2_PeerTransportFailureNotAppliedOnHeld(t *testing.T) {
	t.Skip("unfixed: audit 2026-10-03 finding #18; delete this line with the fix")
	exec, _, rep := peerExecutor(t)
	r := peerResult(localA, time.Now())
	r.StatusCode, r.Body = 0, nil
	r.TransportError = "endpoint not in current session after rollover: endpoints stale: session rolled over"
	r.TransportReason, r.TransportSeverity = "transport_error", "minor"
	exec.applyPeerResult(context.Background(), r)

	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.signals) != 0 {
		s := rep.signals[0]
		t.Fatalf("peer's transport failure applied here as %q (reason %q) on %s; want no signal",
			s.signal.Type, s.signal.Reason, s.endpoint)
	}
}

// HC-6. A peer result older than max_age does not cover a check (coveredByPeer)
// and must not be applied as a reputation signal either: it describes the
// backend as it was, and a booting replica replaying the stream would score
// suppliers on hour-old evidence.
func TestAudit2_PeerResultOlderThanMaxAgeNotApplied(t *testing.T) {
	t.Skip("unfixed: audit 2026-10-03 finding #18; delete this line with the fix")
	exec, _, rep := peerExecutor(t)
	exec.SetPeerSource(idleSource{}, func(domain.ServiceID) time.Duration { return time.Minute })
	exec.applyPeerResult(context.Background(), peerResult(peerOnNode1, time.Now().Add(-time.Hour)))

	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.signals) != 0 {
		t.Fatalf("an hour-old peer result (max age 1m) applied %d signal(s); want none", len(rep.signals))
	}
}

// audit2Stream is a StreamRedisClient whose XRange honours its start id, so a
// reader that resumes from where it left off sees only what is new. The first
// xreadErrs XRead calls fail; after that XRead blocks until ctx is done.
type audit2Stream struct {
	mu        sync.Mutex
	entries   []redis.XMessage
	xreadErrs int
}

func (f *audit2Stream) XAdd(context.Context, *redis.XAddArgs) *redis.StringCmd {
	return redis.NewStringResult("", nil)
}

func (f *audit2Stream) XRange(_ context.Context, _, start, _ string) *redis.XMessageSliceCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []redis.XMessage
	for _, m := range f.entries {
		if streamIDAtOrAfter(m.ID, start) {
			out = append(out, m)
		}
	}
	return redis.NewXMessageSliceCmdResult(out, nil)
}

func (f *audit2Stream) XRead(ctx context.Context, _ *redis.XReadArgs) *redis.XStreamSliceCmd {
	f.mu.Lock()
	if f.xreadErrs > 0 {
		f.xreadErrs--
		f.mu.Unlock()
		return redis.NewXStreamSliceCmdResult(nil, errors.New("i/o timeout"))
	}
	f.mu.Unlock()
	<-ctx.Done()
	return redis.NewXStreamSliceCmdResult(nil, ctx.Err())
}

// streamIDAtOrAfter reports whether id is inside an XRANGE starting at start:
// "-" is everything, "(" makes the bound exclusive.
func streamIDAtOrAfter(id, start string) bool {
	if start == "-" || start == "" {
		return true
	}
	exclusive := strings.HasPrefix(start, "(")
	start = strings.TrimPrefix(start, "(")
	c := compareStreamIDs(id, start)
	return c > 0 || (c == 0 && !exclusive)
}

func compareStreamIDs(a, b string) int {
	am, as := splitStreamID(a)
	bm, bs := splitStreamID(b)
	switch {
	case am != bm:
		if am < bm {
			return -1
		}
		return 1
	case as != bs:
		if as < bs {
			return -1
		}
		return 1
	}
	return 0
}

func splitStreamID(id string) (ms, seq uint64) {
	m, s, _ := strings.Cut(id, "-")
	ms, _ = strconv.ParseUint(m, 10, 64)
	seq, _ = strconv.ParseUint(s, 10, 64)
	return ms, seq
}

// HC-3. runFeed restarts Run after any read error. Each restart replays the
// whole window from scratch, so one probe result is applied once per restart:
// a flapping Redis connection multiplies every signal in the window.
func TestAudit2_StreamRestartDoesNotReapplyWindow(t *testing.T) {
	t.Skip("unfixed: audit 2026-10-03 finding #18; delete this line with the fix")
	raw, _ := json.Marshal(streamEntry{Producer: "leader-1", Result: ProbeResult{ServiceID: "eth", Check: "block_number"}})
	f := &audit2Stream{
		entries:   []redis.XMessage{{ID: fmt.Sprintf("%d-0", time.Now().UnixMilli()), Values: map[string]any{probeStreamField: string(raw)}}},
		xreadErrs: 1,
	}
	s := NewRedisProbeStream(f, "follower-1", 4*time.Minute, "")
	applied := 0
	apply := func(ProbeResult) { applied++ }

	if err := s.Run(context.Background(), apply); err == nil {
		t.Fatal("precondition: the first Run must end on the XRead error")
	}
	if applied != 1 {
		t.Fatalf("precondition: the first Run applied the entry %d times, want 1", applied)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = s.Run(ctx, apply)

	if applied != 1 {
		t.Fatalf("one stream entry applied %d times across one feed restart; want 1", applied)
	}
}
