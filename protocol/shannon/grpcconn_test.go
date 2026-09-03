package shannon

import (
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// newIdleConn builds a real ClientConn, which grpc.NewClient creates lazily —
// no socket is opened until an RPC is made, so this touches no network. A
// fabricated grpcConn with a nil conn would not do: the sweep closes what it
// evicts, and a nil there is a state production cannot reach.
func newIdleConn(t *testing.T, target string, lastUsed time.Time) *grpcConn {
	t.Helper()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	entry := &grpcConn{conn: conn}
	entry.lastUsed.Store(lastUsed.UnixNano())
	return entry
}

// A ClientConn is a live socket, and a supplier that leaves the network never
// comes back — so without idle eviction the process holds one per gRPC host it
// has ever relayed to, for its whole life.
func TestSweepIdleConns_ClosesWhatWentQuiet(t *testing.T) {
	tr := newGRPCRelayTransport(GRPCModeNative, nil, nil)
	now := time.Now()

	// Two hosts: one used recently, one long idle.
	tr.conns.Store("fresh.example.com:443", newIdleConn(t, "fresh.example.com:443", now.Add(-time.Minute)))
	tr.conns.Store("stale.example.com:443", newIdleConn(t, "stale.example.com:443", now.Add(-2*grpcConnIdleTTL)))

	tr.sweepIdleConns(now)

	if _, ok := tr.conns.Load("fresh.example.com:443"); !ok {
		t.Error("closed a connection used a minute ago")
	}
	if _, ok := tr.conns.Load("stale.example.com:443"); ok {
		t.Errorf("kept a connection idle for %v", 2*grpcConnIdleTTL)
	}
}

// The sweep is called from the dial path, so it has to be cheap when called
// often: at most one walk per interval.
func TestSweepIdleConns_RunsAtMostOncePerInterval(t *testing.T) {
	tr := newGRPCRelayTransport(GRPCModeNative, nil, nil)
	now := time.Now()

	tr.conns.Store("stale.example.com:443", newIdleConn(t, "stale.example.com:443", now.Add(-2*grpcConnIdleTTL)))

	tr.sweepIdleConns(now)
	if _, ok := tr.conns.Load("stale.example.com:443"); ok {
		t.Fatal("precondition: first sweep did not evict")
	}

	// A second stale entry arrives immediately: the sweep must not run again
	// until the interval has passed.
	tr.conns.Store("other.example.com:443", newIdleConn(t, "other.example.com:443", now.Add(-2*grpcConnIdleTTL)))

	tr.sweepIdleConns(now.Add(grpcConnSweepInterval / 2))
	if _, ok := tr.conns.Load("other.example.com:443"); !ok {
		t.Error("swept twice inside one interval")
	}

	tr.sweepIdleConns(now.Add(grpcConnSweepInterval * 2))
	if _, ok := tr.conns.Load("other.example.com:443"); ok {
		t.Error("did not sweep once the interval had passed")
	}
}

// Using a host keeps its connection: a service probed once per health-check
// cycle must not lose its connection between cycles.
func TestConn_UseKeepsTheConnectionAlive(t *testing.T) {
	tr := newGRPCRelayTransport(GRPCModeNative, nil, nil)
	now := time.Now()

	entry := newIdleConn(t, "host.example.com:443", now.Add(-2*grpcConnIdleTTL))
	tr.conns.Store("host.example.com:443", entry)

	// A relay to it touches the entry...
	if _, ok := tr.conns.Load("host.example.com:443"); !ok {
		t.Fatal("precondition")
	}
	entry.touch(now)

	tr.sweepIdleConns(now)
	if _, ok := tr.conns.Load("host.example.com:443"); !ok {
		t.Error("evicted a connection that was just used")
	}
}
