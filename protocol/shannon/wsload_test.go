package shannon

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// An endpoint address carries a staked supplier that rotates every session, so
// a counter per address ever bound is a map that grows for the life of the
// process. Zero carries no information, so the entry goes.
func TestWSRelayer_ActiveLoadDeletesAtZero(t *testing.T) {
	r := &WSRelayer{}
	const ep = domain.EndpointAddr("supA-https://a.example.com")

	r.incLoad(ep)
	r.incLoad(ep)
	if got := r.snapshotLoad()[ep]; got != 2 {
		t.Fatalf("load = %d, want 2", got)
	}

	r.decLoad(ep)
	if len(r.activeLoad) != 1 {
		t.Fatal("entry removed while a bridge is still open")
	}

	r.decLoad(ep)
	if _, present := r.activeLoad[ep]; present {
		t.Error("entry survived at zero; a rotated supplier would cost a counter forever")
	}
	if got := r.snapshotLoad()[ep]; got != 0 {
		t.Errorf("snapshot reports %d for an endpoint with nothing bound", got)
	}
}

// Many endpoints, all closed, must leave nothing behind — this is the shape
// session rotation produces over a day.
func TestWSRelayer_ActiveLoadLeavesNothingAfterChurn(t *testing.T) {
	r := &WSRelayer{}
	for i := range 500 {
		ep := domain.EndpointAddr("sup" + string(rune('A'+i%26)) + "-https://host.example.com/" + string(rune('a'+i%26)))
		r.incLoad(ep)
		r.decLoad(ep)
	}

	if n := len(r.activeLoad); n != 0 {
		t.Errorf("%d entries left after 500 open/close pairs, want 0", n)
	}
}

// The delete races an increment: the count must never be lost, because it
// steers selection away from hot endpoints.
func TestWSRelayer_ActiveLoadSurvivesConcurrentOpenAndClose(t *testing.T) {
	r := &WSRelayer{}
	const ep = domain.EndpointAddr("supA-https://a.example.com")
	const workers = 8
	const rounds = 200

	var wg sync.WaitGroup
	var opened atomic.Int64
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds {
				r.incLoad(ep)
				opened.Add(1)
				r.decLoad(ep)
				opened.Add(-1)
			}
		}()
	}
	wg.Wait()

	if opened.Load() != 0 {
		t.Fatalf("test bookkeeping is off: %d", opened.Load())
	}
	// Everything closed, so nothing should remain — and nothing should have
	// gone negative on the way.
	if got := r.snapshotLoad()[ep]; got != 0 {
		t.Errorf("load = %d after every bridge closed, want 0", got)
	}
	if n := len(r.activeLoad); n != 0 {
		t.Errorf("%d entries left after every bridge closed, want 0", n)
	}
}
