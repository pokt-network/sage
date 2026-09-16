package qos

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func TestHeightProjection_Project(t *testing.T) {
	head := time.Now()
	p := HeightProjection{rate: 2, headAt: head, perceived: 1000, window: 2 * time.Minute}
	for name, tc := range map[string]struct {
		p      HeightProjection
		height uint64
		at     time.Time
		want   uint64
	}{
		"read 30s before the head":        {p, 900, head.Add(-30 * time.Second), 960},
		"never above the head":            {p, 990, head.Add(-30 * time.Second), 1000},
		"just over one window":            {p, 700, head.Add(-150 * time.Second), 1000},
		"older than two windows":          {p, 700, head.Add(-5 * time.Minute), 700},
		"read after the head":             {p, 990, head.Add(time.Second), 990},
		"no observation time":             {p, 900, time.Time{}, 900},
		"rate unknown":                    {HeightProjection{perceived: 1000, window: time.Minute}, 900, head.Add(-30 * time.Second), 900},
		"already at the head":             {p, 1000, head.Add(-30 * time.Second), 1000},
		"zero projection changes nothing": {HeightProjection{}, 900, head.Add(-30 * time.Second), 900},
	} {
		if got := tc.p.Project(tc.height, tc.at); got != tc.want {
			t.Errorf("%s: Project(%d) = %d, want %d", name, tc.height, got, tc.want)
		}
	}
}

// The case the projection exists for: an endpoint read a probe cycle before
// the head is behind by the blocks produced in between, not by lag. Raw, it
// fails a filter it passes once projected; a genuinely lagging endpoint read
// at the same moment still fails.
func TestHeightGetter_JudgesReadingsAtTheHeadsMoment(t *testing.T) {
	type ep struct{ height uint64 }
	store := NewEndpointStore[ep](nil)
	store.ObserveHeight("pokt1a-https://fresh.example.com", func(e *ep) { e.height = 880 })
	store.ObserveHeight("pokt1b-https://lagging.example.com", func(e *ep) { e.height = 700 })
	// Both were read 60 s before the head, at 2 blocks a second.
	head := time.Now()
	store.mu.Lock()
	for addr, e := range store.endpoints {
		e.HeightAt = head.Add(-60 * time.Second)
		store.endpoints[addr] = e
	}
	store.mu.Unlock()

	projection := HeightProjection{rate: 2, headAt: head, perceived: 1000, window: 2 * time.Minute}
	filter := BlockHeightFilter(HeightGetter(store, func(e ep) uint64 { return e.height }, projection), MinAllowedHeight(1000, 50))
	raw := BlockHeightFilter(HeightGetter(store, func(e ep) uint64 { return e.height }, HeightProjection{}), MinAllowedHeight(1000, 50))

	if raw("pokt1a-https://fresh.example.com") == nil {
		t.Fatal("precondition: raw, the endpoint read a minute ago looks 120 blocks behind")
	}
	if err := filter("pokt1a-https://fresh.example.com"); err != nil {
		t.Errorf("projected 880 + 120 = 1000 passes an allowance of 50, got %v", err)
	}
	if filter("pokt1b-https://lagging.example.com") == nil {
		t.Error("projected 700 + 120 = 820 is still 180 behind and must be filtered")
	}
}

// Update without a height, as a chain-id check does, must not make an old
// height reading look fresh.
func TestEndpointStore_UpdateKeepsTheHeightTime(t *testing.T) {
	type ep struct{ height, other uint64 }
	store := NewEndpointStore[ep](nil)
	addr := domain.EndpointAddr("pokt1a-https://a.example.com")
	store.ObserveHeight(addr, func(e *ep) { e.height = 5 })
	store.mu.Lock()
	e := store.endpoints[addr]
	old := time.Now().Add(-time.Minute)
	e.HeightAt = old
	store.endpoints[addr] = e
	store.mu.Unlock()

	store.Update(addr, func(e *ep) { e.other = 1 })
	store.mu.RLock()
	defer store.mu.RUnlock()
	if got := store.endpoints[addr].HeightAt; !got.Equal(old) {
		t.Errorf("HeightAt moved to %v on an update with no height", got)
	}
}

func TestBlockConsensus_Projection(t *testing.T) {
	bc := NewBlockConsensus(nil, 10)
	if (bc.Projection() != HeightProjection{perceived: 0, window: bc.windowDuration}) {
		t.Error("a consensus with no rate history must project nothing")
	}
	now := time.Now()
	bc.mu.Lock()
	bc.rateSamples = []rateSample{{height: 100, at: now.Add(-50 * time.Second)}, {height: 200, at: now}}
	bc.perceived.Store(200)
	bc.mu.Unlock()
	p := bc.Projection()
	if p.rate != 2 || !p.headAt.Equal(now) || p.perceived != 200 {
		t.Errorf("Projection = %+v, want rate 2 at the newest sample, perceived 200", p)
	}
}
