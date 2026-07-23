package observe

import (
	"sync"
	"testing"
)

func TestCollector_AddAndDrain(t *testing.T) {
	c := NewCollector()

	c.Add(Observation{ServiceID: "eth"})
	c.Add(Observation{ServiceID: "poly"})

	obs := c.Drain()
	if len(obs) != 2 {
		t.Fatalf("expected 2 observations, got %d", len(obs))
	}
	if obs[0].ServiceID != "eth" || obs[1].ServiceID != "poly" {
		t.Error("unexpected observation order")
	}

	// Drain again should be empty.
	obs = c.Drain()
	if len(obs) != 0 {
		t.Errorf("expected 0 observations after second drain, got %d", len(obs))
	}
}

func TestCollector_ConcurrentAdd(t *testing.T) {
	c := NewCollector()
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			c.Add(Observation{ServiceID: "eth"})
		}()
	}
	wg.Wait()

	obs := c.Drain()
	if len(obs) != n {
		t.Errorf("expected %d observations, got %d", n, len(obs))
	}
}
