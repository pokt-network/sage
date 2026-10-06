package shannon

import (
	"testing"

	"github.com/pokt-network/sage/domain"
)

// A supplier's relays are set against the median of its session peers; a new
// session starts the count over and a late relay of the old one is dropped.
func TestSessionLoad_RatioAgainstPeers(t *testing.T) {
	l := newSessionLoad()
	add := func(supplier string, end int64, n int) {
		for range n {
			l.add("eth", supplier, end)
		}
	}
	add("a", 100, 100)
	add("b", 100, 120)
	add("c", 100, 80)
	add("early", 100, 10)

	if r, ok := l.ratio("eth", "early", 100); !ok || r != 0.1 {
		t.Errorf("early refuser: %v %v, want 0.1 (10 against a median of 100)", r, ok)
	}
	if _, ok := l.ratio("eth", "a", 100); !ok {
		t.Error("three peers is enough to say")
	}
	if _, ok := l.ratio("eth", "a", 99); ok {
		t.Error("a session not held has no ratio")
	}

	add("a", 101, 5)
	add("b", 100, 50) // late relay of the old session
	if r, ok := l.ratio("eth", "a", 101); ok {
		t.Errorf("one supplier in the new session has no peers, got %v", r)
	}
	if _, ok := l.ratio("eth", "early", 100); ok {
		t.Error("the old session's counts were not dropped")
	}

	var nilLoad *sessionLoad
	nilLoad.add("eth", "a", 1) // a Protocol built as a literal has none
	if _, ok := nilLoad.ratio("eth", "a", 1); ok {
		t.Error("nil load has a ratio")
	}
}

// loadMetrics records the ratios markOverServed observes.
type loadMetrics struct {
	noopSupplierMetrics
	ratios map[string]float64
}

func (m *loadMetrics) RecordOverServedLoad(_ domain.ServiceID, operator string, ratio float64) {
	m.ratios[operator] = ratio
}

// The first refusal of a session records what the supplier took against its
// peers, once.
func TestMarkOverServed_RecordsLoadOnce(t *testing.T) {
	m := &loadMetrics{ratios: map[string]float64{}}
	p := &Protocol{overServed: newOverServed(), sessionLoad: newSessionLoad(), metrics: m}
	early := domain.EndpointAddr("pokt1early-https://r1.early.example")
	for _, s := range []string{"pokt1a", "pokt1b", "pokt1c"} {
		for range 40 {
			p.sessionLoad.add("eth", s, 100)
		}
	}
	for range 4 {
		p.sessionLoad.add("eth", early.Supplier(), 100)
	}
	p.markOverServed("eth", early, 100)
	if r := m.ratios["early.example"]; r != 0.1 {
		t.Fatalf("ratio %v, want 0.1", r)
	}
	m.ratios = map[string]float64{}
	p.markOverServed("eth", early, 100)
	if len(m.ratios) != 0 {
		t.Error("a second refusal in the same session recorded again")
	}
}
