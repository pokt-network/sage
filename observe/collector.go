package observe

import "sync"

// Collector accumulates observations during a single request lifecycle.
// It is safe for concurrent use.
type Collector struct {
	mu           sync.Mutex
	observations []Observation
}

// NewCollector creates a new empty Collector.
func NewCollector() *Collector {
	return &Collector{}
}

// Add appends an observation to the collector.
func (c *Collector) Add(obs Observation) {
	c.mu.Lock()
	c.observations = append(c.observations, obs)
	c.mu.Unlock()
}

// Drain returns all collected observations and resets the collector.
func (c *Collector) Drain() []Observation {
	c.mu.Lock()
	out := c.observations
	c.observations = nil
	c.mu.Unlock()
	return out
}
