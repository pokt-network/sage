// Package observe provides an async observation pipeline for deep response
// parsing without blocking the hot path.
package observe

import "github.com/pokt-network/sage/domain"

// ObservationSource indicates where an observation originated.
type ObservationSource string

// Where an observation came from. The two are sampled at different rates —
// relays at a fraction, health checks at every one — because health checks are
// low-volume and deliberate, while relay traffic is the hot path.
const (
	SourceRelay       ObservationSource = "relay"
	SourceHealthCheck ObservationSource = "health_check"
)

// Observation captures data about a single relay or health check interaction.
type Observation struct {
	ServiceID    domain.ServiceID
	EndpointAddr domain.EndpointAddr
	Source       ObservationSource
	RequestBody  []byte
	ResponseBody []byte
}
