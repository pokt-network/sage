// Package observe provides an async observation pipeline for deep response
// parsing without blocking the hot path.
package observe

import (
	"time"

	"github.com/pokt-network/sage/domain"
)

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
	ServiceID    domain.ServiceID    `json:"service_id"`
	EndpointAddr domain.EndpointAddr `json:"endpoint_addr"`
	Timestamp    time.Time           `json:"timestamp"`
	Source       ObservationSource   `json:"source"`
	Latency      time.Duration       `json:"latency"`
	HTTPStatus   int                 `json:"http_status"`
	RequestBody  []byte              `json:"-"` // not serialized for pub/sub
	ResponseBody []byte              `json:"-"` // not serialized for pub/sub
	Extracted    *ExtractedData      `json:"extracted,omitempty"`
}

// ExtractedData holds parsed fields from a response.
type ExtractedData struct {
	BlockHeight  *uint64 `json:"block_height,omitempty"`
	ChainID      *string `json:"chain_id,omitempty"`
	IsSyncing    *bool   `json:"is_syncing,omitempty"`
	IsArchival   *bool   `json:"is_archival,omitempty"`
	ResponseHash []byte  `json:"response_hash,omitempty"`
}
