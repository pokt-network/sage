package healthcheck

import (
	"context"
	"time"

	"github.com/pokt-network/sage/domain"
)

// ProbeResult is one health-check probe as a fact the whole fleet can apply:
// which backend was asked what, and what came back. It is what the leader
// publishes and what followers consume, so that a probe — a paid relay
// against the app's stake — is sent once and its knowledge lands on every
// replica.
//
// A transport failure travels as its verdict (reason and severity from
// heuristic.AnalyzeTransportError), not as the error: the error's shape
// does not survive serialisation, and the verdict is all applyResult needs.
type ProbeResult struct {
	ServiceID domain.ServiceID `json:"service_id"`
	// Endpoint is the registration that was probed; Siblings are every
	// registration on the same backend, Endpoint included. What the backend
	// said applies to all of them; a transport failure to Endpoint alone.
	Endpoint domain.EndpointAddr     `json:"endpoint"`
	Siblings domain.EndpointAddrList `json:"siblings"`
	Check    string                  `json:"check"`
	RPCType  domain.RPCType          `json:"rpc_type"`
	Request  []byte                  `json:"request"`

	StatusCode int    `json:"status_code,omitempty"`
	Body       []byte `json:"body,omitempty"`
	LatencyMS  int64  `json:"latency_ms"`

	TransportError    string `json:"transport_error,omitempty"`
	TransportReason   string `json:"transport_reason,omitempty"`
	TransportSeverity string `json:"transport_severity,omitempty"`

	ProbedAt time.Time `json:"probed_at"`

	// Source says whether this replica produced the result or received it.
	// Not serialised: it is a fact about the receiver.
	Source ResultSource `json:"-"`
}

// ResultSource is where a result came from, for the metric.
type ResultSource string

// The two ways a result reaches applyResult.
const (
	// ResultSourceProbe: this replica sent the relay.
	ResultSourceProbe ResultSource = "probe"
	// ResultSourceStream: another replica sent it and published the result.
	ResultSourceStream ResultSource = "stream"
)

// maxProbeBodyBytes caps the response body a result carries. Health-check
// answers are small (a block number, a chain id, a status document); a
// larger one is truncated, and a follower that cannot parse the truncation
// grades it as it would any unparseable answer.
const maxProbeBodyBytes = 64 * 1024

// Leader reports whether this replica should be probing. LeaderElector
// satisfies it; without Redis the elector is always leader, so a
// single-replica gateway probes exactly as before.
//
// This is the one hook a sharded assignment would replace: "leader probes
// everything" is the one-shard case of "each live replica probes its share
// of the backends and publishes", which the stream already supports.
type Leader interface {
	IsLeader() bool
}

// ProbeSink receives the results this replica produced.
type ProbeSink interface {
	Publish(ctx context.Context, result ProbeResult) error
}

// ProbeSource delivers results other replicas produced. Run blocks until ctx
// is done, calling apply for each result in order; it returns the error that
// ended it, and the executor restarts it.
type ProbeSource interface {
	Run(ctx context.Context, apply func(ProbeResult)) error
}

// ResultRecorder is the metrics hook the executor reports through:
// RecordHealthCheckResult counts applied results by source (this replica's
// probes and the ones streamed from other replicas alike), RecordProbeRelay
// counts only the relays this replica actually sent, into the same series as
// client relay attempts under request_type="probe", and
// RecordHealthCheckSkipped counts the checks traffic-informed probing decided
// not to send at all, and RecordHealthCheckCycle times the whole cycle so the
// achieved cadence is visible next to the configured one. metrics.Recorder
// satisfies it; nil disables them all.
type ResultRecorder interface {
	RecordHealthCheckResult(serviceID domain.ServiceID, source string)
	RecordProbeRelay(
		serviceID domain.ServiceID,
		endpoint domain.EndpointAddr,
		statusCode int,
		latency time.Duration,
		err error,
	)
	RecordHealthCheckSkipped(serviceID domain.ServiceID)
	// RecordHealthCheckCycle records how long one cycle took and whether it
	// overran the tick it was scheduled on.
	RecordHealthCheckCycle(elapsed, tick time.Duration)
	// RecordHealthCheckCycleProbes publishes how many probes the completed
	// cycle issued per service.
	RecordHealthCheckCycleProbes(perService map[domain.ServiceID]int)
}
