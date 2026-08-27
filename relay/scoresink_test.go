package relay

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/reputation"
)

func TestScoreSink_WorstOfPerEndpoint(t *testing.T) {
	s := NewScoreSink()
	a := domain.EndpointAddr("pokt1a-https://a")
	b := domain.EndpointAddr("pokt1b-https://b")
	s.Add(a, domain.RPCTypeJSONRPC, reputation.NewSuccessSignal("ok", 10*time.Millisecond))
	s.Add(a, domain.RPCTypeJSONRPC, reputation.NewFatalErrorSignal("fabricated", 30*time.Millisecond))
	s.Add(a, domain.RPCTypeJSONRPC, reputation.NewSuccessSignal("ok", 50*time.Millisecond))
	s.Add(b, domain.RPCTypeJSONRPC, reputation.NewSuccessSignal("ok", 20*time.Millisecond))
	s.Add(b, domain.RPCTypeREST, reputation.NewMinorErrorSignal("meh", 5*time.Millisecond))

	got := map[string]reputation.Signal{}
	s.Flush(func(ep domain.EndpointAddr, rpc domain.RPCType, sig reputation.Signal) {
		got[string(ep)+"|"+string(rpc)] = sig
	})
	assert.Len(t, got, 3)
	assert.Equal(t, reputation.SignalFatalError, got["pokt1a-https://a|json_rpc"].Type)
	assert.Equal(t, "fabricated", got["pokt1a-https://a|json_rpc"].Reason)
	assert.Equal(t, 50*time.Millisecond, got["pokt1a-https://a|json_rpc"].Latency, "latency is the max over the endpoint's items")
	assert.Equal(t, reputation.SignalSuccess, got["pokt1b-https://b|json_rpc"].Type)
	assert.Equal(t, reputation.SignalMinorError, got["pokt1b-https://b|rest"].Type)
}

func TestScoreSink_SeverityOrder(t *testing.T) {
	s := NewScoreSink()
	ep := domain.EndpointAddr("pokt1a-https://a")
	s.Add(ep, domain.RPCTypeJSONRPC, reputation.NewCriticalErrorSignal("c", 0))
	s.Add(ep, domain.RPCTypeJSONRPC, reputation.NewMajorErrorSignal("m", 0))
	var got reputation.Signal
	s.Flush(func(_ domain.EndpointAddr, _ domain.RPCType, sig reputation.Signal) { got = sig })
	assert.Equal(t, reputation.SignalCriticalError, got.Type, "major must not overwrite critical")
}

func TestScoreSink_FlushEmpty(t *testing.T) {
	calls := 0
	NewScoreSink().Flush(func(domain.EndpointAddr, domain.RPCType, reputation.Signal) { calls++ })
	assert.Equal(t, 0, calls)
}

// A hedge's losing arm runs on a context detached with context.WithoutCancel,
// so inside a batch sub-relay it can still hold the sink after the batch
// flushed. A signal it adds then must be scored, not collapsed into a map
// nothing reads.
func TestScoreSink_AddAfterFlushForwards(t *testing.T) {
	s := NewScoreSink()
	s.Add(domain.EndpointAddr("pokt1a-https://a"), domain.RPCTypeJSONRPC, reputation.NewSuccessSignal("ok", 0))

	var recorded []recordedSignal
	record := func(ep domain.EndpointAddr, rpc domain.RPCType, sig reputation.Signal) {
		recorded = append(recorded, recordedSignal{ep, rpc, sig})
	}
	s.Flush(record)
	require.Len(t, recorded, 1, "the flush itself emits the collapsed signal")

	late := domain.EndpointAddr("pokt1b-https://b")
	s.Add(late, domain.RPCTypeJSONRPC, reputation.NewCriticalErrorSignal("late arm", 7*time.Millisecond))

	require.Len(t, recorded, 2, "an Add after Flush is forwarded, not stored")
	assert.Equal(t, late, recorded[1].ep)
	assert.Equal(t, domain.RPCTypeJSONRPC, recorded[1].rpc)
	assert.Equal(t, reputation.SignalCriticalError, recorded[1].sig.Type)
	assert.Equal(t, "late arm", recorded[1].sig.Reason)
	assert.Equal(t, 7*time.Millisecond, recorded[1].sig.Latency)

	// The forwarded signal was not also retained: a second flush has nothing.
	before := len(recorded)
	s.Flush(record)
	assert.Len(t, recorded, before, "a forwarded signal must not be double-counted by a later Flush")
}

// recordedSignal is one call the flush function saw.
type recordedSignal struct {
	ep  domain.EndpointAddr
	rpc domain.RPCType
	sig reputation.Signal
}
