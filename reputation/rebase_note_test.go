package reputation

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/domain"
)

// A drain's end restarts its keys at the bottom of probation: a key the drain
// froze at 100 does not walk straight back into tier 1. A key already lower
// is left where it is.
func TestService_RebaseAfterDrainRestartsAtProbation(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	top := domain.EndpointAddr("pokt1a-https://a.opa.example")
	low := domain.EndpointAddr("pokt1b-https://b.opa.example")
	require.NoError(t, svc.RecordSignal(ctx, "sei", top, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0)))
	for i := 0; i < 4; i++ {
		require.NoError(t, svc.RecordSignal(ctx, "sei", low, domain.RPCTypeJSONRPC, NewCriticalErrorSignal("bad", 0)))
	}

	n := svc.RebaseAfterDrain("sei", domain.EndpointAddrList{top, low}, domain.RPCTypeJSONRPC)
	assert.Equal(t, 1, n, "only the key above the floor is lowered")
	got, _ := svc.GetScore(ctx, "sei", top, domain.RPCTypeJSONRPC)
	assert.Equal(t, svc.selector.cfg.Load().MinThreshold, got)
	still, _ := svc.GetScore(ctx, "sei", low, domain.RPCTypeJSONRPC)
	assert.Equal(t, 0.0, still)
}

// An unscored attempt leaves a timeline event naming the host's answer, and
// no score change.
func TestService_RecordNoteIsTimelineOnly(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	ep := domain.EndpointAddr("pokt1a-https://a.opa.example")
	require.NoError(t, svc.RecordSignal(ctx, "sei", ep, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0)))
	before, _ := svc.GetScore(ctx, "sei", ep, domain.RPCTypeJSONRPC)

	svc.RecordNote("sei", ep, domain.RPCTypeJSONRPC, "server_error", "server error (code -32000): some node-specific failure")

	after, _ := svc.GetScore(ctx, "sei", ep, domain.RPCTypeJSONRPC)
	assert.Equal(t, before, after)
	events := svc.timeline.Get(scoreKey("sei", svc.keyOf(ep, domain.RPCTypeJSONRPC)))
	found := false
	for _, e := range events {
		if e.Event == "unscored" && strings.Contains(e.Detail, "some node-specific failure") {
			found = true
		}
	}
	assert.True(t, found, "no unscored event in %+v", events)
}
