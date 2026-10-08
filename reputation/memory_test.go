package reputation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStorage_StateRoundTrip(t *testing.T) {
	m := NewMemoryStorage()
	ctx := context.Background()
	require.NoError(t, m.SetState(ctx, "eth:ep1", State{Score: 85.5}))
	require.NoError(t, m.SetState(ctx, "eth:ep1", State{Score: 42, Rate: 0.5, Attempts: 3}))
	require.NoError(t, m.SetState(ctx, "poly:ep1", State{Score: 80}))
	all, err := m.GetStates(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]State{
		"eth:ep1":  {Score: 42, Rate: 0.5, Attempts: 3},
		"poly:ep1": {Score: 80},
	}, all)
}
