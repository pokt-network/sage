package reputation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisStorage_DecodeLegacyFloat(t *testing.T) {
	st, err := decodeState("87.5")
	require.NoError(t, err)
	assert.Equal(t, State{Score: 87.5}, st)
}

func TestRedisStorage_EncodeDecodeState(t *testing.T) {
	in := State{Score: 60, Rate: 0.0031, Attempts: 12000, TrafficAttempts: 11800, LatencyMS: 140}
	out, err := decodeState(encodeState(in))
	require.NoError(t, err)
	in.LatencyMS = 0 // not persisted
	assert.Equal(t, in, out)
}

func TestRedisStorage_DecodeGarbage(t *testing.T) {
	_, err := decodeState("not a state")
	assert.Error(t, err)
}
