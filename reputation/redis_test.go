package reputation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScoreField(t *testing.T) {
	tests := []struct {
		key      string
		expected string
	}{
		{"eth:supplier1-https://example.com", "eth:supplier1-https://example.com"},
		{"poly:ep2", "poly:ep2"},
		{"", ""},
	}
	for _, tt := range tests {
		got := ScoreField(tt.key)
		if got != tt.expected {
			t.Errorf("ScoreField(%q) = %q, want %q", tt.key, got, tt.expected)
		}
	}
}

func TestNewRedisStorage_NilClient(t *testing.T) {
	_, err := NewRedisStorage(nil, "test:hash")
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

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
