package reputation

import (
	"testing"
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
