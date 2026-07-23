package healthcheck

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestLeaderElector_LocalOnlyMode_AlwaysLeader(t *testing.T) {
	le := NewLeaderElector(nil, slog.Default())
	le.Start(context.Background())

	if !le.IsLeader() {
		t.Error("expected IsLeader=true in local-only mode")
	}

	if err := le.Stop(); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
}

func TestLeaderElector_Stop_CancelsContext(t *testing.T) {
	le := NewLeaderElector(nil, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	le.Start(ctx)
	cancel()
	// Should not block.
	done := make(chan struct{})
	go func() {
		_ = le.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("Stop did not return in time after context cancel")
	}
}

func TestLeaderElector_ID_IsUnique(t *testing.T) {
	id1 := instanceID()
	id2 := instanceID()
	// IDs should differ because of the random suffix.
	if id1 == id2 {
		t.Errorf("expected unique IDs, got %q twice", id1)
	}
}

func TestLeaderElector_IsLeader_Default(t *testing.T) {
	le := NewLeaderElector(nil, slog.Default())
	// Before Start: not yet set.
	if le.IsLeader() {
		t.Error("expected IsLeader=false before Start")
	}
}
