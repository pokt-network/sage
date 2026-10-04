package healthcheck

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestLeaderElector_LocalOnlyMode_AlwaysLeader(t *testing.T) {
	le := NewLeaderElector(nil, slog.Default(), "")
	le.Start(context.Background())

	if !le.IsLeader() {
		t.Error("expected IsLeader=true in local-only mode")
	}

	if err := le.Stop(); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
}

func TestLeaderElector_Stop_CancelsContext(t *testing.T) {
	le := NewLeaderElector(nil, slog.Default(), "")
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
	le := NewLeaderElector(nil, slog.Default(), "")
	// Before Start: not yet set.
	if le.IsLeader() {
		t.Error("expected IsLeader=false before Start")
	}
}

// A configured Redis that does not answer leaves no replica without a leader:
// each probes for itself until an election can be held again.
func TestLeaderElector_RedisDown_LeadsItself(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens: every command is refused

	client := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	defer client.Close()
	le := NewLeaderElector(client, slog.Default(), "")
	le.tryAcquire(context.Background())
	if !le.IsLeader() || !le.fallback.Load() {
		t.Fatalf("leader %v fallback %v with Redis down; want this instance leading itself", le.IsLeader(), le.fallback.Load())
	}
	le.tryAcquire(context.Background()) // still down: still its own leader
	if !le.IsLeader() {
		t.Fatal("leadership lapsed on the second failed attempt")
	}
	if err := le.Stop(); err != nil {
		t.Errorf("Stop tried to release a lock it never held: %v", err)
	}
}
