package reputation

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStorage_CRUD(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryStorage()

	// Get non-existent key.
	_, err := m.GetState(ctx, "missing")
	if !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("expected ErrStateNotFound, got %v", err)
	}

	// Set and get.
	if err := m.SetState(ctx, "eth:ep1", State{Score: 85.5}); err != nil {
		t.Fatal(err)
	}
	st, err := m.GetState(ctx, "eth:ep1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Score != 85.5 {
		t.Errorf("score = %f, want 85.5", st.Score)
	}

	// Overwrite.
	if err := m.SetState(ctx, "eth:ep1", State{Score: 70}); err != nil {
		t.Fatal(err)
	}
	st, _ = m.GetState(ctx, "eth:ep1")
	if st.Score != 70 {
		t.Errorf("score = %f, want 70", st.Score)
	}

	// Delete.
	if err := m.DeleteState(ctx, "eth:ep1"); err != nil {
		t.Fatal(err)
	}
	_, err = m.GetState(ctx, "eth:ep1")
	if !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("expected ErrStateNotFound after delete, got %v", err)
	}
}

func TestMemoryStorage_GetScores(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryStorage()

	_ = m.SetState(ctx, "eth:ep1", State{Score: 90})
	_ = m.SetState(ctx, "eth:ep2", State{Score: 70})
	_ = m.SetState(ctx, "poly:ep1", State{Score: 80})

	states, err := m.GetStates(ctx, "eth:")
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 {
		t.Fatalf("expected 2 states, got %d", len(states))
	}
	if states["eth:ep1"].Score != 90 || states["eth:ep2"].Score != 70 {
		t.Errorf("unexpected states: %v", states)
	}

	// Empty prefix returns all.
	all, err := m.GetStates(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 states for empty prefix, got %d", len(all))
	}
}

func TestMemoryStorage_StateRoundTrip(t *testing.T) {
	m := NewMemoryStorage()
	ctx := context.Background()
	_, err := m.GetState(ctx, "svc:k")
	assert.ErrorIs(t, err, ErrStateNotFound)
	require.NoError(t, m.SetState(ctx, "svc:k", State{Score: 42, Rate: 0.5, Attempts: 3}))
	st, err := m.GetState(ctx, "svc:k")
	require.NoError(t, err)
	assert.Equal(t, State{Score: 42, Rate: 0.5, Attempts: 3}, st)
	all, err := m.GetStates(ctx, "svc:")
	require.NoError(t, err)
	assert.Len(t, all, 1)
	require.NoError(t, m.DeleteState(ctx, "svc:k"))
	_, err = m.GetState(ctx, "svc:k")
	assert.ErrorIs(t, err, ErrStateNotFound)
}
