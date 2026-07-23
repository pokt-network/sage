package reputation

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStorage_CRUD(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryStorage()

	// Get non-existent key.
	_, err := m.GetScore(ctx, "missing")
	if !errors.Is(err, ErrScoreNotFound) {
		t.Fatalf("expected ErrScoreNotFound, got %v", err)
	}

	// Set and get.
	if err := m.SetScore(ctx, "eth:ep1", 85.5); err != nil {
		t.Fatal(err)
	}
	score, err := m.GetScore(ctx, "eth:ep1")
	if err != nil {
		t.Fatal(err)
	}
	if score != 85.5 {
		t.Errorf("score = %f, want 85.5", score)
	}

	// Overwrite.
	if err := m.SetScore(ctx, "eth:ep1", 70); err != nil {
		t.Fatal(err)
	}
	score, _ = m.GetScore(ctx, "eth:ep1")
	if score != 70 {
		t.Errorf("score = %f, want 70", score)
	}

	// Delete.
	if err := m.DeleteScore(ctx, "eth:ep1"); err != nil {
		t.Fatal(err)
	}
	_, err = m.GetScore(ctx, "eth:ep1")
	if !errors.Is(err, ErrScoreNotFound) {
		t.Fatalf("expected ErrScoreNotFound after delete, got %v", err)
	}
}

func TestMemoryStorage_GetScores(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryStorage()

	_ = m.SetScore(ctx, "eth:ep1", 90)
	_ = m.SetScore(ctx, "eth:ep2", 70)
	_ = m.SetScore(ctx, "poly:ep1", 80)

	scores, err := m.GetScores(ctx, "eth:")
	if err != nil {
		t.Fatal(err)
	}
	if len(scores) != 2 {
		t.Fatalf("expected 2 scores, got %d", len(scores))
	}
	if scores["eth:ep1"] != 90 || scores["eth:ep2"] != 70 {
		t.Errorf("unexpected scores: %v", scores)
	}

	// Empty prefix returns all.
	all, err := m.GetScores(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 scores for empty prefix, got %d", len(all))
	}
}
