package reputation

import "context"

// Storage defines the persistence layer for reputation scores.
type Storage interface {
	// GetScore retrieves the score for the given key.
	// Returns 0 and an error if the key does not exist.
	GetScore(ctx context.Context, key string) (float64, error)

	// SetScore stores the score for the given key.
	SetScore(ctx context.Context, key string, score float64) error

	// GetScores retrieves all scores whose keys begin with the given prefix.
	GetScores(ctx context.Context, prefix string) (map[string]float64, error)

	// DeleteScore removes the score for the given key.
	DeleteScore(ctx context.Context, key string) error
}
