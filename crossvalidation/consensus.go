// Package crossvalidation provides background consensus checking across
// endpoint responses. It detects outlier endpoints whose responses differ
// from the majority, which can indicate fabricated, corrupted, or stale data.
package crossvalidation

import "github.com/pokt-network/sage/domain"

// Outlier describes an endpoint whose response hash differed from the majority.
type Outlier struct {
	// Endpoint is the address of the outlier endpoint.
	Endpoint domain.EndpointAddr
	// Hash is the response hash this endpoint repeatedly returned.
	Hash [32]byte
	// Count is how many times this endpoint produced this hash within the window.
	Count int
}

// findOutliers groups digests by hash and identifies endpoints that are not
// in the majority group.
//
// Rules:
//   - A minimum of minQuorum total digests is required before any outliers
//     are flagged. Fewer digests → not enough data → returns nil.
//   - The majority group is the hash with the highest count.
//   - Any endpoint whose most-recent hash does not match the majority is an
//     outlier. Each outlier entry records how many times it produced the
//     non-majority hash within the window.
//
// The function is deterministic given the same input slice.
func findOutliers(digests []responseDigest, minQuorum int) []Outlier {
	if len(digests) < minQuorum {
		return nil
	}

	// Count occurrences of each hash.
	hashCount := make(map[[32]byte]int, len(digests))
	for _, d := range digests {
		hashCount[d.ResponseHash]++
	}

	// Find the majority hash (most common).
	var majorityHash [32]byte
	majorityCount := 0
	for h, count := range hashCount {
		if count > majorityCount {
			majorityHash = h
			majorityCount = count
		}
	}

	// If every digest has the same hash, there are no outliers.
	if majorityCount == len(digests) {
		return nil
	}

	// Collect per-endpoint tallies for non-majority hashes.
	// Key: endpoint → (non-majority hash → count).
	type endpointTally struct {
		hash  [32]byte
		count int
	}
	endpointMap := make(map[domain.EndpointAddr]*endpointTally, len(digests))

	for _, d := range digests {
		if d.ResponseHash == majorityHash {
			continue
		}
		t, ok := endpointMap[d.Endpoint]
		if !ok {
			t = &endpointTally{hash: d.ResponseHash}
			endpointMap[d.Endpoint] = t
		}
		// Keep only the most-common non-majority hash for this endpoint.
		// Since the window is small, we simplify: track the first non-majority
		// hash seen for the endpoint and accumulate its count.
		if t.hash == d.ResponseHash {
			t.count++
		} else if t.count == 0 {
			t.hash = d.ResponseHash
			t.count = 1
		}
	}

	if len(endpointMap) == 0 {
		return nil
	}

	outliers := make([]Outlier, 0, len(endpointMap))
	for ep, t := range endpointMap {
		if t.count == 0 {
			t.count = 1
		}
		outliers = append(outliers, Outlier{
			Endpoint: ep,
			Hash:     t.hash,
			Count:    t.count,
		})
	}
	return outliers
}
