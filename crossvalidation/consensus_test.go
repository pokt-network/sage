package crossvalidation

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// makeDigest is a convenience helper.
func makeDigest(endpoint domain.EndpointAddr, hash [32]byte) responseDigest {
	return responseDigest{
		Endpoint:     endpoint,
		ResponseHash: hash,
		Timestamp:    time.Now(),
	}
}

func hash(b byte) [32]byte {
	var h [32]byte
	h[0] = b
	return h
}

func TestFindOutliers_MajorityWins(t *testing.T) {
	// 3 endpoints return hash-A, 1 returns hash-B.
	digests := []responseDigest{
		makeDigest("ep1", hash(0xAA)),
		makeDigest("ep2", hash(0xAA)),
		makeDigest("ep3", hash(0xAA)),
		makeDigest("ep4", hash(0xBB)),
	}

	outliers := findOutliers(digests, 3)
	if len(outliers) != 1 {
		t.Fatalf("expected 1 outlier, got %d", len(outliers))
	}
	if outliers[0].Endpoint != "ep4" {
		t.Errorf("expected outlier ep4, got %q", outliers[0].Endpoint)
	}
}

func TestFindOutliers_AllAgree_NoOutliers(t *testing.T) {
	digests := []responseDigest{
		makeDigest("ep1", hash(0xAA)),
		makeDigest("ep2", hash(0xAA)),
		makeDigest("ep3", hash(0xAA)),
	}

	outliers := findOutliers(digests, 3)
	if len(outliers) != 0 {
		t.Errorf("expected no outliers when all agree, got %v", outliers)
	}
}

func TestFindOutliers_QuorumNotMet_ReturnsNil(t *testing.T) {
	// Only 2 digests but minQuorum is 3.
	digests := []responseDigest{
		makeDigest("ep1", hash(0xAA)),
		makeDigest("ep2", hash(0xBB)),
	}

	outliers := findOutliers(digests, 3)
	if outliers != nil {
		t.Errorf("expected nil when quorum not met, got %v", outliers)
	}
}

func TestFindOutliers_SingleEndpoint_NoOutliers(t *testing.T) {
	digests := []responseDigest{
		makeDigest("ep1", hash(0xAA)),
		makeDigest("ep1", hash(0xAA)),
		makeDigest("ep1", hash(0xAA)),
	}

	outliers := findOutliers(digests, 3)
	if len(outliers) != 0 {
		t.Errorf("single endpoint should never be an outlier, got %v", outliers)
	}
}

func TestFindOutliers_TiedCounts_StillFindsMajority(t *testing.T) {
	// ep1 and ep2 each have 2 digests but with different hashes.
	// The majority is whichever group findOutliers picks first.
	// We just verify no crash and some result.
	digests := []responseDigest{
		makeDigest("ep1", hash(0xAA)),
		makeDigest("ep1", hash(0xAA)),
		makeDigest("ep2", hash(0xBB)),
		makeDigest("ep2", hash(0xBB)),
	}

	outliers := findOutliers(digests, 3)
	// With a tie, one group will be majority and the other outlier.
	if len(outliers) == 0 {
		t.Error("expected outliers in a tie scenario (one group must lose)")
	}
}

func TestFindOutliers_MultipleOutliers(t *testing.T) {
	// ep1, ep2, ep3 agree; ep4 and ep5 disagree (different hashes from each other).
	digests := []responseDigest{
		makeDigest("ep1", hash(0xAA)),
		makeDigest("ep2", hash(0xAA)),
		makeDigest("ep3", hash(0xAA)),
		makeDigest("ep4", hash(0xBB)),
		makeDigest("ep5", hash(0xCC)),
	}

	outliers := findOutliers(digests, 3)
	if len(outliers) != 2 {
		t.Fatalf("expected 2 outliers, got %d: %v", len(outliers), outliers)
	}
}

func TestFindOutliers_EmptyDigests(t *testing.T) {
	outliers := findOutliers(nil, 3)
	if outliers != nil {
		t.Errorf("expected nil for empty digests, got %v", outliers)
	}
}

func TestFindOutliers_ExactlyAtQuorum(t *testing.T) {
	// Exactly 3 digests (minQuorum=3); 2 agree, 1 disagrees → should flag.
	digests := []responseDigest{
		makeDigest("ep1", hash(0xAA)),
		makeDigest("ep2", hash(0xAA)),
		makeDigest("ep3", hash(0xBB)),
	}

	outliers := findOutliers(digests, 3)
	if len(outliers) != 1 {
		t.Fatalf("expected 1 outlier at quorum boundary, got %d", len(outliers))
	}
	if outliers[0].Endpoint != "ep3" {
		t.Errorf("expected ep3 as outlier, got %q", outliers[0].Endpoint)
	}
}
