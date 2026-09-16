package qos

import "testing"

func TestSelectionTiers(t *testing.T) {
	var s SelectionTiers
	s.ReportTier(1) // no recorder: nothing to call, and no panic

	var got []int
	var reporter SelectionTierReporter = &s
	reporter.SetSelectionTierRecorder(func(tier int) { got = append(got, tier) })
	s.ReportTier(3)
	if len(got) != 1 || got[0] != 3 {
		t.Errorf("recorded %v, want [3]", got)
	}
}
