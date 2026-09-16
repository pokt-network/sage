package qos

// SelectionTierReporter is implemented by plugins whose SelectEndpoints
// degrades through the height tiers of SelectWithKnownHeights. Wire sets the
// recorder once per service, before traffic, so the tier each selection
// settled on can be counted per service.
//
// It exists because the tier was only ever a WARN log line, and production
// runs at log level error: a change to height filtering had no before and
// after anyone could read.
type SelectionTierReporter interface {
	SetSelectionTierRecorder(fn func(tier int))
}

// SelectionTiers implements SelectionTierReporter for a plugin that embeds
// it; the plugin calls ReportTier with every SelectResult.Tier.
type SelectionTiers struct {
	record func(tier int)
}

// SetSelectionTierRecorder installs the recorder. Wire time only: it is read
// without a lock on every selection.
func (s *SelectionTiers) SetSelectionTierRecorder(fn func(tier int)) { s.record = fn }

// ReportTier passes one selection's tier to the recorder, if one is set.
func (s *SelectionTiers) ReportTier(tier int) {
	if s.record != nil {
		s.record(tier)
	}
}
