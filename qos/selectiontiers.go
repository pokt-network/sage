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

// Archival filter outcomes and kinds, the labels ArchivalFilterReporter
// records.
const (
	// ArchivalFiltered: the filter kept the request off at least one host.
	ArchivalFiltered = "filtered"
	// ArchivalBypassed: the filter would have left the request one party,
	// so it was skipped.
	ArchivalBypassed = "bypassed"
	// ArchivalNoop: the filter applied and kept every host.
	ArchivalNoop = "noop"
	// ArchivalState and ArchivalLogs are the retention the filter read:
	// state at a block, or eth_getLogs history.
	ArchivalState = "state"
	ArchivalLogs  = "logs"
)

// ArchivalFilterReporter is implemented by plugins that keep historical
// requests off hosts that refused the history they ask for. Wire sets the
// recorder once per service, before traffic: one call per selection that
// needed the history, with the kind, the outcome and how many parties the
// filter took out of the pool (0 unless filtered).
type ArchivalFilterReporter interface {
	SetArchivalFilterRecorder(fn func(kind, outcome string, removedParties int))
}
