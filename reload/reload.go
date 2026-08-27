// Package reload carries the vocabulary of a config reload: what a reload
// changed, what it could not change, and the one error the HTTP layer has to
// tell apart from a bad file.
//
// It exists as its own package so the router can describe a reloader without
// importing the binary that performs one — cmd/sagegw imports router, never
// the reverse.
package reload

import "errors"

// ErrNoConfigFile is returned when SAGE was started from inline YAML
// (GATEWAY_CONFIG) rather than from a path.
//
// There is nothing to re-read in that case, and the distinction matters to the
// caller: it is not a config that failed to validate, it is a gateway that has
// no file to validate. The admin route maps it to 409 rather than 400, and
// SIGHUP logs it and does nothing.
var ErrNoConfigFile = errors.New("no config file to reload from (started from GATEWAY_CONFIG)")

// Result is the honest account of one reload.
//
// Applied and NeedsRestart are key paths into the YAML, in the same vocabulary
// an operator typed: "gateway_config.defaults", "feature_flags",
// "gateway_config.services[eth].chain_id". A section appears in exactly one of
// them, and only when it actually changed — a reload that changed nothing
// reports nothing rather than reciting the whole file.
//
// NeedsRestart is the point of the whole exercise. A reload that silently
// dropped the sections it cannot apply would leave an operator believing a new
// service, a new listener or a new signing key was live.
//
// Ignored and Inert are the parse's own report on the file that was just read
// (config.Config.Ignored and .Inert), repeated here because a reload is the
// one moment an operator is looking at the result of editing it.
//
// Every field is a non-nil slice when it comes back from a reload, so the JSON
// carries `[]` rather than `null` for the empty case.
type Result struct {
	// Applied lists the sections that changed and took effect.
	Applied []string `json:"applied"`
	// NeedsRestart lists the sections that changed and did not take effect,
	// because nothing in the running process can be swapped for them.
	NeedsRestart []string `json:"needs_restart"`
	// Ignored lists keys in the file that SAGE has no field for.
	Ignored []string `json:"ignored"`
	// Inert lists keys in the file that parse into a field nothing reads.
	Inert []string `json:"inert"`
	// Warnings lists anything the apply steps had to say — a health-check rule
	// that could not be built, a flag name SAGE does not know.
	Warnings []string `json:"warnings"`
}

// NewResult returns a Result with every slice non-nil, so a caller that
// appends nothing still marshals to arrays rather than nulls.
func NewResult() Result {
	return Result{
		Applied:      []string{},
		NeedsRestart: []string{},
		Ignored:      []string{},
		Inert:        []string{},
		Warnings:     []string{},
	}
}
