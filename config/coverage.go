package config

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pokt-network/sage/domain"
)

// QoSCoverage is what SAGE understands about each configured service, as
// sentences for the startup report.
//
// A service's `type` selects its QoS plugin, and anything the switch does not
// recognise falls through to the passthrough — which relays and scores but
// runs no health checks, observes no block heights and publishes no chain
// view. Selection on such a service is reputation from client relays and
// nothing else.
//
// That is a legitimate configuration and a serious one, and until 2026-09-04
// nothing said it was happening. The mainnet canary carried five of them, one
// of which was tron — the largest service on the sibling PATH fleet by relay
// count, running with no probes and no chain view because a field said
// "generic". Nobody had decided that; it had simply never been reported.
//
// The two cases are separated because they need different answers. A declared
// passthrough is a choice, and the line exists so somebody can confirm it is
// still the right one. An UNRECOGNISED type is almost always a typo, and it
// costs the same coverage while looking like a configured chain — the same
// shape as a config key that parses into nothing, and reported in the same
// spirit.
type QoSCoverage struct {
	// Passthrough names services whose type is a deliberate non-chain value.
	Passthrough []string
	// Unrecognised names services whose type matched no plugin and was
	// probably meant to.
	Unrecognised []string
}

// QoSCoverageFor reports which services run without chain-specific QoS.
func QoSCoverageFor(services []ServiceConfig) QoSCoverage {
	var cov QoSCoverage
	for _, svc := range services {
		t := domain.ServiceType(svc.Type)
		if domain.IsKnownServiceType(t) {
			continue
		}
		if t == domain.ServiceTypeGeneric || svc.Type == "" {
			cov.Passthrough = append(cov.Passthrough, svc.ID)
			continue
		}
		cov.Unrecognised = append(cov.Unrecognised, fmt.Sprintf("%s (type %q)", svc.ID, svc.Type))
	}
	sort.Strings(cov.Passthrough)
	sort.Strings(cov.Unrecognised)
	return cov
}

// Lines renders the coverage as operator-facing sentences, empty when every
// service has a chain-specific plugin.
//
// One line per case rather than per service: a fleet can carry dozens, and the
// startup report is read by somebody deciding whether to act, not audited.
func (c QoSCoverage) Lines() []string {
	var out []string
	if len(c.Unrecognised) > 0 {
		out = append(out, fmt.Sprintf(
			"%d service(s) name a type no QoS plugin implements and are being served by the passthrough — "+
				"no health checks, no block-height tracking, no chain view, and selection on reputation alone. "+
				"Recognised types are %s. This is what a typo in `type` looks like: %s",
			len(c.Unrecognised),
			strings.Join(knownTypeNames(), ", "),
			strings.Join(c.Unrecognised, ", "),
		))
	}
	if len(c.Passthrough) > 0 {
		out = append(out, fmt.Sprintf(
			"%d service(s) are configured for the passthrough QoS plugin — no health checks, no block-height "+
				"tracking, no chain view, and selection on reputation from client relays alone. Deliberate if "+
				"the chain has no plugin; worth revisiting for a busy service: %s",
			len(c.Passthrough),
			strings.Join(c.Passthrough, ", "),
		))
	}
	return out
}

func knownTypeNames() []string {
	known := domain.KnownServiceTypes()
	names := make([]string, 0, len(known))
	for _, t := range known {
		names = append(names, string(t))
	}
	return names
}
