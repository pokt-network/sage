package healthcheck

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// RPCTypeCoverageGaps reports, per service, the RPC types the config declares
// that no health check probes. A type with no check is graded by client
// traffic alone — nothing is wrong with that in itself, but until 2026-09-04
// nothing said so, and an operator reading the probe counts for a service
// took them to cover every type it served. TRON's REST surface, about a
// quarter of its traffic, was one such.
//
// websocket and grpc are named too, though they cannot be probed by a
// one-shot relay: the line says which is which.
func RPCTypeCoverageGaps(services []config.ServiceConfig, reg *qos.Registry, configured *ConfiguredChecks) []string {
	var out []string
	for _, svc := range services {
		id := domain.ServiceID(svc.ID)
		probed := map[domain.RPCType]bool{}
		for _, c := range pluginChecks(reg.Get(id)) {
			probed[c.Payload.RPCType()] = true
		}
		for _, c := range configured.For(id) {
			probed[c.Payload.RPCType()] = true
		}
		if len(probed) == 0 {
			// Nothing probes this service at all. That is the QoS-coverage
			// report's line ("no chain-specific QoS"), not a per-type gap.
			continue
		}
		var missing, unprobeable []string
		for _, rt := range svc.RPCTypes {
			t := domain.RPCType(rt)
			if probed[t] {
				continue
			}
			switch t {
			case domain.RPCTypeWebSocket, domain.RPCTypeGRPC:
				unprobeable = append(unprobeable, rt)
			default:
				missing = append(missing, rt)
			}
		}
		sort.Strings(missing)
		sort.Strings(unprobeable)
		if len(missing) > 0 {
			out = append(out, fmt.Sprintf("service %q declares %s with no health check for it: those suppliers are graded by client traffic alone (add a local[].checks entry of that type)",
				svc.ID, strings.Join(missing, ", ")))
		}
		if len(unprobeable) > 0 {
			out = append(out, fmt.Sprintf("service %q declares %s, which a one-shot probe cannot check: graded by client traffic alone",
				svc.ID, strings.Join(unprobeable, ", ")))
		}
	}
	sort.Strings(out)
	return out
}
