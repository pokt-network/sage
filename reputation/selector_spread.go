package reputation

import (
	"math/rand/v2"

	"github.com/pokt-network/sage/domain"
)

// pickWeightedByInverseLoad selects one endpoint from the candidate list with
// probability proportional to 1/(1+load[ep]). Endpoints with no entry in the
// load map are treated as load=0 and receive the maximum weight of 1.
//
// Behaviour:
//   - Empty candidates → returns "".
//   - Empty or nil load map → uniform random over candidates.
//   - All weights identical → uniform random (equivalent to nil load).
//   - Single candidate → returned deterministically.
func pickWeightedByInverseLoad(candidates domain.EndpointAddrList, load map[domain.EndpointAddr]int) domain.EndpointAddr {
	if len(candidates) == 0 {
		return ""
	}
	if len(candidates) == 1 {
		return candidates[0]
	}
	if len(load) == 0 {
		return candidates[rand.IntN(len(candidates))]
	}

	weights := make([]float64, len(candidates))
	var total float64
	for i, ep := range candidates {
		w := 1.0 / float64(1+load[ep])
		weights[i] = w
		total += w
	}
	if total <= 0 {
		// Shouldn't happen (all weights ≥ some positive value), but guard anyway.
		return candidates[rand.IntN(len(candidates))]
	}
	r := rand.Float64() * total
	var cum float64
	for i, w := range weights {
		cum += w
		if r < cum {
			return candidates[i]
		}
	}
	// Floating-point rounding safety net.
	return candidates[len(candidates)-1]
}
