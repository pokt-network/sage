package drain

import (
	"context"
	"sort"

	"github.com/pokt-network/sage/domain"
)

// Merge is local's drains plus peer's, for an instance that honours another
// instance's drains — today canary-sage following mainnet's auto drains
// (docs/auto-drain.md §10). An endpoint either drains is drained. Set and
// Release act on local only: a peer's drain is released where it was set.
func Merge(local, peer Store) Store { return merged{Store: local, peer: peer} }

type merged struct {
	Store
	peer Store
}

// Drained reports a drain in either store.
func (m merged) Drained(serviceID domain.ServiceID, operator string, rpcType domain.RPCType) bool {
	return m.Store.Drained(serviceID, operator, rpcType) || m.peer.Drained(serviceID, operator, rpcType)
}

// Active lists both stores' live drains, sorted by Operator then RPCType.
func (m merged) Active(ctx context.Context, serviceID domain.ServiceID) []Entry {
	out := append(m.Store.Active(ctx, serviceID), m.peer.Active(ctx, serviceID)...)
	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Operator != out[j].Operator {
			return out[i].Operator < out[j].Operator
		}
		return out[i].RPCType < out[j].RPCType
	})
	return out
}
