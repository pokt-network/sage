package drain

import (
	"context"
	"time"

	"github.com/pokt-network/sage/domain"
)

// Key identifies one drain: a service, an operator, and optionally one RPC
// type. Operator is the operator's registrable domain (eTLD+1), lowercased —
// callers on the hot path pass an already-lowercased value; Set and Release
// lowercase defensively so a caller that forgets still gets correct behavior.
// RPCType "" means the drain covers every RPC type for that operator.
//
// JSON tags use the admin API's vocabulary — "domain" for Operator — since
// Key is embedded in Entry and Entry is what the admin drain routes marshal
// directly (see router/admin_drain.go); an untagged embed would otherwise
// serialize as PascalCase inside an endpoint whose other fields are
// snake_case.
type Key struct {
	ServiceID domain.ServiceID `json:"service_id"`
	Operator  string           `json:"domain"`
	RPCType   domain.RPCType   `json:"rpc_type,omitempty"`
}

// Entry is one drain: a Key plus when it stops applying and why it was set.
// Reason is operator-supplied and may be empty.
type Entry struct {
	Key
	Until  time.Time `json:"until"`
	Reason string    `json:"reason,omitempty"`
}

// Store holds operator drains. Set and Release are admin-path writes;
// Drained is the hot-path read consulted once per candidate endpoint per
// selection, so implementations must keep it allocation-free.
type Store interface {
	// Set installs or refreshes a drain. Until is absolute, not a duration —
	// the caller has already applied whatever ceiling applies. A Until that
	// is not after time.Now() is a release rather than a drain: it removes
	// any existing entry at Key instead of installing an already-expired one.
	Set(ctx context.Context, e Entry) error

	// Release removes any drain at Key. Releasing a key with no drain is a
	// no-op that returns nil, not an error.
	Release(ctx context.Context, k Key) error

	// Active lists the live drains for a service, sorted by Operator then
	// RPCType. Expired entries are never returned. Nil when none are live.
	Active(ctx context.Context, serviceID domain.ServiceID) []Entry

	// Drained is the hot-path check: does any live drain cover this
	// (serviceID, operator, rpcType)? True when a scoped entry matches
	// exactly, or an unscoped entry (RPCType "") matches the operator
	// regardless of rpcType. Read-only and allocation-free — called once per
	// candidate endpoint per selection.
	Drained(serviceID domain.ServiceID, operator string, rpcType domain.RPCType) bool
}
