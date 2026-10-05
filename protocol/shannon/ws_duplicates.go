package shannon

import (
	"context"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/reputation"
)

// WebSocket duplicates: a supplier repeating a notification it already sent
// on the subscription (qos.NotificationDuplicate). The repeat is kept from the
// client behind featureflag.FlagWSDropDuplicates, and a supplier repeating
// more than dupMaxShare of what it pushes in a period is charged a minor
// signal behind featureflag.FlagWSDuplicatePenalty; both limits are tuning
// knobs (WSRelayerDeps.DuplicateLimits). A merged mempool feed
// repeats some; on mainnet bsc (2026-10-05) two operators repeated 9% of their
// pending hashes and the others 0-0.1%.
const (
	dupPeriod          = time.Minute
	reasonWSDuplicates = "ws_duplicate_notifications"
)

// The defaults for WSRelayerDeps.DuplicateLimits, and the bases of the
// websocket.duplicate_* tuning knobs.
const (
	// DuplicateMaxShare is the repeat share a supplier may push on a
	// connection in a minute before it is charged.
	DuplicateMaxShare = 0.01
	// DuplicateMinNotifications is how many notifications that minute needs
	// before its share is judged.
	DuplicateMinNotifications = 500
)

// wsDuplicates counts one supplier's notifications and repeats on one
// connection. Bridge loop only.
type wsDuplicates struct {
	drop, penalize func() bool
	record         func()
	// limits is the repeat share charged above and the notifications a
	// minute needs, read when a minute closes.
	limits      func() (maxShare float64, minNotes int)
	since       time.Time
	notes, dups int
}

// duplicates builds the check for one supplier on serviceID.
func (r *WSRelayer) duplicates(serviceID domain.ServiceID, addr domain.EndpointAddr) *wsDuplicates {
	enabled := func(flag string) func() bool {
		return func() bool { return r.deps.Flags.IsEnabled(context.Background(), flag, serviceID) }
	}
	limits := func() (float64, int) { return DuplicateMaxShare, DuplicateMinNotifications }
	if r.deps.DuplicateLimits != nil {
		limits = func() (float64, int) { return r.deps.DuplicateLimits(serviceID) }
	}
	return &wsDuplicates{
		drop:     enabled(featureflag.FlagWSDropDuplicates),
		penalize: enabled(featureflag.FlagWSDuplicatePenalty),
		limits:   limits,
		record: func() {
			_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, addr, domain.RPCTypeWebSocket,
				reputation.NewSignal(reputation.SignalMinorError, reasonWSDuplicates, 0))
		},
	}
}

// observe counts one notification the supplier pushed, charging the period
// that just ended, and reports whether to keep it from the client.
func (d *wsDuplicates) observe(dup bool, now time.Time) bool {
	if d == nil {
		return false
	}
	if d.since.IsZero() {
		d.since = now
	}
	d.notes++
	if dup {
		d.dups++
	}
	if now.Sub(d.since) >= dupPeriod {
		maxShare, minNotes := d.limits()
		if d.notes >= minNotes && float64(d.dups) > maxShare*float64(d.notes) && d.penalize() {
			d.record()
		}
		d.since, d.notes, d.dups = now, 0, 0
	}
	return dup && d.drop()
}
