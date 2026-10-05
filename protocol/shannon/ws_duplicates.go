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
// signal behind featureflag.FlagWSDuplicatePenalty. A merged mempool feed
// repeats some; on mainnet bsc (2026-10-05) two operators repeated 9% of their
// pending hashes and the others 0-0.1%.
const (
	dupPeriod          = time.Minute
	dupMinNotes        = 500
	dupMaxShare        = 0.01
	reasonWSDuplicates = "ws_duplicate_notifications"
)

// wsDuplicates counts one supplier's notifications and repeats on one
// connection. Bridge loop only.
type wsDuplicates struct {
	drop, penalize func() bool
	record         func()
	since          time.Time
	notes, dups    int
}

// duplicates builds the check for one supplier on serviceID.
func (r *WSRelayer) duplicates(serviceID domain.ServiceID, addr domain.EndpointAddr) *wsDuplicates {
	enabled := func(flag string) func() bool {
		return func() bool { return r.deps.Flags.IsEnabled(context.Background(), flag, serviceID) }
	}
	return &wsDuplicates{
		drop:     enabled(featureflag.FlagWSDropDuplicates),
		penalize: enabled(featureflag.FlagWSDuplicatePenalty),
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
		if d.notes >= dupMinNotes && float64(d.dups) > dupMaxShare*float64(d.notes) && d.penalize() {
			d.record()
		}
		d.since, d.notes, d.dups = now, 0, 0
	}
	return dup && d.drop()
}
