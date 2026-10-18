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
// client behind featureflag.FlagWSDropDuplicates. A supplier repeating more
// than the max share of what it pushes in a minute on a connection is charged
// a minor signal, a major one above the major share, behind
// featureflag.FlagWSDuplicatePenalty; the limits are tuning knobs
// (WSRelayerDeps.DuplicateLimits). Every minute's counts also go to the
// party's repeat share in reputation (featureflag.FlagWSDuplicateShare), which
// is what ranks a repeating party down: its keys are one URL per node, so a
// per-key signal spreads thin. On mainnet bsc (2026-10-05) three parties
// repeated 25-28% of their pending hashes and the others 0-0.9%.
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
	// DuplicateMajorShare is the repeat share above which the minute is
	// charged major rather than minor, feeding the failure rate.
	DuplicateMajorShare = 0.10
	// DuplicateMinNotifications is how many notifications that minute needs
	// before its share is judged.
	DuplicateMinNotifications = 500
)

// DuplicateLimits is how a supplier's repeats in a minute are charged.
type DuplicateLimits struct {
	MaxShare, MajorShare float64
	MinNotifications     int
}

// defaultDuplicateLimits is DuplicateLimits from the defaults above.
var defaultDuplicateLimits = DuplicateLimits{MaxShare: DuplicateMaxShare, MajorShare: DuplicateMajorShare, MinNotifications: DuplicateMinNotifications}

// notificationRecorder is the reputation service's repeat-share evidence
// (reputation.serviceImpl.RecordNotifications).
type notificationRecorder interface {
	RecordNotifications(serviceID domain.ServiceID, party string, notes, dups int)
}

// wsDuplicates counts one supplier's notifications and repeats on one
// connection. Bridge loop only.
type wsDuplicates struct {
	drop, penalize func() bool
	record         func(reputation.SignalType)
	// report hands each minute's counts to the party's repeat share.
	report func(notes, dups int)
	// limits is read when a minute closes.
	limits      func() DuplicateLimits
	since       time.Time
	notes, dups int
}

// duplicates builds the check for one supplier on serviceID.
func (r *WSRelayer) duplicates(serviceID domain.ServiceID, addr domain.EndpointAddr) *wsDuplicates {
	enabled := func(flag string) func() bool {
		return func() bool { return r.deps.Flags.IsEnabled(context.Background(), flag, serviceID) }
	}
	limits := func() DuplicateLimits { return defaultDuplicateLimits }
	if r.deps.DuplicateLimits != nil {
		limits = func() DuplicateLimits { return r.deps.DuplicateLimits(serviceID) }
	}
	report := func(int, int) {}
	if rec, ok := r.deps.Reputation.(notificationRecorder); ok {
		party := addr.Party()
		report = func(notes, dups int) { rec.RecordNotifications(serviceID, party, notes, dups) }
	}
	return &wsDuplicates{
		drop:     enabled(featureflag.FlagWSDropDuplicates),
		penalize: enabled(featureflag.FlagWSDuplicatePenalty),
		limits:   limits,
		report:   report,
		record: func(severity reputation.SignalType) {
			_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, addr, domain.RPCTypeWebSocket,
				reputation.NewSignal(severity, reasonWSDuplicates, 0))
		},
	}
}

// observe counts one notification the supplier pushed, charging and reporting
// the minute that just ended, and reports whether to keep it from the client.
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
		d.report(d.notes, d.dups)
		l := d.limits()
		share := float64(d.dups) / float64(d.notes)
		if d.notes >= l.MinNotifications && share > l.MaxShare && d.penalize() {
			severity := reputation.SignalMinorError
			if share > l.MajorShare {
				severity = reputation.SignalMajorError
			}
			d.record(severity)
		}
		d.since, d.notes, d.dups = now, 0, 0
	}
	return dup && d.drop()
}

// duplicateShares is the reputation service's priced repeat shares
// (reputation.serviceImpl.PartyDuplicateShares).
type duplicateShares interface {
	PartyDuplicateShares() []reputation.PartyShare
}

// evictedParties is the parties ws_duplicate_evict keeps off WebSocket on
// serviceID: the ones its repeat share charges the whole penalty. Nil with
// the flag off.
func (r *WSRelayer) evictedParties(serviceID domain.ServiceID) map[string]bool {
	shares, ok := r.deps.Reputation.(duplicateShares)
	if !ok || !r.deps.Flags.IsEnabled(context.Background(), featureflag.FlagWSDuplicateEvict, serviceID) {
		return nil
	}
	var out map[string]bool
	for _, s := range shares.PartyDuplicateShares() {
		if s.ServiceID == serviceID && s.Penalty <= reputation.DuplicateSharePenalty {
			if out == nil {
				out = map[string]bool{}
			}
			out[s.Party] = true
		}
	}
	return out
}

// withoutEvicted drops the endpoints of evicted parties, unless that would
// leave none: a session holding nothing else is served by them.
func withoutEvicted(eps domain.EndpointAddrList, evicted map[string]bool) domain.EndpointAddrList {
	if len(evicted) == 0 {
		return eps
	}
	kept := make(domain.EndpointAddrList, 0, len(eps))
	for _, ep := range eps {
		if !evicted[ep.Party()] {
			kept = append(kept, ep)
		}
	}
	if len(kept) == 0 {
		return eps
	}
	return kept
}

// evictedWithElsewhere reports whether ep's party is evicted on serviceID and
// the session's WebSocket endpoints hold another party to move to: with none,
// a move would only land on the same party again.
func (r *WSRelayer) evictedWithElsewhere(serviceID domain.ServiceID, ep domain.EndpointAddr) bool {
	evicted := r.evictedParties(serviceID)
	if !evicted[ep.Party()] || r.deps.Protocol == nil {
		return false
	}
	eps, err := r.deps.Protocol.AvailableEndpoints(context.Background(), serviceID, domain.RPCTypeWebSocket)
	if err != nil {
		return false
	}
	for _, other := range eps {
		if !evicted[other.Party()] {
			return true
		}
	}
	return false
}
