package router

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/override"
	"github.com/pokt-network/sage/reputation"
)

// ReputationResetPrefix is where a reputation reset is announced in the
// override store: "reputation_reset/<service>/<base64url target>", the value
// the time of the reset. Reputation state is per replica (the leader alone
// writes storage, and a follower's cache is its own), so a reset taken by
// one pod's admin port has to be repeated on every other pod; before
// 2026-09-14 an operator did that by hand, one port-forward per pod. Each
// replica watches the prefix and applies announcements newer than its own
// start, through the same matching the route uses, so an announcement that
// matches nothing on a replica does nothing there.
const ReputationResetPrefix = "reputation_reset/"

// resetAnnouncementTTL is how long an announcement stays in the store. A
// replica applies only what was announced after it started, so the entry
// only has to outlive the watch interval on every replica; the next publish
// prunes older ones.
const resetAnnouncementTTL = 10 * time.Minute

// resetReplayGrace is how far before its own start a replica still applies
// an announcement, covering clock skew between the pod that published and
// the one that reads. A pod that starts later than that hydrates the reset
// from storage, where the publishing pod wrote it through.
const resetReplayGrace = 30 * time.Second

func resetAnnouncementKey(serviceID domain.ServiceID, target string) string {
	return ReputationResetPrefix + string(serviceID) + "/" + base64.RawURLEncoding.EncodeToString([]byte(target))
}

func parseResetAnnouncementKey(key string) (domain.ServiceID, string, bool) {
	rest := strings.TrimPrefix(key, ReputationResetPrefix)
	svc, enc, ok := strings.Cut(rest, "/")
	if !ok || svc == "" || enc == "" {
		return "", "", false
	}
	target, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil || len(target) == 0 {
		return "", "", false
	}
	return domain.ServiceID(svc), string(target), true
}

// applyReset runs one reset against the service, preferring the form that
// names the keys it touched.
func applyReset(ctx context.Context, svc reputation.Service, serviceID domain.ServiceID, target string) ([]string, error) {
	if kr, ok := svc.(reputation.KeyResetter); ok {
		return kr.ResetMatching(ctx, serviceID, target)
	}
	return nil, svc.ResetScore(ctx, serviceID, domain.EndpointAddr(target))
}

// publishReputationReset announces a reset the local replica has already
// applied, and prunes announcements past their TTL. Reports whether the
// announcement reaches other replicas (a shared store). An error is logged,
// not returned: the local reset stands either way and the response says
// what was persisted.
func (a *AdminAPI) publishReputationReset(ctx context.Context, serviceID domain.ServiceID, target string) bool {
	if a.overrides == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	now := time.Now()
	key := resetAnnouncementKey(serviceID, target)
	value := now.UTC().Format(time.RFC3339Nano)
	if err := a.overrides.Set(ctx, key, value); err != nil {
		a.logger.Error("admin: announce reputation reset to other replicas", "service", serviceID, "target", target, "error", err)
		return false
	}
	// This replica applied the reset before announcing it; its own watcher
	// must not apply it again and wipe a signal recorded in between.
	a.resetsApplied.Store(key, value)
	if listed, err := a.overrides.List(ctx, ReputationResetPrefix); err == nil {
		for k, v := range listed {
			if k == key {
				continue
			}
			at, err := time.Parse(time.RFC3339Nano, v)
			if err == nil && now.Sub(at) < resetAnnouncementTTL {
				continue
			}
			_ = a.overrides.Delete(ctx, k)
		}
	}
	return a.overrides.Shared()
}

// WatchReputationResets applies, on this replica, every reset announced in
// the override store after this replica started (see ReputationResetPrefix).
// Call once after SetOverrides; a nil store means resets stay per replica.
func (a *AdminAPI) WatchReputationResets(ctx context.Context) {
	if a.overrides == nil || a.repService == nil {
		return
	}
	logger := a.logger
	if logger == nil {
		logger = slog.Default()
	}
	start := time.Now()
	override.Watch(ctx, logger, a.overrides, ReputationResetPrefix, a.resetWatchInterval, func(m map[string]string) {
		for key, value := range m {
			if prev, ok := a.resetsApplied.Load(key); ok && prev == value {
				continue
			}
			a.resetsApplied.Store(key, value)
			serviceID, target, ok := parseResetAnnouncementKey(key)
			if !ok {
				continue
			}
			at, err := time.Parse(time.RFC3339Nano, value)
			if err != nil || at.Before(start.Add(-resetReplayGrace)) {
				// Announced before this replica existed: whatever it reset
				// was hydrated from storage already, and replaying it now
				// would erase what this replica has learned since.
				continue
			}
			keys, err := applyReset(ctx, a.repService, serviceID, target)
			switch {
			case errors.Is(err, reputation.ErrNoScore):
				logger.Info("reputation reset announced elsewhere matched nothing here", "service", serviceID, "target", target)
			case err != nil:
				logger.Warn("reputation reset announced elsewhere did not fully apply", "service", serviceID, "target", target, "error", err)
			default:
				logger.Warn("reputation reset applied from another replica", "service", serviceID, "target", target, "keys", keys)
			}
		}
		// Forget announcements the store no longer lists, so the map does
		// not grow with every reset ever made.
		a.resetsApplied.Range(func(k, _ any) bool {
			if _, ok := m[k.(string)]; !ok {
				a.resetsApplied.Delete(k)
			}
			return true
		})
	})
}

// resetsApplied is the announcements this replica has already acted on,
// key to value, shared by the route (which applies before it announces)
// and the watcher.
type resetsApplied = sync.Map
