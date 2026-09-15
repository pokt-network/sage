package drain

import (
	"context"
	"log/slog"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
)

// WatchEnds calls onEnd for every drain that was live at the previous check
// and is not live now — expired or released — across services.
//
// It exists so what a drain froze can be let go of: a drained endpoint gets
// neither traffic nor health checks, so its score is whatever it was when it
// was benched, and at the drain's end it re-entered selection on that score
// (mainnet sei, 2026-09-15 18:57Z: eight keys straight back into tier 1).
// A drain ending on another replica or in the peer's store is seen here at the
// next check, since Active reads the shared view.
func WatchEnds(ctx context.Context, logger *slog.Logger, store Store, services []domain.ServiceID, every time.Duration, onEnd func(Entry)) {
	safego.GoCtx(ctx, logger, "drain.watch_ends", func(ctx context.Context) {
		prev := live(ctx, store, services)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				safego.Run(logger, "drain.watch_ends.tick", func() {
					cur := live(ctx, store, services)
					for _, e := range ended(prev, cur) {
						onEnd(e)
					}
					prev = cur
				})
			}
		}
	})
}

func live(ctx context.Context, store Store, services []domain.ServiceID) map[Key]Entry {
	out := map[Key]Entry{}
	for _, svc := range services {
		for _, e := range store.Active(ctx, svc) {
			out[e.Key] = e
		}
	}
	return out
}

// ended returns the entries of prev that cur no longer has.
func ended(prev, cur map[Key]Entry) []Entry {
	var out []Entry
	for k, e := range prev {
		if _, ok := cur[k]; !ok {
			out = append(out, e)
		}
	}
	return out
}
