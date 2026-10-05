package shannon

import (
	"context"
	"fmt"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/reputation"
)

// A supplier's repeat of a notification it already sent is kept from the
// client behind ws_drop_duplicates; off, it is forwarded as before.
func TestWSProcessor_DropsDuplicateNotifications(t *testing.T) {
	for _, drop := range []bool{true, false} {
		p, _, _, fn := buildProcessorFixture()
		proc := newWSMessageProcessor(context.Background(), p,
			&sessiontypes.SessionHeader{ServiceId: "eth", SessionId: "s-1", SessionEndBlockHeight: 200},
			"pokt1supplier", "pokt1supplier-https://rel001.op-alpha.example",
			&apptypes.Application{Address: "pokt1app"}, nil)
		proc.subs = qos.NewSubscriptionRegistry(&evm.Plugin{})
		proc.duplicates = &wsDuplicates{drop: func() bool { return drop }, penalize: func() bool { return false }, record: func(reputation.SignalType) {}, report: func(int, int) {}, limits: defaultDupLimits}
		endpoint := func(payload string) []byte {
			t.Helper()
			fn.validateResponse = &servicetypes.RelayResponse{Payload: []byte(payload)}
			out, err := proc.ProcessEndpointMessage([]byte(`wire`))
			if err != nil {
				t.Fatal(err)
			}
			return out
		}
		if _, err := proc.ProcessClientMessage([]byte(`{"jsonrpc":"2.0","id":9,"method":"eth_subscribe","params":["newPendingTransactions"]}`)); err != nil {
			t.Fatal(err)
		}
		endpoint(`{"jsonrpc":"2.0","id":9,"result":"0xsub"}`)
		tx := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xsub","result":"0xabc"}}`
		if out := endpoint(tx); out == nil {
			t.Fatalf("drop=%v: the first copy was not forwarded", drop)
		}
		if out := endpoint(tx); (out == nil) != drop {
			t.Fatalf("drop=%v: the repeat forwarded=%v", drop, out != nil)
		}
	}
}

// A supplier is charged once per period in which more than the max share of
// at least the minimum notifications were repeats (1% of 500 by default, both
// tuning knobs), minor up to the major share (10%) and major above it, and only
// behind ws_duplicate_penalty. Every minute's counts are reported for the
// party's repeat share, charged or not.
func TestWSDuplicates_PenalizesARepeatingSupplier(t *testing.T) {
	lim := func(maxShare, majorShare float64, minNotes int) func() DuplicateLimits {
		return func() DuplicateLimits {
			return DuplicateLimits{MaxShare: maxShare, MajorShare: majorShare, MinNotifications: minNotes}
		}
	}
	for name, tc := range map[string]struct {
		notes, dups int
		on          bool
		limits      func() DuplicateLimits
		want        string
	}{
		"2% of 600":                   {600, 12, true, defaultDupLimits, "minor"},
		"15% of 600":                  {600, 90, true, defaultDupLimits, "major"},
		"0.5% of 600":                 {600, 3, true, defaultDupLimits, ""},
		"2% of 400":                   {400, 8, true, defaultDupLimits, ""},
		"15% of 600, off":             {600, 90, false, defaultDupLimits, ""},
		"2% of 600, max share 5%":     {600, 12, true, lim(0.05, 0.10, 500), ""},
		"2% of 400, min notes at 300": {400, 8, true, lim(0.01, 0.10, 300), "minor"},
		"15% of 600, major at 20%":    {600, 90, true, lim(0.01, 0.20, 500), "minor"},
	} {
		var charged []string
		reported := 0
		d := &wsDuplicates{
			drop: func() bool { return false }, penalize: func() bool { return tc.on },
			record: func(st reputation.SignalType) {
				charged = append(charged, map[reputation.SignalType]string{reputation.SignalMinorError: "minor", reputation.SignalMajorError: "major"}[st])
			},
			report: func(notes, _ int) { reported += notes },
			limits: tc.limits,
		}
		start := time.Unix(0, 0)
		for i := 0; i < tc.notes; i++ {
			d.observe(i < tc.dups, start.Add(time.Duration(i)*time.Millisecond))
		}
		d.observe(false, start.Add(dupPeriod+time.Second)) // closes the period
		want := []string{}
		if tc.want != "" {
			want = []string{tc.want}
		}
		if fmt.Sprint(charged) != fmt.Sprint(want) {
			t.Errorf("%s: charged %v, want %v", name, charged, want)
		}
		if reported != tc.notes+1 {
			t.Errorf("%s: reported %d notifications, want %d", name, reported, tc.notes+1)
		}
	}
}

func defaultDupLimits() DuplicateLimits { return defaultDuplicateLimits }
