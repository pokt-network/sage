package healthcheck

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/override"
)

type checksSpy struct{ last *ConfiguredChecks }

func (s *checksSpy) SetConfiguredChecks(c *ConfiguredChecks) { s.last = c }

func (s *checksSpy) names(svc domain.ServiceID) []string {
	var out []string
	for _, c := range s.last.For(svc) {
		out = append(out, c.Name)
	}
	return out
}

// sei's EVM probe added on a running gateway: it replaces the file's block,
// survives a reload, reaches another replica through the store, and DELETE
// returns to the file's.
func TestCheckOverrides_ReplaceReloadFanOutRemove(t *testing.T) {
	base := config.HealthCheckConfig{Local: []config.ServiceHealthChecks{{
		ServiceID: "sei", Enabled: true, Checks: []config.HealthCheck{{Name: "file_check", Body: `{}`}},
	}}}
	known := func(id domain.ServiceID) bool { return id == "sei" }
	store := override.NewMemoryStore()

	spy := &checksSpy{}
	m := NewCheckOverrides(spy, known, nil)
	m.SetOverrides(store)
	m.SetBase(base)
	if got := spy.names("sei"); !slices.Equal(got, []string{"sei:file_check"}) {
		t.Fatalf("base checks = %v", got)
	}

	spec := ServiceChecksSpec{Checks: []CheckSpec{{Name: "evm_block_number", Type: "json_rpc", Body: `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`}}}
	v, err := m.Set("sei", spec)
	if err != nil {
		t.Fatal(err)
	}
	if v.Origin != SourceOriginAdmin || v.Configured == nil {
		t.Fatalf("view = %+v, want origin admin with the file's block underneath", v)
	}
	if got := spy.names("sei"); !slices.Equal(got, []string{"sei:evm_block_number"}) {
		t.Fatalf("after PUT = %v, want the admin block in place of the file's", got)
	}

	m.SetBase(base) // a config reload keeps the admin block on top
	if got := spy.names("sei"); !slices.Equal(got, []string{"sei:evm_block_number"}) {
		t.Fatalf("after reload = %v", got)
	}

	other := &checksSpy{}
	m2 := NewCheckOverrides(other, known, nil)
	m2.SetBase(base)
	entries, _ := store.List(context.Background(), checkOverridePrefix)
	m2.applyPersisted(entries)
	if got := other.names("sei"); !slices.Equal(got, []string{"sei:evm_block_number"}) {
		t.Fatalf("another replica = %v, want the persisted block", got)
	}

	if !m.Remove("sei") {
		t.Fatal("Remove found nothing to clear")
	}
	if got := spy.names("sei"); !slices.Equal(got, []string{"sei:file_check"}) {
		t.Fatalf("after DELETE = %v, want the file's block again", got)
	}
}

func TestCheckOverrides_Refuses(t *testing.T) {
	m := NewCheckOverrides(&checksSpy{}, func(id domain.ServiceID) bool { return id == "sei" }, nil)
	m.SetBase(config.HealthCheckConfig{})

	if _, err := m.Set("nope", ServiceChecksSpec{}); !errors.Is(err, ErrUnknownService) {
		t.Errorf("unknown service: err = %v", err)
	}
	for name, spec := range map[string]ServiceChecksSpec{
		"websocket type": {Checks: []CheckSpec{{Name: "ws", Type: "websocket"}}},
		"no name":        {Checks: []CheckSpec{{Type: "json_rpc"}}},
		"bad timeout":    {Checks: []CheckSpec{{Name: "x", Timeout: "soon"}}},
		"bad interval":   {CheckInterval: "often"},
	} {
		if _, err := m.Set("sei", spec); !errors.Is(err, ErrInvalidChecks) {
			t.Errorf("%s: err = %v, want ErrInvalidChecks", name, err)
		}
	}
}
