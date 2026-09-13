package healthcheck

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/override"
	"github.com/pokt-network/sage/qos"
)

type floorSpy struct{ last atomic.Uint64 }

func (f *floorSpy) SetExternalFloor(h uint64) { f.last.Store(h) }

func resolveTo(spy *floorSpy, known ...domain.ServiceID) func(domain.ServiceID) (qos.ExternalFloorSetter, bool) {
	return func(id domain.ServiceID) (qos.ExternalFloorSetter, bool) {
		for _, k := range known {
			if k == id {
				return spy, true
			}
		}
		return nil, false
	}
}

func heightServer(t *testing.T, hex string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"` + hex + `"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func waitFloor(t *testing.T, spy *floorSpy, want uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if spy.last.Load() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("floor = %d, want %d", spy.last.Load(), want)
}

// The manager exists so a source can be replaced on a running process: the
// old fetcher stops, the new one starts, and the floor follows the new source.
func TestExternalSourceManager_SetReplacesRunningSource(t *testing.T) {
	spy := &floorSpy{}
	m := NewExternalSourceManager(slog.Default(), nil, resolveTo(spy, "eth"))
	old := heightServer(t, "0x64")   // 100
	fresh := heightServer(t, "0xc8") // 200

	if err := m.Configure("eth", []config.ExternalBlockSource{{URL: old.URL, Interval: time.Second}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	waitFloor(t, spy, 100)

	v, err := m.Set("eth", []config.ExternalBlockSource{{URL: fresh.URL, Interval: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	if v.Origin != SourceOriginAdmin || len(v.Sources) != 1 || v.Sources[0].URL != fresh.URL || !v.Status.Running {
		t.Fatalf("view after Set = %+v", v)
	}
	waitFloor(t, spy, 200)

	got, ok := m.Get("eth")
	if !ok || got.Status.LastHeight != 200 || got.Status.Failing {
		t.Fatalf("Get after Set = %+v, %v", got, ok)
	}
	// Remove clears the admin override: the file's source is polled again.
	if !m.Remove("eth") {
		t.Fatal("Remove should report there was an override")
	}
	waitFloor(t, spy, 100)
	back, ok := m.Get("eth")
	if !ok || back.Origin != SourceOriginConfig || back.Sources[0].URL != old.URL {
		t.Fatalf("after Remove = %+v, %v; want the file's source back", back, ok)
	}
	if m.Remove("eth") {
		t.Fatal("second Remove should report no override")
	}

	// An empty list stops polling but keeps the file's sources known.
	v, err = m.Set("eth", nil)
	if err != nil {
		t.Fatal(err)
	}
	if v.Origin != SourceOriginAdminDisabled || v.Status.Running || len(v.Configured) != 1 {
		t.Fatalf("disabled view = %+v", v)
	}
}

// An override persisted by one manager is applied by another sharing the
// store — a second replica, or the process after a restart — and clearing it
// on one clears it on the other.
func TestExternalSourceManager_PersistsAcrossManagers(t *testing.T) {
	shared := override.NewMemoryStore()
	file := heightServer(t, "0x64")   // 100, the "config file" source
	fresh := heightServer(t, "0x12c") // 300, the admin's

	spyA := &floorSpy{}
	a := NewExternalSourceManager(slog.Default(), nil, resolveTo(spyA, "eth"))
	a.SetOverrides(shared)
	_ = a.Configure("eth", []config.ExternalBlockSource{{URL: file.URL, Interval: time.Second}})
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	a.Start(ctxA)
	waitFloor(t, spyA, 100)
	if _, err := a.Set("eth", []config.ExternalBlockSource{{URL: fresh.URL, Interval: time.Second}}); err != nil {
		t.Fatal(err)
	}
	waitFloor(t, spyA, 300)
	if v, ok, _ := shared.Get(context.Background(), "external_sources/eth"); !ok || !strings.Contains(v, fresh.URL) {
		t.Fatalf("store = %q,%v; want the override persisted", v, ok)
	}

	// The "restarted" process: same file, same store, no admin call.
	spyB := &floorSpy{}
	b := NewExternalSourceManager(slog.Default(), nil, resolveTo(spyB, "eth"))
	b.SetOverrides(shared)
	_ = b.Configure("eth", []config.ExternalBlockSource{{URL: file.URL, Interval: time.Second}})
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	b.Start(ctxB)
	waitFloor(t, spyB, 300)
	if v, _ := b.Get("eth"); v.Origin != SourceOriginAdmin {
		t.Fatalf("second manager origin = %q, want admin from the store", v.Origin)
	}

	// Cleared on A, B follows back to the file's source.
	if !a.Remove("eth") {
		t.Fatal("Remove on A should clear the override")
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := b.Get("eth"); v.Origin == SourceOriginConfig {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if v, _ := b.Get("eth"); v.Origin != SourceOriginConfig {
		t.Fatalf("B origin = %q after A cleared the override, want config", v.Origin)
	}
	if a.Persistent() {
		t.Fatal("a memory store is not shared")
	}
}

// A service set before Start polls once Start runs; the config origin is
// reported until an admin replaces it.
func TestExternalSourceManager_ViewAndOrigin(t *testing.T) {
	spy := &floorSpy{}
	m := NewExternalSourceManager(slog.Default(), nil, resolveTo(spy, "eth", "poly"))
	srv := heightServer(t, "0x10")
	if err := m.Configure("poly", []config.ExternalBlockSource{{URL: srv.URL, Type: "json_rpc", Interval: 15 * time.Second, Timeout: 5 * time.Second}}); err != nil {
		t.Fatal(err)
	}
	views := m.View()
	if len(views) != 1 || views[0].ServiceID != "poly" || views[0].Origin != SourceOriginConfig || views[0].Status.Running {
		t.Fatalf("views before Start = %+v", views)
	}
	if views[0].Sources[0].Interval != "15s" || views[0].Sources[0].Timeout != "5s" {
		t.Fatalf("durations should render as strings: %+v", views[0].Sources[0])
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	waitFloor(t, spy, 16)
	if v, _ := m.Get("poly"); !v.Status.Running {
		t.Fatalf("running after Start: %+v", v)
	}
}

func TestExternalSourceManager_SetRefusals(t *testing.T) {
	spy := &floorSpy{}
	m := NewExternalSourceManager(slog.Default(), nil, resolveTo(spy, "eth"))
	good := []config.ExternalBlockSource{{URL: "https://example.com"}}

	if _, err := m.Set("nope", good); !errors.Is(err, ErrNoFloor) {
		t.Fatalf("unknown service: err = %v, want ErrNoFloor from the resolver", err)
	}
	cases := map[string][]config.ExternalBlockSource{
		"bad scheme":   {{URL: "ftp://example.com"}},
		"no host":      {{URL: "https://"}},
		"bad type":     {{URL: "https://example.com", Type: "grpc"}},
		"interval low": {{URL: "https://example.com", Interval: 100 * time.Millisecond}},
		"timeout high": {{URL: "https://example.com", Timeout: 2 * time.Minute}},
	}
	for name, srcs := range cases {
		if _, err := m.Set("eth", srcs); !errors.Is(err, ErrInvalidSources) {
			t.Errorf("%s: err = %v, want ErrInvalidSources", name, err)
		}
	}
	nine := make([]config.ExternalBlockSource, 9)
	for i := range nine {
		nine[i] = config.ExternalBlockSource{URL: "https://example.com"}
	}
	if _, err := m.Set("eth", nine); !errors.Is(err, ErrInvalidSources) {
		t.Errorf("nine sources: err = %v, want ErrInvalidSources", err)
	}
	if _, ok := m.Get("eth"); ok {
		t.Fatal("a refused Set must not leave sources behind")
	}
}

func TestSourceFromSpec_ParsesDurations(t *testing.T) {
	src, err := SourceFromSpec(ExternalSourceSpec{URL: " https://x.example ", Type: "rest", Path: "/status", Interval: "20s", Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	if src.URL != "https://x.example" || src.Interval != 20*time.Second || src.Timeout != 5*time.Second || src.Path != "/status" {
		t.Fatalf("parsed = %+v", src)
	}
	if _, err := SourceFromSpec(ExternalSourceSpec{URL: "https://x.example", Interval: "soon"}); err == nil {
		t.Fatal("a bad duration must be an error")
	}
}
