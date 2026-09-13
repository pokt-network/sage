package healthcheck

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
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
	if !m.Remove("eth") {
		t.Fatal("Remove should report the service had sources")
	}
	if _, ok := m.Get("eth"); ok {
		t.Fatal("Get after Remove should be empty")
	}
	if m.Remove("eth") {
		t.Fatal("second Remove should report nothing to remove")
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
		"empty":        {},
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
