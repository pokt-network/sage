package blocklist

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/config"
)

// fakeApplier records every union it was handed, and can reject.
type fakeApplier struct {
	mu      sync.Mutex
	applied [][]config.BlockedDomain
	reject  error
}

func (f *fakeApplier) SetBlockedDomains(entries []config.BlockedDomain) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reject != nil {
		return f.reject
	}
	f.applied = append(f.applied, append([]config.BlockedDomain(nil), entries...))
	return nil
}

func (f *fakeApplier) last() []config.BlockedDomain {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.applied) == 0 {
		return nil
	}
	return f.applied[len(f.applied)-1]
}

func domains(entries []config.BlockedDomain) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Domain)
	}
	sort.Strings(out)
	return out
}

func TestManager_SetAppliesUnionAndPersists(t *testing.T) {
	ap := &fakeApplier{}
	be := NewMemoryBackend()
	m := New(ap, be, []config.BlockedDomain{{Domain: "config.example"}})
	ctx := context.Background()

	require.NoError(t, m.Set(ctx, Entry{Domain: " NodeFleet.NET ", RPCTypes: []string{"WebSocket"}, Reason: "dead since July"}))

	assert.Equal(t, []string{"config.example", "nodefleet.net"}, domains(ap.last()))
	assert.Equal(t, []string{"websocket"}, ap.last()[1].RPCTypes, "rpc types lower-cased")

	saved, err := be.Load(ctx)
	require.NoError(t, err)
	require.Len(t, saved, 1)
	assert.Equal(t, "nodefleet.net", saved[0].Domain)
	assert.False(t, saved[0].Since.IsZero(), "Since is stamped")

	entries := m.Entries()
	require.Len(t, entries, 1)
	assert.Equal(t, "dead since July", entries[0].Reason)
	assert.Equal(t, 1, m.Len())
}

func TestManager_SetRejectedByProtocolIsNotStored(t *testing.T) {
	ap := &fakeApplier{reject: errors.New("unknown rpc_type")}
	be := NewMemoryBackend()
	m := New(ap, be, nil)

	err := m.Set(context.Background(), Entry{Domain: "x.example", RPCTypes: []string{"telnet"}})
	require.Error(t, err)
	assert.Empty(t, m.Entries())
	saved, _ := be.Load(context.Background())
	assert.Empty(t, saved, "a rejected entry must not reach the backend")
}

func TestManager_ReleaseRemovesAdminEntryOnly(t *testing.T) {
	ap := &fakeApplier{}
	m := New(ap, NewMemoryBackend(), []config.BlockedDomain{{Domain: "config.example"}})
	ctx := context.Background()
	require.NoError(t, m.Set(ctx, Entry{Domain: "admin.example"}))

	require.NoError(t, m.Release(ctx, "ADMIN.example"))
	assert.Equal(t, []string{"config.example"}, domains(ap.last()))

	err := m.Release(ctx, "config.example")
	assert.ErrorIs(t, err, ErrNotFound, "config entries are the file's to remove")
}

func TestManager_SetBlockedDomainsReplacesBaseKeepsAdmin(t *testing.T) {
	ap := &fakeApplier{}
	m := New(ap, NewMemoryBackend(), []config.BlockedDomain{{Domain: "old.example"}})
	ctx := context.Background()
	require.NoError(t, m.Set(ctx, Entry{Domain: "admin.example"}))

	require.NoError(t, m.SetBlockedDomains([]config.BlockedDomain{{Domain: "new.example"}}))
	assert.Equal(t, []string{"admin.example", "new.example"}, domains(ap.last()))
	assert.Equal(t, []string{"new.example"}, domains(m.Base()))
}

type failingBackend struct {
	MemoryBackend
	fail bool
}

func (b *failingBackend) Save(ctx context.Context, e Entry) error {
	if b.fail {
		return errors.New("redis down")
	}
	return b.MemoryBackend.Save(ctx, e)
}

func TestManager_BackendFailureIsPropagationErrorAndBanStands(t *testing.T) {
	ap := &fakeApplier{}
	be := &failingBackend{MemoryBackend: MemoryBackend{entries: map[string]Entry{}}, fail: true}
	m := New(ap, be, nil)

	err := m.Set(context.Background(), Entry{Domain: "x.example"})
	assert.ErrorIs(t, err, ErrPropagation)
	assert.Equal(t, []string{"x.example"}, domains(ap.last()), "the ban is real on this replica")
	assert.Len(t, m.Entries(), 1)
}

func TestManager_StartLoadsBackendAndPollsPeersWrites(t *testing.T) {
	ap := &fakeApplier{}
	be := NewMemoryBackend()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, be.Save(ctx, Entry{Domain: "restart-survivor.example", Since: time.Now()}))

	m := New(ap, be, nil, WithPollInterval(5*time.Millisecond))
	require.NoError(t, m.Start(ctx))
	assert.Equal(t, []string{"restart-survivor.example"}, domains(ap.last()), "Start applies what the backend holds")

	// A peer writes straight to the backend; the poll picks it up.
	require.NoError(t, be.Save(ctx, Entry{Domain: "peer.example", Since: time.Now()}))
	require.Eventually(t, func() bool {
		return len(domains(ap.last())) == 2
	}, time.Second, time.Millisecond)
	assert.Equal(t, []string{"peer.example", "restart-survivor.example"}, domains(ap.last()))

	// And a peer's release.
	require.NoError(t, be.Delete(ctx, "peer.example"))
	require.Eventually(t, func() bool {
		return len(domains(ap.last())) == 1
	}, time.Second, time.Millisecond)
}

func TestManager_PollDoesNotReapplyWhenUnchanged(t *testing.T) {
	ap := &fakeApplier{}
	be := NewMemoryBackend()
	m := New(ap, be, nil)
	ctx := context.Background()
	require.NoError(t, m.Start(ctx))
	n := len(ap.applied)
	require.NoError(t, m.reload(ctx))
	require.NoError(t, m.reload(ctx))
	assert.Equal(t, n, len(ap.applied), "an unchanged backend must not swap the protocol's list")
}

// --- Redis backend over a fake hash ---

type fakeHash struct {
	mu     sync.Mutex
	fields map[string]string
	err    error
}

func (f *fakeHash) HSet(_ context.Context, _ string, values ...interface{}) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return redis.NewIntResult(0, f.err)
	}
	for i := 0; i+1 < len(values); i += 2 {
		f.fields[values[i].(string)] = values[i+1].(string)
	}
	return redis.NewIntResult(1, nil)
}

func (f *fakeHash) HDel(_ context.Context, _ string, fields ...string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return redis.NewIntResult(0, f.err)
	}
	for _, k := range fields {
		delete(f.fields, k)
	}
	return redis.NewIntResult(1, nil)
}

func (f *fakeHash) HGetAll(_ context.Context, _ string) *redis.MapStringStringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return redis.NewMapStringStringResult(nil, f.err)
	}
	out := make(map[string]string, len(f.fields))
	for k, v := range f.fields {
		out[k] = v
	}
	return redis.NewMapStringStringResult(out, nil)
}

func TestRedisBackend_RoundTripAndSkipsGarbage(t *testing.T) {
	fake := &fakeHash{fields: map[string]string{}}
	be := NewRedisBackend(fake)
	ctx := context.Background()
	since := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	require.NoError(t, be.Save(ctx, Entry{Domain: "a.example", RPCTypes: []string{"websocket"}, Reason: "r", Since: since}))
	fake.fields["garbage.example"] = "{not json"

	got, err := be.Load(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, Entry{Domain: "a.example", RPCTypes: []string{"websocket"}, Reason: "r", Since: since}, got[0])

	require.NoError(t, be.Delete(ctx, "a.example"))
	got, err = be.Load(ctx)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestRedisBackend_ErrorsSurface(t *testing.T) {
	fake := &fakeHash{fields: map[string]string{}, err: fmt.Errorf("connection refused")}
	be := NewRedisBackend(fake)
	ctx := context.Background()
	assert.Error(t, be.Save(ctx, Entry{Domain: "a.example"}))
	assert.Error(t, be.Delete(ctx, "a.example"))
	_, err := be.Load(ctx)
	assert.Error(t, err)
}
