package featureflag

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/domain"
)

func TestMemoryStore_IsEnabled_Default(t *testing.T) {
	store := NewMemoryStore(nil)
	ctx := context.Background()

	// Should fall back to compiled defaults.
	if !store.IsEnabled(ctx, "retry", "eth") {
		t.Error("expected retry to be enabled by default")
	}
	if store.IsEnabled(ctx, "tracing", "eth") {
		t.Error("expected tracing to be disabled by default")
	}
}

func TestMemoryStore_IsEnabled_Global(t *testing.T) {
	store := NewMemoryStore(map[string]bool{"retry": false})
	ctx := context.Background()

	if store.IsEnabled(ctx, "retry", "eth") {
		t.Error("expected retry to be disabled via constructor defaults")
	}
}

func TestMemoryStore_Set(t *testing.T) {
	store := NewMemoryStore(nil)
	ctx := context.Background()

	if err := store.Set(ctx, "tracing", true); err != nil {
		t.Fatal(err)
	}
	if !store.IsEnabled(ctx, "tracing", "eth") {
		t.Error("expected tracing to be enabled after Set")
	}
}

func TestMemoryStore_ServiceOverride(t *testing.T) {
	store := NewMemoryStore(nil)
	ctx := context.Background()

	// Disable retry globally.
	if err := store.Set(ctx, "retry", false); err != nil {
		t.Fatal(err)
	}

	// Enable retry for "eth" only.
	if err := store.SetForService(ctx, "retry", "eth", true); err != nil {
		t.Fatal(err)
	}

	if !store.IsEnabled(ctx, "retry", "eth") {
		t.Error("expected retry enabled for eth via service override")
	}
	if store.IsEnabled(ctx, "retry", "poly") {
		t.Error("expected retry disabled for poly (no override)")
	}
}

func TestMemoryStore_IsEnabled_Priority(t *testing.T) {
	store := NewMemoryStore(nil)
	ctx := context.Background()

	// Default: retry=true
	// Set global to false
	_ = store.Set(ctx, "retry", false)
	// Set service override to true for eth
	_ = store.SetForService(ctx, "retry", "eth", true)

	// Service override > global > default
	if !store.IsEnabled(ctx, "retry", "eth") {
		t.Error("service override should take priority over global")
	}
	if store.IsEnabled(ctx, "retry", "poly") {
		t.Error("global=false should apply when no service override")
	}
}

func TestMemoryStore_Delete_ServiceOverride(t *testing.T) {
	store := NewMemoryStore(nil)
	ctx := context.Background()

	_ = store.Set(ctx, "retry", false)
	_ = store.SetForService(ctx, "retry", "eth", true)

	// Delete service override.
	if err := store.Delete(ctx, "retry", "eth"); err != nil {
		t.Fatal(err)
	}

	// Should fall back to global (false).
	if store.IsEnabled(ctx, "retry", "eth") {
		t.Error("expected retry disabled after deleting service override")
	}
}

func TestMemoryStore_Delete_Global(t *testing.T) {
	store := NewMemoryStore(nil)
	ctx := context.Background()

	_ = store.Set(ctx, "retry", false)
	_ = store.SetForService(ctx, "retry", "eth", true)

	// Delete global + all overrides.
	if err := store.Delete(ctx, "retry", ""); err != nil {
		t.Fatal(err)
	}

	// Should fall back to compiled default (true).
	if !store.IsEnabled(ctx, "retry", "eth") {
		t.Error("expected retry enabled from compiled default after global delete")
	}
}

func TestMemoryStore_GetAll(t *testing.T) {
	store := NewMemoryStore(nil)
	ctx := context.Background()

	_ = store.Set(ctx, "tracing", true)
	_ = store.SetForService(ctx, "retry", "eth", false)

	all, err := store.GetAll(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !all["tracing"].Enabled {
		t.Error("expected tracing enabled in GetAll")
	}

	retryState := all["retry"]
	if !retryState.Enabled {
		t.Error("expected retry globally enabled (default)")
	}
	if retryState.ServiceOverrides[domain.ServiceID("eth")] != false {
		t.Error("expected eth override to be false")
	}
}

func TestMemoryStore_UnknownFlag(t *testing.T) {
	store := NewMemoryStore(nil)
	ctx := context.Background()

	// Unknown flag should return false (zero value, not in DefaultFlags).
	if store.IsEnabled(ctx, "nonexistent_flag", "eth") {
		t.Error("expected unknown flag to be disabled")
	}
}

// TestMemoryStore_DeleteGlobal_KeepsServiceOverrides: config carries global
// values only, so a flag dropped from the file must not revoke a per-service
// decision an operator made through the admin API.
func TestMemoryStore_DeleteGlobal_KeepsServiceOverrides(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(map[string]bool{FlagTracing: true})
	if err := store.SetForService(ctx, FlagTracing, "eth", true); err != nil {
		t.Fatalf("set for service: %v", err)
	}

	if err := store.DeleteGlobal(ctx, FlagTracing); err != nil {
		t.Fatalf("delete global: %v", err)
	}

	if store.IsEnabled(ctx, FlagTracing, "poly") {
		t.Error("the global value survived DeleteGlobal; DefaultFlags should apply again")
	}
	if !store.IsEnabled(ctx, FlagTracing, "eth") {
		t.Error("DeleteGlobal wiped the per-service override too")
	}
}
