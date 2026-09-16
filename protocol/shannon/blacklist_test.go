package shannon

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func TestBlacklist_AddAndCheck(t *testing.T) {
	bl := newBlacklist()
	svcID := domain.ServiceID("eth")
	addr := "pokt1supplier"

	if bl.IsBlacklisted(svcID, addr) {
		t.Error("should not be blacklisted before adding")
	}

	bl.BlacklistSupplier(svcID, addr)

	if !bl.IsBlacklisted(svcID, addr) {
		t.Error("should be blacklisted after adding")
	}
}

func TestBlacklist_Remove(t *testing.T) {
	bl := newBlacklist()
	svcID := domain.ServiceID("eth")
	addr := "pokt1supplier"

	bl.BlacklistSupplier(svcID, addr)
	removed := bl.UnblacklistSupplier(svcID, addr)

	if !removed {
		t.Error("UnblacklistSupplier should return true when entry existed")
	}
	if bl.IsBlacklisted(svcID, addr) {
		t.Error("should not be blacklisted after removal")
	}
}

func TestBlacklist_RemoveNonExistent(t *testing.T) {
	bl := newBlacklist()
	removed := bl.UnblacklistSupplier("eth", "pokt1unknown")
	if removed {
		t.Error("UnblacklistSupplier should return false for non-existent entry")
	}
}

func TestBlacklist_Expiry(t *testing.T) {
	bl := newBlacklist()
	bl.duration = 1 * time.Millisecond

	svcID := domain.ServiceID("eth")
	addr := "pokt1supplier"

	bl.BlacklistSupplier(svcID, addr)

	// Confirm it's blacklisted immediately.
	if !bl.IsBlacklisted(svcID, addr) {
		t.Error("should be blacklisted immediately after adding")
	}

	// Wait for expiry.
	time.Sleep(5 * time.Millisecond)

	if bl.IsBlacklisted(svcID, addr) {
		t.Error("should not be blacklisted after expiry")
	}
}

func TestBlacklist_ServiceIsolation(t *testing.T) {
	bl := newBlacklist()
	addr := "pokt1supplier"

	bl.BlacklistSupplier("eth", addr)

	if bl.IsBlacklisted("poly", addr) {
		t.Error("blacklist should be scoped to service ID")
	}
}

// Expired entries leave the map when the next supplier is blacklisted.
func TestBlacklist_AddPrunesExpired(t *testing.T) {
	bl := newBlacklist()
	bl.duration = 1 * time.Millisecond

	bl.BlacklistSupplier("eth", "pokt1a")
	bl.BlacklistSupplier("eth", "pokt1b")

	time.Sleep(5 * time.Millisecond)
	bl.BlacklistSupplier("eth", "pokt1c")

	bl.mu.RLock()
	n := len(bl.blocked)
	bl.mu.RUnlock()

	if n != 1 {
		t.Errorf("blocked holds %d entries, want only the fresh one", n)
	}
}
