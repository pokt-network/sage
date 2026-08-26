package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pokt-network/sage/methodblock"
)

func TestAdmin_MethodBlocks_GetAndClear(t *testing.T) {
	store := methodblock.New()
	store.Mark("eth", "slow.example.com", "eth_getLogs", true)
	api := newTestAdminAPIWithBlocks(t, store) // helper: same as the existing admin test constructor, plus the store
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/method-blocks/eth", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status %d: %s", rec.Code, rec.Body)
	}
	var blocks []methodblock.Block
	if err := json.Unmarshal(rec.Body.Bytes(), &blocks); err != nil || len(blocks) != 1 || blocks[0].Method != "eth_getLogs" {
		t.Fatalf("GET body %s (%v)", rec.Body, err)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/method-blocks/clear/eth", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST status %d: %s", rec.Code, rec.Body)
	}
	if store.Blocked("eth", "slow.example.com", "eth_getLogs") {
		t.Fatal("clear did not clear")
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/method-blocks/eth", nil))
	if rec.Body.String() != "[]" {
		t.Fatalf("empty list must be [], got %s", rec.Body)
	}
}
