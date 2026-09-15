package router

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/pokt-network/sage/autodrain"
)

func TestAdmin_AutoDrainEvents(t *testing.T) {
	a, srv := newAdminServer(t)

	resp, err := http.Get(srv.URL + "/admin/auto-drain/events")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("no engine: status %d, want 503", resp.StatusCode)
	}

	log := &autodrain.MemoryLog{}
	_ = log.Append(context.Background(), autodrain.Event{At: time.Now(), ServiceID: "sei", Operator: "opa.example", Outcome: autodrain.OutcomeShadow})
	_ = log.Append(context.Background(), autodrain.Event{At: time.Now(), ServiceID: "base", Operator: "opc.example", Outcome: autodrain.OutcomeDrained})
	a.SetAutoDrainEvents(log)

	resp, err = http.Get(srv.URL + "/admin/auto-drain/events?service=sei")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body struct {
		Events []autodrain.Event `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || len(body.Events) != 1 || body.Events[0].Operator != "opa.example" {
		t.Fatalf("status %d, events %+v; want sei's one decision", resp.StatusCode, body.Events)
	}

	bad, err := http.Get(srv.URL + "/admin/auto-drain/events?limit=0")
	if err != nil {
		t.Fatal(err)
	}
	_ = bad.Body.Close()
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("limit=0: status %d, want 400", bad.StatusCode)
	}
}
