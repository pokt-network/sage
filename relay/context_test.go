package relay

import "testing"

func TestContext_Clone(t *testing.T) {
	ctx := &Context{RequestID: "abc"}

	clone := ctx.Clone()

	if clone.RequestID != "abc" {
		t.Error("clone should preserve RequestID")
	}

	clone.Degraded = true
	if ctx.Degraded {
		t.Error("modifying clone fields should not affect original")
	}
}
