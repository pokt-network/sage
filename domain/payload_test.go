package domain

import "testing"

// The id must survive with its JSON type intact: a client matching responses
// to requests compares 1 and "1" as different ids.
func TestPayload_JSONRPCID(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    string
	}{
		{"numeric id", `{"id":1}`, "1"},
		{"string id", `{"id":"req-1"}`, `"req-1"`},
		{"null id", `{"id":null}`, "null"},
		{"missing id", `{}`, "null"},
		{"invalid json", `not json`, "null"},
		{"empty", ``, "null"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := string(NewPayload([]byte(tc.payload), RPCTypeJSONRPC, "").JSONRPCID())
			if got != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
		})
	}
}
