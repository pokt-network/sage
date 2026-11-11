package qos

import "testing"

func TestEmptyResult(t *testing.T) {
	for body, want := range map[string]bool{
		`{"jsonrpc":"2.0","id":1,"result":[]}`:                         true,
		`{"jsonrpc":"2.0","id":1,"result":[ ]}`:                        true,
		`{"jsonrpc":"2.0","id":1,"result":null}`:                       true,
		`{"jsonrpc":"2.0","id":1,"result":{}}`:                         true,
		`{"jsonrpc":"2.0","id":1,"result":""}`:                         true,
		`{"jsonrpc":"2.0","id":1,"result":[{"address":"0x1"}]}`:        false,
		`{"jsonrpc":"2.0","id":1,"result":"0x0"}`:                      false,
		`{"jsonrpc":"2.0","id":1,"result":{"a":1}}`:                    false,
		`{"jsonrpc":"2.0","id":1,"error":{"code":4444,"message":"x"}}`: false,
		`{"jsonrpc":"2.0","id":1,"result":[],"error":null}`:            true,
		`not json`: false,
	} {
		if got := EmptyResult([]byte(body)); got != want {
			t.Errorf("EmptyResult(%s) = %v, want %v", body, got, want)
		}
	}
}
