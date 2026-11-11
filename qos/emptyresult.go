package qos

import "github.com/tidwall/gjson"

// EmptyResult reports whether a JSON-RPC answer's result is empty: null, or
// an empty array, object or string. An error answer has no result and is not
// empty; "error": null is no error. Only the first element is read, so a large result costs nothing.
func EmptyResult(response []byte) bool {
	parsed := gjson.ParseBytes(response)
	if e := parsed.Get("error"); e.Exists() && e.Type != gjson.Null {
		return false
	}
	result := parsed.Get("result")
	switch {
	case !result.Exists():
		return false
	case result.IsArray():
		return !result.Get("0").Exists()
	case result.IsObject():
		empty := true
		result.ForEach(func(_, _ gjson.Result) bool { empty = false; return false })
		return empty
	default:
		return result.Type == gjson.Null || (result.Type == gjson.String && result.Str == "")
	}
}
