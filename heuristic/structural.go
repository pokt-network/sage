package heuristic

import "bytes"

// Structural checks for Tier 1 analysis.
// These are fast, pre-JSON checks on the raw response body.

// IsEmpty returns true if the response body is empty or only whitespace.
func IsEmpty(body []byte) bool {
	return len(bytes.TrimSpace(body)) == 0
}

// htmlPrefixes are common prefixes of HTML responses.
var htmlPrefixes = [][]byte{
	[]byte("<!DOCTYPE"),
	[]byte("<!doctype"),
	[]byte("<html"),
	[]byte("<HTML"),
}

// IsHTML returns true if the response body looks like HTML.
func IsHTML(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return false
	}
	// JSON bodies ({ or [) cannot be HTML — early-out for the common case so
	// every successful JSON-RPC response skips the scan below.
	if trimmed[0] == '{' || trimmed[0] == '[' {
		return false
	}
	for _, prefix := range htmlPrefixes {
		if bytes.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	// Check for HTML tags anywhere near the start (some error pages don't start with DOCTYPE).
	if len(trimmed) > 500 {
		trimmed = trimmed[:500]
	}
	return containsFold(trimmed, "<html") && containsFold(trimmed, "</html")
}

// IsPlainText returns true if the response body is plain text (not JSON, not HTML, not XML).
// It detects common error pages and proxy error messages.
func IsPlainText(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return false
	}
	first := trimmed[0]
	// JSON starts with { or [
	if first == '{' || first == '[' {
		return false
	}
	// XML/HTML starts with <
	if first == '<' {
		return false
	}
	// If it's printable ASCII and doesn't start with JSON/XML markers, it's plain text.
	return true
}

// IsXML returns true if the response body looks like XML (but not HTML).
func IsXML(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return false
	}
	if bytes.HasPrefix(trimmed, []byte("<?xml")) || bytes.HasPrefix(trimmed, []byte("<?XML")) {
		return true
	}
	// Check for XML-like structure that isn't HTML.
	if trimmed[0] == '<' && !IsHTML(body) {
		// Look for closing tags typical of XML error responses.
		if bytes.Contains(trimmed, []byte("</Error>")) || bytes.Contains(trimmed, []byte("</error>")) {
			return true
		}
	}
	return false
}
