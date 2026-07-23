package heuristic

// containsFold reports whether haystack contains needle using ASCII
// case-insensitive comparison, without allocating. The needle must be
// lowercase ASCII (all heuristic patterns are). This replaces the
// strings.ToLower(string(body)) idiom, which allocated up to 2KB per
// response on the hot path.
func containsFold(haystack []byte, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if matchFoldAt(haystack, i, needle) {
			return true
		}
	}
	return false
}

// matchFoldAt reports whether needle matches haystack at offset start,
// ASCII case-insensitively.
func matchFoldAt(haystack []byte, start int, needle string) bool {
	for j := 0; j < len(needle); j++ {
		c := haystack[start+j]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != needle[j] {
			return false
		}
	}
	return true
}
