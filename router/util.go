package router

import "fmt"

// formatTypeName returns a short string identifying the concrete type of v.
// Used for display-only purposes in the config dump.
func formatTypeName(v any) string {
	return fmt.Sprintf("%T", v)
}
