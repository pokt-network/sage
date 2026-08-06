package docgen

import "path/filepath"

// GenerateAll produces every generated doc, keyed by its path relative to the
// repository root. Callers write them or compare them; the generators
// themselves never touch the filesystem beyond reading source, which is what
// lets the golden test and the `make docs` command share one code path and
// therefore never disagree.
func GenerateAll(root string) (map[string]string, error) {
	out := make(map[string]string)

	cfg, err := GenerateConfigReference(root)
	if err != nil {
		return nil, err
	}
	out["docs/configuration.md"] = cfg

	metrics, err := GenerateMetricsReference(filepath.Join(root, "metrics"))
	if err != nil {
		return nil, err
	}
	out["docs/metrics.md"] = metrics

	admin, err := GenerateAdminAPIReference(filepath.Join(root, "router"))
	if err != nil {
		return nil, err
	}
	out["docs/admin-api.md"] = admin

	return out, nil
}
