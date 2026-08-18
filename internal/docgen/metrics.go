package docgen

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const metricsPreamble = `# Metrics reference

Every Prometheus series SAGE exports, generated from the collectors in
` + "`metrics/`" + `.

Metrics are served on their own listener — ` + "`metrics_config.prometheus_addr`" + `,
default ` + "`:9090`" + ` — separate from both the relay port and the admin API. See
[operations.md](operations.md) for the port layout and why it is split three
ways.

**Label cardinality is bounded on purpose.** ` + "`service_id`" + ` comes from the
client's ` + "`Target-Service-Id`" + ` header, so any value not in the configured
service set collapses to ` + "`__unknown__`" + ` rather than minting a new time
series per junk header. A spike in ` + "`__unknown__`" + ` is worth alerting on: it
means traffic is arriving for services this gateway does not serve.
`

// metricDoc is one exported Prometheus series.
type metricDoc struct {
	name   string
	kind   string
	help   string
	labels []string
}

// GenerateMetricsReference renders the metric reference from the collector
// definitions in metricsDir.
//
// It reads the source rather than scraping a live registry: a scrape only shows
// series that have been touched, so a counter nobody has incremented yet — the
// interesting ones, usually — would be missing from its own documentation.
func GenerateMetricsReference(metricsDir string) (string, error) {
	entries, err := os.ReadDir(metricsDir)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", metricsDir, err)
	}

	fset := token.NewFileSet()
	var metrics []metricDoc
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(metricsDir, name), nil, parser.ParseComments)
		if err != nil {
			return "", fmt.Errorf("parse %s: %w", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if m, ok := metricFromVecCall(call); ok {
				metrics = append(metrics, m)
			}
			if m, ok := metricFromDescCall(call); ok {
				metrics = append(metrics, m)
			}
			return true
		})
	}

	sort.Slice(metrics, func(i, j int) bool { return metrics[i].name < metrics[j].name })

	var b strings.Builder
	b.WriteString(generatedNotice)
	b.WriteString("\n\n")
	b.WriteString(metricsPreamble)
	b.WriteString("\n| Metric | Type | Labels | Description |\n|---|---|---|---|\n")
	for _, m := range metrics {
		labels := "—"
		if len(m.labels) > 0 {
			quoted := make([]string, len(m.labels))
			for i, l := range m.labels {
				quoted[i] = "`" + l + "`"
			}
			labels = strings.Join(quoted, ", ")
		}
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s |\n", m.name, m.kind, labels, mdEscape(m.help))
	}
	return b.String(), nil
}

// metricFromVecCall recognises prometheus.New{Counter,Gauge,Histogram}Vec and
// their unlabelled Func variants. The Vec forms take an Opts literal and a
// label list; the Func forms take an Opts literal and the function that
// supplies the value, so they document as a metric with no labels.
//
// The Func forms are here because a metric this generator cannot see is a
// metric the reference silently omits, which is the one failure mode generated
// docs are supposed to remove.
func metricFromVecCall(call *ast.CallExpr) (metricDoc, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || len(call.Args) < 2 {
		return metricDoc{}, false
	}
	var kind string
	labelled := true
	switch sel.Sel.Name {
	case "NewCounterVec":
		kind = "counter"
	case "NewGaugeVec":
		kind = "gauge"
	case "NewHistogramVec":
		kind = "histogram"
	case "NewCounterFunc":
		kind, labelled = "counter", false
	case "NewGaugeFunc":
		kind, labelled = "gauge", false
	default:
		return metricDoc{}, false
	}

	opts, ok := call.Args[0].(*ast.CompositeLit)
	if !ok {
		return metricDoc{}, false
	}
	var namespace, name, help string
	for _, elt := range opts.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}
		val := stringLit(kv.Value)
		switch key.Name {
		case "Namespace":
			namespace = val
		case "Name":
			name = val
		case "Help":
			help = val
		}
	}
	if name == "" {
		return metricDoc{}, false
	}
	if namespace != "" {
		name = namespace + "_" + name
	}
	var labels []string
	if labelled {
		labels = stringSlice(call.Args[1])
	}
	return metricDoc{name: name, kind: kind, help: help, labels: labels}, true
}

// metricFromDescCall recognises prometheus.NewDesc(name, help, labels, nil),
// which is how custom Collectors declare their series.
func metricFromDescCall(call *ast.CallExpr) (metricDoc, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "NewDesc" || len(call.Args) < 3 {
		return metricDoc{}, false
	}
	name := stringLit(call.Args[0])
	if name == "" {
		return metricDoc{}, false
	}
	return metricDoc{
		name:   name,
		kind:   "gauge",
		help:   stringLit(call.Args[1]),
		labels: stringSlice(call.Args[2]),
	}, true
}

// stringLit unquotes a string literal expression, returning "" for anything
// else — a metric whose name is computed is not something a doc can report.
func stringLit(e ast.Expr) string {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return ""
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return ""
	}
	return s
}

// stringSlice reads a []string{...} literal.
func stringSlice(e ast.Expr) []string {
	lit, ok := e.(*ast.CompositeLit)
	if !ok {
		return nil
	}
	var out []string
	for _, elt := range lit.Elts {
		if s := stringLit(elt); s != "" {
			out = append(out, s)
		}
	}
	return out
}
