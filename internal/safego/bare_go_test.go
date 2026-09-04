package safego

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// Every bare `go` statement in the tree must open with `defer safego.Recover`.
//
// This is the rule at the top of CLAUDE.md, checked. net/http contains a
// panic inside a request; that protection stops at the goroutine boundary,
// and every `go` in this codebase crosses it. The rule was written down in
// August and the 2026-09-04 audit found five goroutines that broke it — a
// rule nothing enforces is remembered until the next contributor. safego.Go,
// GoCtx and Call are not `go` statements and do not appear here; this is
// only for the goroutines that keep their own `go func()` for its defers.
func TestEveryBareGoStatementRecoversFirst(t *testing.T) {
	root := filepath.Join("..", "..")
	fset := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "local", "bin", "docs":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		// This package is the exception: it implements the rule.
		if strings.Contains(filepath.ToSlash(path), "internal/safego/") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			stmt, ok := n.(*ast.GoStmt)
			if !ok {
				return true
			}
			pos := fset.Position(stmt.Pos())
			where := filepath.ToSlash(strings.TrimPrefix(pos.Filename, root+string(filepath.Separator))) + ":" + itoa(pos.Line)
			if !recoversFirst(stmt) {
				violations = append(violations, where)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) > 0 {
		t.Fatalf("bare go statements whose function does not begin with `defer safego.Recover(...)`:\n  %s\n"+
			"use safego.Go / GoCtx / Call, or make `defer safego.Recover(logger, name)` the FIRST statement of the goroutine (CLAUDE.md, Goroutines)",
			strings.Join(violations, "\n  "))
	}
}

// recoversFirst reports whether the goroutine is a function literal whose
// first statement defers safego.Recover.
func recoversFirst(stmt *ast.GoStmt) bool {
	lit, ok := stmt.Call.Fun.(*ast.FuncLit)
	if !ok || lit.Body == nil || len(lit.Body.List) == 0 {
		return false
	}
	deferStmt, ok := lit.Body.List[0].(*ast.DeferStmt)
	if !ok {
		return false
	}
	sel, ok := deferStmt.Call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Recover" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "safego"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
