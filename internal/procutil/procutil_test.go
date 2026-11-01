package procutil

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestHideWindowAndCommandHelpers(t *testing.T) {
	if HideWindow(nil) != nil {
		t.Fatal("expected nil cmd to return nil")
	}

	cmd1 := Command("git", "status")
	cmd2 := CommandContext(context.Background(), "git", "status")
	cmd3 := HideWindow(exec.Command("git", "status"))

	for i, cmd := range []*exec.Cmd{cmd1, cmd2, cmd3} {
		if cmd == nil {
			t.Fatalf("cmd[%d] is nil", i)
		}
		if !isWindowHidden(cmd) {
			t.Fatalf("cmd[%d] expected window hidden attributes set", i)
		}
	}

	if IsPIDAlive(0) || IsPIDAlive(-1) {
		t.Fatal("expected IsPIDAlive(<=0) to be false")
	}
	if !IsPIDAlive(os.Getpid()) {
		t.Fatal("expected IsPIDAlive(os.Getpid()) to be true")
	}
	if IsPIDAlive(99999999) {
		t.Fatal("expected IsPIDAlive(99999999) to be false")
	}
}

func TestNoUnguardedExecCommandInProductionCode(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	fset := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == "vendor" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, relErr := filepath.Rel(repoRoot, path)
		if relErr != nil {
			rel = path
		}
		relSlash := filepath.ToSlash(rel)
		if strings.HasPrefix(relSlash, "internal/procutil/") {
			return nil
		}

		fileAst, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		ast.Inspect(fileAst, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			if ident.Name == "exec" && (sel.Sel.Name == "Command" || sel.Sel.Name == "CommandContext") {
				pos := fset.Position(call.Pos())
				violations = append(violations, pos.String()+": unguarded exec."+sel.Sel.Name+" (use procutil."+sel.Sel.Name+")")
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk repo failed: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("found %d unguarded exec.Command/CommandContext call(s) in production code:\n%s",
			len(violations), strings.Join(violations, "\n"))
	}
}
