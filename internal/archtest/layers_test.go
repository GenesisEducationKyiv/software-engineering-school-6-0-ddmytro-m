//go:build unit

// Package archtest enforces the layered dependency direction documented in
// docs/architecture.md: shared < infra < worker < transport < cmd. A package
// may import its own layer or anything strictly below it, never above.
// Worker services are additionally forbidden from importing one another —
// per ADR 004/005 they are independent bounded contexts that communicate
// only through the broker.
package archtest

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
)

// layerRank orders layers from innermost (lowest) to outermost (highest).
// A package may depend on its own rank or lower, never higher.
var layerRank = map[string]int{
	"shared":    1,
	"infra":     2,
	"worker":    3,
	"transport": 4,
	"cmd":       5,
}

// hasPathPrefix reports whether rel is prefix itself or a descendant of it,
// so a package sitting directly in a layer's root directory (not just one
// of its subpackages) is still classified.
func hasPathPrefix(rel, prefix string) bool {
	return rel == prefix || strings.HasPrefix(rel, prefix+"/")
}

// layerOf classifies a module-relative package directory (e.g.
// "internal/worker/scanner") into one of the layers above. For the worker
// layer it also returns the specific service name ("scanner", "notifier",
// ...) so sibling worker imports can be flagged separately from rank
// violations. ok is false for anything not part of the layered graph
// (e.g. this package itself, or generated proto stubs).
func layerOf(rel string) (layer, service string, ok bool) {
	switch {
	case rel == "internal/logger", rel == "internal/utils", rel == "internal/config",
		rel == "internal/events", rel == "internal/metrics":
		return "shared", "", true
	case hasPathPrefix(rel, "internal/infra"), hasPathPrefix(rel, "internal/api"):
		return "infra", "", true
	case hasPathPrefix(rel, "internal/worker"):
		rest := strings.TrimPrefix(strings.TrimPrefix(rel, "internal/worker"), "/")
		service, _, _ = strings.Cut(rest, "/")
		return "worker", service, true
	case hasPathPrefix(rel, "internal/transport"):
		return "transport", "", true
	case hasPathPrefix(rel, "cmd"):
		return "cmd", "", true
	default:
		return "", "", false
	}
}

type violation struct {
	from, to, reason string
}

func TestLayerDependencyDirection(t *testing.T) {
	root := repoRoot(t)
	modulePath := mainModulePath(t)
	fset := token.NewFileSet()

	var (
		violations   []violation
		filesScanned int
		edgesChecked int // internal-module import edges actually classified on both ends
	)

	for _, dir := range []string{"internal", "cmd"} {
		walkRoot := filepath.Join(root, dir)
		err := filepath.WalkDir(walkRoot, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}

			relDir, relErr := filepath.Rel(root, filepath.Dir(path))
			if relErr != nil {
				return relErr
			}
			relDir = filepath.ToSlash(relDir)
			fromLayer, fromService, ok := layerOf(relDir)
			if !ok {
				return nil
			}
			filesScanned++

			f, parseErr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if parseErr != nil {
				return parseErr
			}
			for _, imp := range f.Imports {
				importPath := strings.Trim(imp.Path.Value, `"`)
				if !strings.HasPrefix(importPath, modulePath+"/") {
					continue // stdlib, third-party, or generated proto stubs
				}
				rel := strings.TrimPrefix(importPath, modulePath+"/")
				toLayer, toService, toOK := layerOf(rel)
				if !toOK {
					continue
				}
				edgesChecked++

				switch {
				case fromLayer == "worker" && toLayer == "worker" && fromService != toService:
					violations = append(violations, violation{
						from:   relDir,
						to:     rel,
						reason: "worker services must not import one another — they communicate only through the broker (ADR 004/005)",
					})
				case layerRank[fromLayer] < layerRank[toLayer]:
					violations = append(violations, violation{
						from:   relDir,
						to:     rel,
						reason: fmt.Sprintf("layer %q must not depend on higher layer %q", fromLayer, toLayer),
					})
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", dir, err)
		}
	}

	// Guard against the test silently no-op'ing (e.g. a wrong repo root or a
	// module path that no longer matches go.mod would make every import look
	// like a third-party one, and the loop above would "pass" having checked
	// nothing). These minimums are well below the current package count and
	// only need bumping if the module is ever pared down drastically.
	if filesScanned < 20 {
		t.Fatalf("only scanned %d files under internal/ and cmd/ — layer classification is probably broken", filesScanned)
	}
	if edgesChecked < 20 {
		t.Fatalf("only checked %d intra-module import edges — modulePath (%q) probably doesn't match go.mod", edgesChecked, modulePath)
	}

	sort.Slice(violations, func(i, j int) bool { return violations[i].from < violations[j].from })
	for _, v := range violations {
		t.Errorf("%s -> %s: %s", v.from, v.to, v.reason)
	}
}

// mainModulePath reads the module path from the test binary's embedded
// build info rather than hardcoding it, so this test can't silently drift
// out of sync with go.mod.
func mainModulePath(t *testing.T) string {
	t.Helper()
	bi, ok := debug.ReadBuildInfo()
	if !ok || bi.Main.Path == "" {
		t.Fatal("could not read main module path from build info")
	}
	return bi.Main.Path
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine caller for repo root lookup")
	}
	// This file lives at internal/archtest/layers_test.go, two levels
	// below the repo root.
	return filepath.Join(filepath.Dir(file), "..", "..")
}
