package pythondeps

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestExtractImportsUniqueAndOrdered(t *testing.T) {
	tmp := t.TempDir()
	script := filepath.Join(tmp, "script.py")
	content := "import numpy\nfrom pandas import DataFrame\nimport numpy\nfrom fastapi import FastAPI\n"
	if err := os.WriteFile(script, []byte(content), 0644); err != nil {
		t.Fatalf("write script: %v", err)
	}

	got := ExtractImports(script)
	want := []string{"numpy", "pandas", "fastapi"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("imports mismatch\nwant: %#v\n got: %#v", want, got)
	}
}

func TestResolveNixPackage(t *testing.T) {
	if got := ResolveNixPackage("numpy"); got != "numpy" {
		t.Fatalf("expected numpy mapping, got %q", got)
	}
	if got := ResolveNixPackage("sklearn"); got != "scikit-learn" {
		t.Fatalf("expected sklearn mapping to scikit-learn, got %q", got)
	}
	if got := ResolveNixPackage("joblib"); got != "joblib" {
		t.Fatalf("expected joblib mapping, got %q", got)
	}
	if got := ResolveNixPackage("unknown_pkg"); got != "" {
		t.Fatalf("expected empty mapping, got %q", got)
	}
}

func TestExtractImportScanSeparatesExternalAndLocal(t *testing.T) {
	tmp := t.TempDir()
	script := filepath.Join(tmp, "train.py")
	localModule := filepath.Join(tmp, "helpers.py")

	if err := os.WriteFile(localModule, []byte("def x():\n    return 1\n"), 0644); err != nil {
		t.Fatalf("write local module: %v", err)
	}

	content := "import numpy\nfrom sklearn.tree import DecisionTreeClassifier\nimport helpers\n"
	if err := os.WriteFile(script, []byte(content), 0644); err != nil {
		t.Fatalf("write script: %v", err)
	}

	scan := ExtractImportScan(script)

	wantDeps := []string{"numpy", "sklearn"}
	if !reflect.DeepEqual(scan.ExternalDeps, wantDeps) {
		t.Fatalf("external deps mismatch\nwant: %#v\n got: %#v", wantDeps, scan.ExternalDeps)
	}

	wantLocals := []string{localModule}
	if !reflect.DeepEqual(scan.LocalFiles, wantLocals) {
		t.Fatalf("local files mismatch\nwant: %#v\n got: %#v", wantLocals, scan.LocalFiles)
	}
}

func TestResolvePackages(t *testing.T) {
	tmp := t.TempDir()
	reqFile := filepath.Join(tmp, "requirements.txt")
	content := "numpy\npandas>=2.0\npytest==7.0.0\nunknown-pkg"
	if err := os.WriteFile(reqFile, []byte(content), 0644); err != nil {
		t.Fatalf("write req file: %v", err)
	}

	astImports := []string{"flask", "scipy"}
	explicit := []string{"pillow", reqFile, "boto3"}

	got := ResolvePackages(astImports, explicit)
	want := []string{"flask", "scipy", "pillow", "numpy", "pandas", "pytest", "boto3"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resolved packages mismatch\nwant: %#v\n got: %#v", want, got)
	}
}

func TestStdlibModulesFilteredFromExternalDeps(t *testing.T) {
	tmp := t.TempDir()
	script := filepath.Join(tmp, "script.py")
	content := "import os\nimport sys\nimport json\nimport pickle\nimport numpy\nimport math\nfrom pathlib import Path\nimport csv\n"
	if err := os.WriteFile(script, []byte(content), 0644); err != nil {
		t.Fatalf("write script: %v", err)
	}

	got := ExtractImports(script)
	// Only numpy should appear — os, sys, json, pickle, math, pathlib, csv are all stdlib
	want := []string{"numpy"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stdlib filtering failed\nwant: %#v\n got: %#v", want, got)
	}
}

// TestCommaSeparatedImportsAreAllDetected covers a bug that produced a closure
// missing a package and an error blaming the user's script.
//
// The detector matched the first name on an import line, so "import numpy,
// pandas" put numpy in the closure and left pandas out. The job then failed on
// the worker with ModuleNotFoundError, which points at the script rather than
// at the dependency detection that caused it. Found by a benchmark whose
// fixture happened to use that form.
func TestCommaSeparatedImportsAreAllDetected(t *testing.T) {
	cases := []struct {
		line string
		want []string
	}{
		{"import numpy, pandas", []string{"numpy", "pandas"}},
		{"import numpy,pandas,scipy", []string{"numpy", "pandas", "scipy"}},
		{"import numpy as np, pandas as pd", []string{"numpy", "pandas"}},
		{"import os.path, sys", []string{"os", "sys"}},

		// Unchanged behaviour, asserted so the rewrite did not quietly alter it.
		{"import numpy", []string{"numpy"}},
		{"import numpy as np", []string{"numpy"}},
		{"from pandas import DataFrame", []string{"pandas"}},
		{"from sklearn.ensemble import RandomForestClassifier", []string{"sklearn"}},

		// A relative import names no top-level module.
		{"from . import helper", nil},
		{"from .util import thing", nil},

		// Not imports at all.
		{"# import numpy, pandas", nil},
		{"important = 1", nil},
		{"print('import numpy')", nil},
		{"", nil},
	}
	for _, c := range cases {
		got := importedModules(c.line)
		if len(got) != len(c.want) {
			t.Errorf("%q gave %v, want %v", c.line, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%q gave %v, want %v", c.line, got, c.want)
				break
			}
		}
	}
}

// TestTrailingCommentDoesNotHideAnImport. Stripping the comment must not take
// the import with it.
func TestTrailingCommentDoesNotHideAnImport(t *testing.T) {
	got := importedModules("import numpy, pandas  # both needed")
	if len(got) != 2 || got[0] != "numpy" || got[1] != "pandas" {
		t.Errorf("got %v, want numpy and pandas", got)
	}
}

func TestUnresolvedImports(t *testing.T) {
	got := UnresolvedImports([]string{"numpy", "os", "foo", "tqdm", "seaborn", "bar"})
	if len(got) != 2 || got[0] != "foo" || got[1] != "bar" {
		t.Fatalf("got %v", got)
	}
}

func TestSplitByExistence(t *testing.T) {
	exists := func(names []string) (map[string]bool, error) {
		return map[string]bool{"rich": true}, nil
	}
	found, missing, err := SplitByExistence([]string{"Rich", "nopkg_xyz"}, exists)
	if err != nil || len(found) != 1 || found[0] != "rich" || len(missing) != 1 || missing[0] != "nopkg_xyz" {
		t.Fatalf("found=%v missing=%v err=%v", found, missing, err)
	}
}

func TestRequirementsBeside(t *testing.T) {
	d := t.TempDir()
	if RequirementsBeside(d) != "" {
		t.Fatal("no file, want empty")
	}
	p := filepath.Join(d, "requirements.txt")
	if err := os.WriteFile(p, []byte("nomappedpkg==13\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := RequirementsBeside(d); got != p {
		t.Fatalf("got %q", got)
	}
	if got := ResolvePackages(nil, []string{p}); len(got) != 0 || len(RequirementsUnmapped([]string{p})) != 1 {
		t.Fatalf("requirements not read: %v", got)
	}
}

func TestExtractImportScanRecursesIntoLocalModules(t *testing.T) {
	d := t.TempDir()
	w := func(name, body string) {
		p := filepath.Join(d, name)
		os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	w("main.py", "import helper\nimport pkg\nimport numpy\n")
	w("helper.py", "import seaborn\nimport main\nimport helper\n")
	w("pkg/__init__.py", "")
	w("pkg/sub.py", "import tqdm\nfrom .. import helper\n")
	got := ExtractImportScan(filepath.Join(d, "main.py"))
	want := []string{"numpy", "seaborn", "tqdm"}
	sort.Strings(got.ExternalDeps)
	sort.Strings(want)
	if !reflect.DeepEqual(got.ExternalDeps, want) {
		t.Fatalf("deps %v want %v", got.ExternalDeps, want)
	}
}

func TestParseRequirementsForms(t *testing.T) {
	in := "# c\n\n-r other.txt\n-e .\n--index-url https://x\nNumPy[extra]>=1.2 ; python_version<'3.12'\nscikit_learn==1.0\npkg @ https://h/p.whl\n./local\nfoo; sys_platform=='linux'\nbar  # note\n"
	got := ParseRequirements(in)
	want := []string{"numpy", "scikit-learn", "foo", "bar"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want %v got %v", want, got)
	}
}

func TestUnresolvedImportsKnowsImportAliases(t *testing.T) {
	got := UnresolvedImports([]string{"numpy", "cv2", "PIL", "somethingobscure"})
	if !reflect.DeepEqual(got, []string{"somethingobscure"}) {
		t.Fatalf("unresolved = %#v, want only the unknown import", got)
	}
}

// Every mapped name must be a nixpkgs python attribute: "Pillow" was not, and
// any script importing PIL failed to build. Checked against the attribute
// spelling rules nixpkgs uses (lowercase, hyphens), not a live evaluation.
func TestMappedNamesAreLowercase(t *testing.T) {
	for imp, pkg := range nixpkgsMapping {
		if pkg != strings.ToLower(pkg) {
			t.Errorf("%s maps to %q; nixpkgs python attributes are lowercase", imp, pkg)
		}
	}
}
