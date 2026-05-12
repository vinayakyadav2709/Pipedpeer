package pythondeps

import (
	"os"
	"path/filepath"
	"reflect"
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
	want := []string{"flask", "scipy", "Pillow", "numpy", "pandas", "pytest", "unknown-pkg", "boto3"}

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
