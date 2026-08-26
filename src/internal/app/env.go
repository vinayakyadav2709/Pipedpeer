package app

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/nixgen"
	"github.com/pipedpeer/pipedpeer/internal/pythondeps"
)

// Environment is everything a job needs that does not depend on which node
// runs it or what arguments it gets: the built Nix closure, its exported NAR,
// and the packed project workspace.
//
// Building it is the expensive part of a run (dependency resolution, a Nix
// build, exporting the closure), so a fan-out builds one and reuses it for
// every task instead of repeating the work per task.
type Environment struct {
	StorePath    string
	NarPath      string
	WorkspaceTar string
	ProjectRoot  string
	ScriptRel    string
	FlakeContent string

	tmpDir string
}

// Close removes the temporary build artifacts.
func (e *Environment) Close() {
	if e != nil && e.tmpDir != "" {
		os.RemoveAll(e.tmpDir)
	}
}

// EnvOptions controls how the environment is built.
type EnvOptions struct {
	PythonVersion string
	Pkgs          []string
	// Intercept embeds the sitecustomize shim into the workspace so parallel
	// primitives route through the cluster (see nixgen/shim.go).
	Intercept bool
	// GPU selects the CUDA build of torch. Off, the closure is several
	// gigabytes smaller and comes from the binary cache instead of a
	// fixed-output wheel fetch.
	GPU bool
}

// StageFn reports build progress. Callers own the numbering so a single run
// and a fan-out can label the same steps differently.
type StageFn func(step int, title string)

// BuildEnvironment resolves dependencies, builds the Nix closure, exports it,
// and packs the workspace for the project containing absScriptPath.
func BuildEnvironment(absScriptPath string, opts EnvOptions, stage StageFn) (*Environment, error) {
	if stage == nil {
		stage = func(int, string) {}
	}
	projectRoot := findProjectRoot(filepath.Dir(absScriptPath))

	stage(1, "Detecting imports...")
	importScan := pythondeps.ExtractImportScan(absScriptPath)

	var imports []string
	uvLockPath := filepath.Join(projectRoot, "uv.lock")
	if uvPkgs, err := pythondeps.ParseUVLock(uvLockPath); err == nil {
		fmt.Printf("      Found uv.lock in project root\n")
		imports = uvPkgs
	} else {
		imports = importScan.ExternalDeps
		if len(imports) > 0 {
			fmt.Printf("      Found external imports: %s\n", strings.Join(imports, ", "))
		}
	}
	if len(importScan.LocalFiles) > 0 {
		fmt.Printf("      Found local imports: %d (will be bundled)\n", len(importScan.LocalFiles))
	}

	stage(2, "Parsing dependencies...")
	nixPkgs := pythondeps.ResolvePackages(imports, opts.Pkgs)
	for _, pkg := range nixPkgs {
		fmt.Printf("      Using Nix package: %s\n", pkg)
	}

	tmpDir, err := os.MkdirTemp("", "pipedpeer-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %v", err)
	}
	env := &Environment{ProjectRoot: projectRoot, tmpDir: tmpDir}

	stage(3, "Generating flake.nix...")
	env.FlakeContent = nixgen.GenerateFlake(nixPkgs, opts.PythonVersion, opts.GPU)
	flakePath := filepath.Join(tmpDir, "flake.nix")
	if err := os.WriteFile(flakePath, []byte(env.FlakeContent), 0644); err != nil {
		env.Close()
		return nil, fmt.Errorf("failed to write flake.nix: %v", err)
	}
	fmt.Printf("      Created: %s\n", flakePath)

	stage(4, "Building locally...")
	nixSystem := nixgen.NixArch()
	cmd := exec.Command("nix", "build", ".#packages."+nixSystem+".default", "--option", "build-users-group", "")
	cmd.Dir = tmpDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		env.Close()
		return nil, fmt.Errorf("nix build failed: %v", err)
	}
	fmt.Printf("      Built successfully\n")

	stage(5, "Exporting closure...")
	storePath, err := os.Readlink(filepath.Join(tmpDir, "result"))
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("failed to readlink result: %v", err)
	}
	env.StorePath = storePath
	fmt.Printf("      Store path: %s\n", storePath)

	env.NarPath = filepath.Join(tmpDir, "closure.nar")
	// The NAR is not exported here: UploadJob exports it on demand only when a
	// target daemon lacks the closure (shared stores mean most uploads skip it
	// entirely), so a 6GB gzip export doesn't run on every repeated run.

	env.WorkspaceTar = filepath.Join(tmpDir, "workspace.tar")
	shimContent := ""
	if opts.Intercept {
		shimContent = nixgen.ShimSitecustomize
	}
	if err := daemonctl.CreateWorkspaceTar(projectRoot, env.WorkspaceTar, shimContent); err != nil {
		env.Close()
		return nil, fmt.Errorf("workspace tar failed: %v", err)
	}

	env.ScriptRel, err = filepath.Rel(projectRoot, absScriptPath)
	if err != nil {
		env.ScriptRel = filepath.Base(absScriptPath)
	}

	return env, nil
}

func findProjectRoot(scriptDir string) string {
	for dir := scriptDir; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, ".pipedpeerignore")); err == nil {
			return dir
		}
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return scriptDir
		}
	}
}
