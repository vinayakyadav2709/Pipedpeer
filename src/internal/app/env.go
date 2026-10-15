package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/nixgen"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/pythondeps"
	"github.com/pipedpeer/pipedpeer/internal/runrec"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
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
	// Stages is how long each build step took. Every run made from this
	// environment starts its timeline with them.
	Stages []jobhistory.Stage

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
	// primitives route through the cluster (see nixgen/shim/).
	Intercept bool
	// GPU selects the CUDA build of torch. Off, the closure is several
	// gigabytes smaller and comes from the binary cache instead of a
	// fixed-output wheel fetch.
	GPU bool
	// Verbose prints the "Found…/Using…/Built…" detail lines and nix's own
	// build output. Off, only warnings print (nix output only on failure).
	Verbose bool
}

// StageFn reports build progress. Callers own the numbering so a single run
// and a fan-out can label the same steps differently.
type StageFn func(step int, title string)

// BuildEnvironment resolves dependencies, builds the Nix closure, exports it,
// and packs the workspace for the project containing absScriptPath. Cancelling
// ctx stops the nix build. The whole build is recorded as one env_build step.
func BuildEnvironment(ctx context.Context, absScriptPath string, opts EnvOptions, stage StageFn) (*Environment, error) {
	detail := func(format string, a ...any) {
		if opts.Verbose {
			fmt.Printf(format, a...)
		}
	}
	started := time.Now()
	defer func() {
		machine := ""
		if me, err := identity.GetOrCreate(); err == nil {
			machine = me.NodeID
		} else {
			fmt.Fprintf(os.Stderr, "      warning: env_build step has no machine id: %v\n", err)
		}
		runrec.Add(runrec.Step{Machine: machine, Node: machine, Chunk: -1, Name: "env_build",
			StartNS: started.UnixNano(), DurNS: time.Since(started).Nanoseconds()})
	}()
	if stage == nil {
		stage = func(int, string) {}
	}
	// Each step runs until the next one begins; the last until the build
	// returns.
	var timeline jobhistory.Timeline
	endStep := func() {}
	report := stage
	stage = func(step int, title string) {
		endStep()
		endStep = timeline.Begin(buildStageName(step, title), "local")
		report(step, title)
	}
	projectRoot := findProjectRoot(filepath.Dir(absScriptPath))

	stage(1, "Detecting imports...")
	importScan := pythondeps.ExtractImportScan(absScriptPath)

	var imports []string
	fromLock := false
	uvLockPath := filepath.Join(projectRoot, "uv.lock")
	if uvPkgs, err := pythondeps.ParseUVLock(uvLockPath); err == nil {
		detail("      Found uv.lock in project root\n")
		imports = uvPkgs
		fromLock = true
	} else {
		imports = importScan.ExternalDeps
		if len(imports) > 0 {
			detail("      Found external imports: %s\n", strings.Join(imports, ", "))
		}
	}
	if len(importScan.LocalFiles) > 0 {
		detail("      Found local imports: %d (will be bundled)\n", len(importScan.LocalFiles))
	}

	stage(2, "Parsing dependencies...")
	if pythondeps.RequirementsBeside(filepath.Dir(absScriptPath)) != "" {
		detail("      Using requirements.txt beside the script (as if passed with --pkg)\n")
	}
	nixPkgs, unknown := PlanPackages(absScriptPath, imports, opts.Pkgs, fromLock)
	if len(unknown) > 0 {
		found, missing, err := pythondeps.SplitByExistence(unknown, ProbePythonPackages)
		if err != nil {
			fmt.Fprintf(os.Stderr, "      warning: could not look up %s in nixpkgs: %v\n", strings.Join(unknown, ", "), err)
		}
		for _, f := range found {
			detail("      Not in the built-in table, but nixpkgs has python3Packages.%s: using it\n", f)
			if !slices.Contains(nixPkgs, f) {
				nixPkgs = append(nixPkgs, f)
			}
		}
		if len(missing) > 0 {
			fmt.Fprintf(os.Stderr, "      warning: not packaged (the job will fail to import it): %s — add it with --pkg <nixpkgs name>\n",
				strings.Join(missing, ", "))
		}
	}
	for _, pkg := range nixPkgs {
		detail("      Using Nix package: %s\n", pkg)
	}

	// On disk, not in RAM: the exported closure is written here and a torch
	// closure is several gigabytes.
	tmpDir, err := userdir.Scratch("build-*")
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
	detail("      Created: %s\n", flakePath)

	stage(4, "Building locally...")
	template := envTemplate(opts.PythonVersion, opts.GPU)
	storePath, reused := findEnv(envIndexPath(), template, nixPkgs)
	if reused {
		detail("      Reusing an environment that already has these packages\n")
	} else {
		nixSystem := nixgen.NixArch()
		cmd, cleanup, err := nixstore.CmdContext(ctx, tmpDir, "nix", "build", flakeRef(nixSystem),
			"--option", "build-users-group", "")
		if err != nil {
			env.Close()
			return nil, err
		}
		defer cleanup()
		var nixOut bytes.Buffer
		if opts.Verbose {
			cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		} else {
			cmd.Stdout, cmd.Stderr = &nixOut, &nixOut
		}
		if err := cmd.Run(); err != nil {
			env.Close()
			os.Stderr.Write(nixOut.Bytes())
			if ctx.Err() != nil {
				return nil, fmt.Errorf("nix build stopped: %w", ctx.Err())
			}
			return nil, fmt.Errorf("nix build failed: %v", err)
		}
		detail("      Built successfully\n")
		storePath, err = os.Readlink(filepath.Join(tmpDir, "result"))
		if err != nil {
			env.Close()
			return nil, fmt.Errorf("failed to readlink result: %v", err)
		}
		recordEnv(envIndexPath(), template, nixPkgs, storePath)
	}

	stage(5, "Exporting closure...")
	env.StorePath = storePath
	detail("      Store path: %s\n", storePath)

	env.NarPath = filepath.Join(tmpDir, "closure.nar")
	// The NAR is not exported here: UploadJob exports it on demand only when a
	// target daemon lacks the closure (shared stores mean most uploads skip it
	// entirely), so a 6GB gzip export doesn't run on every repeated run.

	env.WorkspaceTar = filepath.Join(tmpDir, "workspace.tar")
	var shim fs.FS
	if opts.Intercept {
		shim = nixgen.ShimDir()
	}
	if err := daemonctl.CreateWorkspaceTar(projectRoot, env.WorkspaceTar, shim); err != nil {
		env.Close()
		return nil, fmt.Errorf("workspace tar failed: %v", err)
	}

	env.ScriptRel, err = filepath.Rel(projectRoot, absScriptPath)
	if err != nil {
		env.ScriptRel = filepath.Base(absScriptPath)
	}

	endStep()
	env.Stages = timeline.Stages
	return env, nil
}

// PlanPackages turns a script's imports, its --pkg values and its
// requirements files (the one beside the script included) into nix packages,
// reading each requirements file once. unknown lists, once each, the names the
// built-in table does not know: every unmapped requirement name, plus the
// imports nothing covers unless fromLock (uv.lock names are distributions, not
// imports, so the import check does not apply). The caller probes nixpkgs for
// unknown. Used by a run and by `run --check-only`, so both list the same.
func PlanPackages(absScriptPath string, imports, pkgs []string, fromLock bool) (nix, unknown []string) {
	if req := pythondeps.RequirementsBeside(filepath.Dir(absScriptPath)); req != "" {
		pkgs = append(append([]string{}, pkgs...), req)
	}
	var plain, reqNames []string
	read := map[string]bool{}
	for _, p := range pkgs {
		if !strings.HasSuffix(p, ".txt") {
			plain = append(plain, p)
			continue
		}
		key := p
		if abs, err := filepath.Abs(p); err == nil {
			key = abs
		}
		if read[key] {
			continue
		}
		read[key] = true
		b, err := os.ReadFile(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "      warning: cannot read %s: %v\n", p, err)
			continue
		}
		reqNames = append(reqNames, pythondeps.ParseRequirements(string(b))...)
	}
	nix = pythondeps.ResolvePackages(imports, plain)
	addUnknown := func(n string) {
		if !slices.Contains(unknown, n) {
			unknown = append(unknown, n)
		}
	}
	for _, n := range reqNames {
		if m := pythondeps.ResolveNixPackage(n); m == "" {
			addUnknown(n)
		} else if !slices.Contains(nix, m) {
			nix = append(nix, m)
		}
	}
	if !fromLock {
		for _, imp := range unpackaged(imports, append(plain, reqNames...)) {
			addUnknown(imp)
		}
	}
	return nix, unknown
}

// unpackaged lists imports with no nix package, leaving out any the user
// named with --pkg or a requirements file (python3Packages.tqdm covers
// "tqdm", the requirement "foo-bar" covers "foo_bar").
func unpackaged(imports, pkgs []string) []string {
	norm := strings.NewReplacer("_", "-").Replace
	var out []string
	for _, imp := range pythondeps.UnresolvedImports(imports) {
		covered := false
		for _, p := range pkgs {
			if strings.EqualFold(norm(p[strings.LastIndex(p, ".")+1:]), norm(imp)) {
				covered = true
				break
			}
		}
		if !covered {
			out = append(out, imp)
		}
	}
	return out
}

// buildStageNames are the timeline's names for BuildEnvironment's steps.
var buildStageNames = map[int]string{
	1: "detect imports",
	2: "resolve packages",
	3: "generate flake",
	4: "nix build",
	5: "export closure",
}

func buildStageName(step int, title string) string {
	if n, ok := buildStageNames[step]; ok {
		return n
	}
	return strings.TrimSuffix(strings.ToLower(title), "...")
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

// flakeRef uses the path: type so Nix does not treat the scratch dir as part
// of an enclosing git tree (e.g. a git-tracked $HOME).
func flakeRef(system string) string { return "path:.#packages." + system + ".default" }

// ProbePythonPackages asks the pinned nixpkgs, in one nix eval, which of names
// exist as python3Packages attributes. Names are import names already vetted
// as [A-Za-z0-9_] by pythondeps, so they are safe inside the expression.
func ProbePythonPackages(names []string) (map[string]bool, error) {
	var list strings.Builder
	for _, n := range names {
		list.WriteString(`"` + n + `" `)
	}
	expr := fmt.Sprintf(`let ps = (builtins.getFlake %q).legacyPackages.%s.python3Packages; in builtins.listToAttrs (map (n: { name = n; value = ps ? ${n}; }) [ %s])`,
		nixgen.NixpkgsRef, nixgen.NixArch(), list.String())
	if err := os.MkdirAll(userdir.Cache(), 0o755); err != nil {
		return nil, err
	}
	cmd, cleanup, err := nixstore.Cmd(userdir.Cache(), "nix", "eval", "--json", "--expr", expr)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("nix eval: %w", err)
	}
	res := map[string]bool{}
	if err := json.Unmarshal(out, &res); err != nil {
		return nil, fmt.Errorf("nix eval output: %w", err)
	}
	return res, nil
}

// Environment reuse: a script whose packages an already-built environment
// covers runs in that one, so it needs no nix build, and a node holding that
// environment needs nothing sent. The index lives under userdir.Data().

type envEntry struct {
	Template  string   `json:"template"`
	Pkgs      []string `json:"pkgs"`
	StorePath string   `json:"store_path"`
}

func envIndexPath() string { return filepath.Join(userdir.Data(), "envs.json") }

// envTemplate fingerprints everything but the package list: the python, the
// GPU choice, the pinned nixpkgs and the flake template of this build.
func envTemplate(python string, gpu bool) string {
	sum := sha256.Sum256([]byte(nixgen.GenerateFlake(nil, python, gpu)))
	return hex.EncodeToString(sum[:])
}

// heavyPkgs are never brought along as extras: shipping one to a node for a
// script that never imports it costs more than the build it saves.
var heavyPkgs = map[string]bool{"torch": true, "tensorflow": true, "jax": true, "jaxlib": true}

// maxExtraPkgs bounds how much bigger a reused environment may be.
// ponytail: a count, not closure bytes; weigh by size if light extras bloat.
const maxExtraPkgs = 3

// findEnv returns the smallest recorded environment with the same template
// whose packages cover want with at most maxExtraPkgs light extras, if its
// store path is still in the local store.
func findEnv(index, template string, want []string) (string, bool) {
	entries := readEnvIndex(index)
	best, bestExtra := "", maxExtraPkgs+1
	for _, e := range entries {
		if e.Template != template {
			continue
		}
		extra, ok := 0, true
		for _, w := range want {
			if !slices.Contains(e.Pkgs, w) {
				ok = false
				break
			}
		}
		for _, p := range e.Pkgs {
			if !slices.Contains(want, p) {
				if heavyPkgs[p] {
					ok = false
				}
				extra++
			}
		}
		if !ok || extra >= bestExtra {
			continue
		}
		if _, err := os.Stat(filepath.Join(e.StorePath, "bin", "run")); err != nil {
			continue
		}
		best, bestExtra = e.StorePath, extra
	}
	return best, best != ""
}

// recordEnv adds a built environment to the index, replacing an entry for the
// same template and packages.
func recordEnv(index, template string, pkgs []string, storePath string) {
	sorted := slices.Clone(pkgs)
	slices.Sort(sorted)
	entries := slices.DeleteFunc(readEnvIndex(index), func(e envEntry) bool {
		return e.Template == template && slices.Equal(e.Pkgs, sorted)
	})
	entries = append(entries, envEntry{Template: template, Pkgs: sorted, StorePath: storePath})
	b, err := json.Marshal(entries)
	if err == nil {
		err = os.MkdirAll(filepath.Dir(index), 0o755)
	}
	if err == nil {
		err = os.WriteFile(index, b, 0o644)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "      warning: cannot record the environment for reuse (%v)\n", err)
	}
}

func readEnvIndex(index string) []envEntry {
	b, err := os.ReadFile(index)
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "      warning: cannot read %s (%v); building afresh\n", index, err)
		}
		return nil
	}
	var entries []envEntry
	if err := json.Unmarshal(b, &entries); err != nil {
		fmt.Fprintf(os.Stderr, "      warning: %s is unreadable (%v); building afresh\n", index, err)
		return nil
	}
	return entries
}
