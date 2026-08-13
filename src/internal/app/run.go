package app

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/nixgen"
	"github.com/pipedpeer/pipedpeer/internal/pythondeps"
)

type Options struct {
	ScriptPath    string
	DaemonHost    string
	DaemonPort    int
	TargetID      string
	JobName       string
	Isolate       bool
	GPU           bool
	GPUDevices    string
	Mode          string
	PythonVersion string
	Envs          []string
	Pkgs          []string
	ScriptArgs    []string
	// ResultsDir is where files the job produced are written. Defaults to the
	// project root, which is what a single interactive run wants; a fan-out
	// gives each task its own directory so tasks cannot overwrite each other.
	ResultsDir string
	// Coordinator placement diagnostics
	PlacementSource string
	DegradedMode    bool
	CandidateCount  int
	PlacementReason string
	// Resource estimation
	EstimatedMemBytes int64
	EstimationTier    string
}

func Run(opts Options) (runErr error) {
	if opts.DaemonHost == "" {
		return fmt.Errorf("daemon host is required")
	}
	if opts.DaemonPort <= 0 {
		opts.DaemonPort = 38080
	}

	if _, err := os.Stat(opts.ScriptPath); os.IsNotExist(err) {
		return fmt.Errorf("file not found: %s", opts.ScriptPath)
	}

	absScriptPath, err := filepath.Abs(opts.ScriptPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %v", err)
	}

	resolvedJobName := opts.JobName
	if resolvedJobName == "" {
		resolvedJobName = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}

	historyRecord, historyDir, err := jobhistory.NewRecord(absScriptPath, "", opts.TargetID, false, opts.Isolate)
	if err != nil {
		return fmt.Errorf("failed to initialize job history: %v", err)
	}
	historyRecord.JobName = resolvedJobName
	historyRecord.RunHost = opts.DaemonHost
	historyRecord.PlacementSource = opts.PlacementSource
	historyRecord.DegradedMode = opts.DegradedMode
	historyRecord.CandidateCount = opts.CandidateCount
	historyRecord.PlacementReason = opts.PlacementReason
	historyRecord.EstimatedMemBytes = opts.EstimatedMemBytes
	historyRecord.EstimationTier = opts.EstimationTier
	_ = jobhistory.SaveRecord(historyDir, historyRecord)

	defer func() {
		_ = jobhistory.Finalize(historyDir, historyRecord, runErr)
	}()
	_ = jobhistory.CopyFileTo(historyDir, absScriptPath, "script.py")

	fmt.Printf("=== Pipedpeer CLI ===\n")
	fmt.Printf("Script: %s\n", absScriptPath)
	fmt.Printf("Daemon: %s:%d\n\n", opts.DaemonHost, opts.DaemonPort)

	projectRoot := findProjectRoot(filepath.Dir(absScriptPath))

	fmt.Printf("[1/7] Detecting imports...\n")
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

	fmt.Printf("\n[2/7] Parsing dependencies...\n")
	nixPkgs := pythondeps.ResolvePackages(imports, opts.Pkgs)
	for _, pkg := range nixPkgs {
		fmt.Printf("      Using Nix package: %s\n", pkg)
	}

	tmpDir, err := os.MkdirTemp("", "pipedpeer-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("\n[3/7] Generating flake.nix...\n")
	flakeContent := nixgen.GenerateFlake(nixPkgs, opts.PythonVersion)
	flakePath := filepath.Join(tmpDir, "flake.nix")
	if err := os.WriteFile(flakePath, []byte(flakeContent), 0644); err != nil {
		return fmt.Errorf("failed to write flake.nix: %v", err)
	}
	_ = jobhistory.SaveText(historyDir, "flake.nix", flakeContent)
	fmt.Printf("      Created: %s\n", flakePath)

	fmt.Printf("\n[4/7] Building locally...\n")
	jobhistory.UpdateStage(historyDir, "building")
	nixSystem := nixgen.NixArch()
	cmd := exec.Command("nix", "build", ".#packages."+nixSystem+".default", "--option", "build-users-group", "")
	cmd.Dir = tmpDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("nix build failed: %v", err)
	}
	fmt.Printf("      Built successfully\n")

	fmt.Printf("\n[5/7] Exporting closure...\n")
	jobhistory.UpdateStage(historyDir, "exporting")
	resultPath := filepath.Join(tmpDir, "result")
	storePath, err := os.Readlink(resultPath)
	if err != nil {
		return fmt.Errorf("failed to readlink result: %v", err)
	}
	historyRecord.StorePath = storePath
	fmt.Printf("      Store path: %s\n", storePath)

	narPath := filepath.Join(tmpDir, "closure.nar")
	if err := daemonctl.ExportNAR(storePath, narPath); err != nil {
		return fmt.Errorf("nix store export failed: %v", err)
	}
	fmt.Printf("      Exported closure to %s\n", narPath)

	fmt.Printf("\n[6/7] Uploading to daemon...\n")
	jobhistory.UpdateStage(historyDir, "uploading")
	workspaceTar := filepath.Join(tmpDir, "workspace.tar")
	if err := daemonctl.CreateWorkspaceTar(projectRoot, workspaceTar); err != nil {
		return fmt.Errorf("workspace tar failed: %v", err)
	}

	scriptRelPath, err := filepath.Rel(projectRoot, absScriptPath)
	if err != nil {
		scriptRelPath = filepath.Base(absScriptPath)
	}
	fmt.Printf("      Project root: %s\n", projectRoot)
	fmt.Printf("      Script: %s\n", scriptRelPath)

	uploadResp, err := daemonctl.UploadJob(opts.DaemonHost, opts.DaemonPort, workspaceTar, narPath, storePath, scriptRelPath)
	if err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}
	fmt.Printf("      Uploaded job %s\n", uploadResp.JobID)

	fmt.Printf("\n[7/7] Executing on remote...\n")
	jobhistory.UpdateStage(historyDir, "executing")
	ctx := context.Background()
	execCfg := daemonctl.ExecConfig{
		ScriptPath: scriptRelPath,
		Args:       opts.ScriptArgs,
		Envs:       opts.Envs,
		Isolate:    opts.Isolate,
		StorePath:  storePath,
		GPU:        opts.GPU,
		GPUDevices: opts.GPUDevices,
	}

	if err := daemonctl.StreamExecute(ctx, opts.DaemonHost, opts.DaemonPort, uploadResp.JobID, execCfg); err != nil {
		return fmt.Errorf("execution failed: %v", err)
	}

	resultsDir := opts.ResultsDir
	if resultsDir == "" {
		resultsDir = projectRoot
	}
	manifest, err := daemonctl.DownloadResults(opts.DaemonHost, opts.DaemonPort, uploadResp.JobID, resultsDir)
	if err != nil {
		fmt.Printf("      Warning: results download failed: %v\n", err)
	} else {
		manifest.JobID = uploadResp.JobID
		manifestPath, writeErr := jobhistory.WriteManifest(historyDir, manifest)
		if writeErr != nil {
			fmt.Printf("      Warning: could not record manifest: %v\n", writeErr)
		}
		// Set on the record rather than on disk: the deferred Finalize writes
		// this same value out at the end of the run.
		historyRecord.LocalSyncRoot = resultsDir
		historyRecord.ReceivedFiles = manifest.Count()
		historyRecord.NewFiles = len(manifest.New)
		historyRecord.UpdatedFiles = len(manifest.Updated)
		historyRecord.ManifestPath = manifestPath
		switch {
		case manifest.Count() == 0:
			fmt.Printf("      No files changed on the remote\n")
		default:
			fmt.Printf("      Synced %d file(s) to %s (%d new, %d updated)\n",
				manifest.Count(), resultsDir, len(manifest.New), len(manifest.Updated))
		}
	}

	fmt.Printf("\n=== Done ===\n")
	fmt.Printf("History: %s\n", historyDir)
	return nil
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
