package app

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/execution"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/logging"
	"github.com/pipedpeer/pipedpeer/internal/nixgen"
	"github.com/pipedpeer/pipedpeer/internal/pythondeps"
	"github.com/pipedpeer/pipedpeer/internal/remote"
)

type Options struct {
	ScriptPath    string
	Remote        string
	TargetID      string
	Detach        bool
	JobName       string
	Isolate       bool
	Mode          string
	PythonVersion string
	Envs          []string
	Pkgs          []string
	ScriptArgs    []string
	// Coordinator placement diagnostics (populated when --remote is omitted)
	PlacementSource  string
	DegradedMode     bool
	CandidateCount   int
	PlacementReason  string
	// Resource estimation
	EstimatedMemBytes int64
	EstimationTier    string
}

func Run(opts Options) (runErr error) {
	sshCfg, err := remote.Parse(opts.Remote)
	if err != nil {
		return fmt.Errorf("Failed to parse remote: %v", err)
	}

	if _, err := os.Stat(opts.ScriptPath); os.IsNotExist(err) {
		return fmt.Errorf("File not found: %s", opts.ScriptPath)
	}

	absScriptPath, err := filepath.Abs(opts.ScriptPath)
	if err != nil {
		return fmt.Errorf("Failed to get absolute path: %v", err)
	}

	resolvedJobName := opts.JobName
	if resolvedJobName == "" {
		resolvedJobName = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}

	historyRecord, historyDir, err := jobhistory.NewRecord(absScriptPath, opts.Remote, opts.TargetID, opts.Detach, opts.Isolate)
	if err != nil {
		return fmt.Errorf("Failed to initialize job history: %v", err)
	}
	historyRecord.JobName = resolvedJobName
	historyRecord.RunHost = sshCfg.Host
	// Coordinator placement diagnostics
	historyRecord.PlacementSource = opts.PlacementSource
	historyRecord.DegradedMode = opts.DegradedMode
	historyRecord.CandidateCount = opts.CandidateCount
	historyRecord.PlacementReason = opts.PlacementReason
	// Resource estimation
	historyRecord.EstimatedMemBytes = opts.EstimatedMemBytes
	historyRecord.EstimationTier = opts.EstimationTier
	_ = jobhistory.SaveRecord(historyDir, historyRecord)
	defer func() {
		if opts.Detach {
			return
		}
		_ = jobhistory.Finalize(historyDir, historyRecord, runErr)
	}()
	_ = jobhistory.CopyFileTo(historyDir, absScriptPath, "script.py")

	fmt.Printf("=== Pipedpeer CLI ===\n")
	fmt.Printf("Script: %s\n", absScriptPath)
	fmt.Printf("Remote: %s@%s:%d\n\n", sshCfg.User, sshCfg.Host, sshCfg.Port)

	fmt.Printf("[1/6] Detecting imports...\n")
	importScan := pythondeps.ExtractImportScan(absScriptPath)
	imports := importScan.ExternalDeps
	if len(imports) > 0 {
		fmt.Printf("      Found external imports: %s\n", strings.Join(imports, ", "))
	}
	if len(importScan.LocalFiles) > 0 {
		fmt.Printf("      Found local imports: %d (will be bundled into runtime artifact)\n", len(importScan.LocalFiles))
	}

	fmt.Printf("\n[2/6] Parsing dependencies...\n")
	nixPkgs := pythondeps.ResolvePackages(imports, opts.Pkgs)
	for _, pkg := range nixPkgs {
		fmt.Printf("      Using Nix package: %s\n", pkg)
	}

	tmpDir, err := os.MkdirTemp("", "pipedpeer-*")
	if err != nil {
		return fmt.Errorf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("\n[3/6] Generating flake.nix...\n")
	flakeContent := nixgen.GenerateFlake(nixPkgs, opts.PythonVersion)
	flakePath := filepath.Join(tmpDir, "flake.nix")
	if err := os.WriteFile(flakePath, []byte(flakeContent), 0644); err != nil {
		return fmt.Errorf("Failed to write flake.nix: %v", err)
	}
	_ = jobhistory.SaveText(historyDir, "flake.nix", flakeContent)
	fmt.Printf("      Created: %s\n", flakePath)

	fmt.Printf("\n[4/6] Building locally...\n")
	nixSystem := nixgen.NixArch()
	cmd := exec.Command("nix", "build", ".#packages."+nixSystem+".default", "--option", "build-users-group", "")
	cmd.Dir = tmpDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("nix build failed: %v", err)
	}
	fmt.Printf("      Built successfully\n")

	fmt.Printf("\n[5/6] Getting store path...\n")
	resultPath := filepath.Join(tmpDir, "result")
	storePath, err := os.Readlink(resultPath)
	if err != nil {
		return fmt.Errorf("Failed to readlink result: %v", err)
	}
	historyRecord.StorePath = storePath
	fmt.Printf("      Store path: %s\n", storePath)

	fmt.Printf("\n[6/6] Copying to remote...\n")
	sshDest := fmt.Sprintf("ssh://%s@%s:%d", sshCfg.User, sshCfg.Host, sshCfg.Port)
	cmd = exec.Command("nix", "copy", "--to", sshDest, storePath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("nix copy failed: %v", err)
	}
	fmt.Printf("      Copied successfully\n")
	fmt.Printf("      Note: this node reuses its Nix store cache across jobs\n")

	fmt.Printf("\n[7/8] Syncing workspace to remote...\n")
	jobDir := filepath.Join("/tmp/pipedpeer/jobs", resolvedJobName)
	historyRecord.RemoteJobDir = jobDir
	_ = jobhistory.SaveRecord(historyDir, historyRecord)

	localPwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("Failed to get pwd: %v", err)
	}
	scriptRelPath, err := filepath.Rel(localPwd, absScriptPath)
	if err != nil {
		scriptRelPath = filepath.Base(absScriptPath)
	}

	remoteWorkDir := filepath.Join(jobDir, "work")
	syncCmd := fmt.Sprintf("mkdir -p %s && tar -C %s -xf -", execution.ShellQuote(remoteWorkDir), execution.ShellQuote(remoteWorkDir))
	sshOpts := []string{"-o", "StrictHostKeyChecking=accept-new"}
	
	tarArgs := []string{"--exclude=.git", "--exclude=__pycache__", "--exclude=.venv", "--exclude=venv", "--exclude=env", "--exclude=node_modules"}
	ignoreFile := filepath.Join(localPwd, ".pipedpeerignore")
	if content, err := os.ReadFile(ignoreFile); err == nil {
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") {
				tarArgs = append(tarArgs, "--exclude="+line)
			}
		}
	}
	tarArgs = append(tarArgs, "-cf", "-", ".")
	tarCmd := exec.Command("tar", tarArgs...)
	
	tarCmd.Dir = localPwd
	sshSyncArgs := append(sshOpts, "-p", fmt.Sprintf("%d", sshCfg.Port), fmt.Sprintf("%s@%s", sshCfg.User, sshCfg.Host), syncCmd)
	sshSyncCmd := exec.Command("ssh", sshSyncArgs...)

	pipe, err := tarCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("Failed to create tar pipe: %v", err)
	}
	sshSyncCmd.Stdin = pipe

	if err := sshSyncCmd.Start(); err != nil {
		return fmt.Errorf("Failed to start ssh sync: %v", err)
	}
	if err := tarCmd.Run(); err != nil {
		return fmt.Errorf("Failed to run tar: %v", err)
	}
	if err := sshSyncCmd.Wait(); err != nil {
		return fmt.Errorf("Failed to finish ssh sync: %v", err)
	}
	fmt.Printf("      Workspace synced to %s\n", remoteWorkDir)

	fmt.Printf("\n[8/8] Executing on remote...\n")
	runPath := filepath.Join(storePath, "bin", "run")
	historyRecord.RunPath = runPath

	runCmd := execution.BuildRunCommand(runPath, jobDir, opts.Isolate, scriptRelPath, opts.ScriptArgs, opts.Envs)
	_ = jobhistory.SaveText(historyDir, "run_command.sh", runCmd)

	if opts.Detach {
		remoteCmd := execution.BuildDetachedRemoteCommand(runCmd, jobDir, storePath, resolvedJobName)
		cmd = exec.Command("ssh", append(sshOpts, "-p", fmt.Sprintf("%d", sshCfg.Port), fmt.Sprintf("%s@%s", sshCfg.User, sshCfg.Host), remoteCmd)...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("Detached execution failed: %v", err)
		}
		fmt.Printf("      Submitted detached job: %s\n", resolvedJobName)
		if opts.Isolate {
			fmt.Printf("      Sandbox: enabled (isolated network/filesystem namespace)\n")
		} else {
			fmt.Printf("      Sandbox: disabled\n")
		}
		_ = jobhistory.SaveText(historyDir, "remote_logs.txt", fmt.Sprintf("stdout: %s/stdout.log\nstderr: %s/stderr.log\n", jobDir, jobDir))
		historyRecord.Status = "running_detached"
		_ = jobhistory.SaveRecord(historyDir, historyRecord)

		if err := StartDetachedSyncWorker(DetachedSyncWorkerOptions{
			User:       sshCfg.User,
			Host:       sshCfg.Host,
			Port:       sshCfg.Port,
			JobDir:     jobDir,
			LocalRoot:  localPwd,
			HistoryDir: historyDir,
		}); err != nil {
			return fmt.Errorf("Failed to start detached sync worker: %v", err)
		}
		fmt.Printf("      Detached sync worker started (job history will update asynchronously)\n")
		fmt.Printf("      Tip: submit more jobs to this same node with --detach\n")
	} else {
		cmd = exec.Command("ssh", append(sshOpts, "-t", "-p", fmt.Sprintf("%d", sshCfg.Port), fmt.Sprintf("%s@%s", sshCfg.User, sshCfg.Host), runCmd)...)
		var stdoutBuf bytes.Buffer
		var stderrBuf bytes.Buffer
		cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)
		cmd.Stdin = os.Stdin

		if err := cmd.Start(); err != nil {
			return fmt.Errorf("Failed to start remote execution: %v", err)
		}

		// Wait for execution or interrupt
		errCh := make(chan error, 1)
		go func() {
			errCh <- cmd.Wait()
		}()

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt)

		select {
		case err := <-errCh:
			_ = jobhistory.SaveText(historyDir, "stdout.log", stdoutBuf.String())
			_ = jobhistory.SaveText(historyDir, "stderr.log", stderrBuf.String())
			if err != nil {
				fmt.Printf("\n      Execution finished with error: %v\n", err)
			}
		case <-sigCh:
			fmt.Printf("\n      Caught interrupt, terminating remote execution...\n")
			if cmd.Process != nil {
				_ = cmd.Process.Signal(os.Interrupt)
			}
			<-errCh
			_ = jobhistory.SaveText(historyDir, "stdout.log", stdoutBuf.String())
			_ = jobhistory.SaveText(historyDir, "stderr.log", stderrBuf.String())
		}
		signal.Stop(sigCh)

		syncSummary, err := receiveAndSyncOutputs(sshCfg.User, sshCfg.Host, sshCfg.Port, jobDir, localPwd, historyDir)
		if err != nil {
			log := logging.WithComponent("sync")
			log.Warn().Err(err).Msg("failed to receive output files")
		} else {
			historyRecord.LocalSyncRoot = localPwd
			historyRecord.ReceivedFiles = syncSummary.Total
			historyRecord.NewFiles = syncSummary.New
			historyRecord.UpdatedFiles = syncSummary.Updated
			historyRecord.UnchangedFiles = syncSummary.Unchanged
			historyRecord.ManifestPath = syncSummary.ManifestPath
			_ = jobhistory.SaveRecord(historyDir, historyRecord)
			fmt.Printf("      Synced output files: total=%d new=%d updated=%d unchanged=%d\n", syncSummary.Total, syncSummary.New, syncSummary.Updated, syncSummary.Unchanged)
		}
	}

	fmt.Printf("\n=== Done ===\n")
	fmt.Printf("History: %s\n", historyDir)
	return nil
}

type syncEntry struct {
	Path    string `json:"path"`
	Status  string `json:"status"`
	OldHash string `json:"old_hash,omitempty"`
	NewHash string `json:"new_hash"`
	Size    int64  `json:"size"`
}

type syncSummary struct {
	Total        int    `json:"total"`
	New          int    `json:"new"`
	Updated      int    `json:"updated"`
	Unchanged    int    `json:"unchanged"`
	ManifestPath string `json:"manifest_path"`
}

func receiveAndSyncOutputs(user, host string, port int, jobDir, localRoot, historyDir string) (syncSummary, error) {
	remoteWorkDir := filepath.Join(jobDir, "work")
	remoteCmd := "if [ -d " + execution.ShellQuote(remoteWorkDir) + " ]; then tar -C " + execution.ShellQuote(remoteWorkDir) + " -cf - .; fi"

	cmd := exec.Command("ssh", "-o", "StrictHostKeyChecking=accept-new", "-p", fmt.Sprintf("%d", port), fmt.Sprintf("%s@%s", user, host), remoteCmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return syncSummary{}, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return syncSummary{}, err
	}

	receivedDir := filepath.Join(historyDir, "received")
	if err := os.MkdirAll(receivedDir, 0755); err != nil {
		return syncSummary{}, err
	}

	entries := []syncEntry{}
	tr := tar.NewReader(stdout)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = cmd.Wait()
			return syncSummary{}, err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		rel := filepath.Clean(hdr.Name)
		rel = filepath.ToSlash(rel)
		if rel == "." || strings.HasPrefix(rel, "../") {
			continue
		}

		content, err := io.ReadAll(tr)
		if err != nil {
			_ = cmd.Wait()
			return syncSummary{}, err
		}

		dst := filepath.Join(localRoot, filepath.FromSlash(rel))
		historyCopy := filepath.Join(receivedDir, filepath.FromSlash(rel))

		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			_ = cmd.Wait()
			return syncSummary{}, err
		}
		if err := os.MkdirAll(filepath.Dir(historyCopy), 0755); err != nil {
			_ = cmd.Wait()
			return syncSummary{}, err
		}

		oldHash := ""
		status := "new"
		if prev, err := os.ReadFile(dst); err == nil {
			oldHash = hashBytes(prev)
			status = "updated"
		}

		newHash := hashBytes(content)
		if oldHash == newHash && oldHash != "" {
			status = "unchanged"
		}

		if err := os.WriteFile(dst, content, 0644); err != nil {
			_ = cmd.Wait()
			return syncSummary{}, err
		}
		if err := os.WriteFile(historyCopy, content, 0644); err != nil {
			_ = cmd.Wait()
			return syncSummary{}, err
		}

		entries = append(entries, syncEntry{Path: rel, Status: status, OldHash: oldHash, NewHash: newHash, Size: int64(len(content))})
	}

	if err := cmd.Wait(); err != nil {
		return syncSummary{}, fmt.Errorf("remote tar fetch failed: %v (%s)", err, strings.TrimSpace(stderr.String()))
	}

	summary := syncSummary{}
	for _, e := range entries {
		summary.Total++
		switch e.Status {
		case "new":
			summary.New++
		case "updated":
			summary.Updated++
		case "unchanged":
			summary.Unchanged++
		}
	}

	manifestPath := filepath.Join(historyDir, "received_manifest.json")
	manifest := map[string]any{"entries": entries, "summary": summary}
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return syncSummary{}, err
	}
	if err := os.WriteFile(manifestPath, b, 0644); err != nil {
		return syncSummary{}, err
	}
	summary.ManifestPath = manifestPath
	return summary, nil
}

func hashBytes(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
