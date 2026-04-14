package app

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

type DetachedSyncWorkerOptions struct {
	User       string
	Host       string
	Port       int
	JobDir     string
	LocalRoot  string
	HistoryDir string
	TimeoutSec int
}

func StartDetachedSyncWorker(opts DetachedSyncWorkerOptions) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	if opts.TimeoutSec <= 0 {
		opts.TimeoutSec = 12 * 60 * 60
	}

	cmd := exec.Command(exe,
		"__sync_job__",
		"--user", opts.User,
		"--host", opts.Host,
		"--port", strconv.Itoa(opts.Port),
		"--job-dir", opts.JobDir,
		"--local-root", opts.LocalRoot,
		"--history-dir", opts.HistoryDir,
		"--timeout-sec", strconv.Itoa(opts.TimeoutSec),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	_ = cmd.Process.Release()
	return nil
}

func RunDetachedSyncWorker(opts DetachedSyncWorkerOptions) error {
	record, err := jobhistory.ReadRecordAt(opts.HistoryDir)
	if err != nil {
		return err
	}

	record.Status = "running_detached"
	_ = jobhistory.SaveRecord(opts.HistoryDir, record)

	if err := waitForRemoteDone(opts.User, opts.Host, opts.Port, opts.JobDir, time.Duration(opts.TimeoutSec)*time.Second); err != nil {
		_ = jobhistory.Finalize(opts.HistoryDir, record, err)
		return err
	}

	exitCode, err := readRemoteExitCode(opts.User, opts.Host, opts.Port, opts.JobDir)
	if err != nil {
		_ = jobhistory.Finalize(opts.HistoryDir, record, err)
		return err
	}

	stdout, _ := readRemoteText(opts.User, opts.Host, opts.Port, filepath.Join(opts.JobDir, "stdout.log"))
	stderr, _ := readRemoteText(opts.User, opts.Host, opts.Port, filepath.Join(opts.JobDir, "stderr.log"))
	_ = jobhistory.SaveText(opts.HistoryDir, "stdout.log", stdout)
	_ = jobhistory.SaveText(opts.HistoryDir, "stderr.log", stderr)

	syncSummary, syncErr := receiveAndSyncOutputs(opts.User, opts.Host, opts.Port, opts.JobDir, opts.LocalRoot, opts.HistoryDir)
	if syncErr == nil {
		record.LocalSyncRoot = opts.LocalRoot
		record.ReceivedFiles = syncSummary.Total
		record.NewFiles = syncSummary.New
		record.UpdatedFiles = syncSummary.Updated
		record.UnchangedFiles = syncSummary.Unchanged
		record.ManifestPath = syncSummary.ManifestPath
		_ = jobhistory.SaveRecord(opts.HistoryDir, record)
	}

	if exitCode != 0 {
		err = fmt.Errorf("detached remote job failed with exit code %d", exitCode)
		_ = jobhistory.Finalize(opts.HistoryDir, record, err)
		return err
	}
	if syncErr != nil {
		_ = jobhistory.Finalize(opts.HistoryDir, record, syncErr)
		return syncErr
	}

	return jobhistory.Finalize(opts.HistoryDir, record, nil)
}

func waitForRemoteDone(user, host string, port int, jobDir string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok, err := remoteTest(user, host, port, "[ -f "+shellQuote(filepath.Join(jobDir, "done"))+" ]")
		if err == nil && ok {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout waiting for detached job completion")
}

func readRemoteExitCode(user, host string, port int, jobDir string) (int, error) {
	out, err := remoteOutput(user, host, port, "cat "+shellQuote(filepath.Join(jobDir, "exit_code"))+" 2>/dev/null || echo 1")
	if err != nil {
		return 1, err
	}
	out = strings.TrimSpace(out)
	code, convErr := strconv.Atoi(out)
	if convErr != nil {
		return 1, fmt.Errorf("invalid detached exit code: %q", out)
	}
	return code, nil
}

func readRemoteText(user, host string, port int, remotePath string) (string, error) {
	return remoteOutput(user, host, port, "cat "+shellQuote(remotePath)+" 2>/dev/null || true")
}

func remoteTest(user, host string, port int, shCmd string) (bool, error) {
	cmd := exec.Command("ssh", "-p", strconv.Itoa(port), fmt.Sprintf("%s@%s", user, host), "sh", "-lc", shCmd)
	err := cmd.Run()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			if ee.ExitCode() == 1 {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

func remoteOutput(user, host string, port int, shCmd string) (string, error) {
	cmd := exec.Command("ssh", "-p", strconv.Itoa(port), fmt.Sprintf("%s@%s", user, host), "sh", "-lc", shCmd)
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err := cmd.Run()
	if err != nil {
		msg := strings.TrimSpace(errOut.String())
		if msg != "" {
			return "", fmt.Errorf("%w: %s", err, msg)
		}
		return "", err
	}
	return out.String(), nil
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}
