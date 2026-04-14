package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestConcurrentJobsAndSharedNumpyCache(t *testing.T) {
	if os.Getenv("PIPEDPEER_INTEGRATION") != "1" {
		t.Skip("set PIPEDPEER_INTEGRATION=1 to run integration tests")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is required for integration test")
	}

	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("failed to resolve repo root: %v", err)
	}
	labDir := filepath.Join(repoRoot, "lab")

	ctx := context.Background()

	runCmd(t, ctx, labDir, "docker", "compose", "up", "-d", "--build", "worker-1")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		cmd := exec.CommandContext(cleanupCtx, "docker", "compose", "down")
		cmd.Dir = labDir
		_ = cmd.Run()
	})

	setupScript := `
set -eu
rm -rf /tmp/pipedpeer-it
mkdir -p /tmp/pipedpeer-it/job1 /tmp/pipedpeer-it/job2

cat > /tmp/pipedpeer-it/job1/main.py <<'PY'
import numpy as np
import time
print(np.arange(3).sum())
time.sleep(2)
PY

cat > /tmp/pipedpeer-it/job2/main.py <<'PY'
import numpy as np
import pandas as pd
import time
print(np.arange(3).sum(), pd.Series([1,2,3]).sum())
time.sleep(2)
PY

cat > /tmp/pipedpeer-it/job1/flake.nix <<'NIX'
{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";

  outputs = { self, nixpkgs }: {
    packages.x86_64-linux.default =
      let
        pkgs = nixpkgs.legacyPackages.x86_64-linux;
        python = pkgs.python3.withPackages (ps: [
          ps.numpy
        ]);
      in
      pkgs.writeShellScriptBin "run" ''
        ${python}/bin/python3 ${./main.py}
      '';
  };
}
NIX

cat > /tmp/pipedpeer-it/job2/flake.nix <<'NIX'
{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";

  outputs = { self, nixpkgs }: {
    packages.x86_64-linux.default =
      let
        pkgs = nixpkgs.legacyPackages.x86_64-linux;
        python = pkgs.python3.withPackages (ps: [
          ps.numpy
          ps.pandas
        ]);
      in
      pkgs.writeShellScriptBin "run" ''
        ${python}/bin/python3 ${./main.py}
      '';
  };
}
NIX

cat > /tmp/pipedpeer-it/run-job.sh <<'SH'
#!/bin/sh
set -eu
job_dir="$1"

start_ns=$(date +%s%N)
echo "$start_ns" > "$job_dir/started_at_ns"

store_path=$(cat "$job_dir/store_path")

mkdir -p "$job_dir/work" "$job_dir/home"

bwrap \
  --die-with-parent \
  --unshare-net \
  --unshare-pid \
  --unshare-ipc \
  --unshare-uts \
  --ro-bind /nix /nix \
  --dev /dev \
  --proc /proc \
  --tmpfs /tmp \
  --bind "$job_dir/work" /work \
  --bind "$job_dir/home" /home/root \
  --chdir /work \
  --setenv HOME /home/root \
  --setenv PATH /nix/var/nix/profiles/default/bin:/nix/var/nix/profiles/default/sbin:/root/.nix-profile/bin \
  -- "$store_path/bin/run" >"$job_dir/stdout.log" 2>"$job_dir/stderr.log"

end_ns=$(date +%s%N)
echo "$end_ns" > "$job_dir/finished_at_ns"
echo done > "$job_dir/done"
SH
chmod +x /tmp/pipedpeer-it/run-job.sh

cat > /tmp/pipedpeer-it/build-job.sh <<'SH'
#!/bin/sh
set -eu
job_dir="$1"

cd "$job_dir"
nix build .#packages.x86_64-linux.default --option build-users-group "" >/dev/null
store_path=$(readlink result)
echo "$store_path" > "$job_dir/store_path"
SH
chmod +x /tmp/pipedpeer-it/build-job.sh
`

	dockerExec(t, ctx, labDir, setupScript)
	dockerExec(t, ctx, labDir, "/tmp/pipedpeer-it/build-job.sh /tmp/pipedpeer-it/job1")
	dockerExec(t, ctx, labDir, "/tmp/pipedpeer-it/build-job.sh /tmp/pipedpeer-it/job2")

	storePath1 := strings.TrimSpace(dockerExec(t, ctx, labDir, "cat /tmp/pipedpeer-it/job1/store_path"))
	storePath2 := strings.TrimSpace(dockerExec(t, ctx, labDir, "cat /tmp/pipedpeer-it/job2/store_path"))
	if storePath1 == "" || storePath2 == "" {
		t.Fatalf("missing store paths")
	}

	numpyPath1 := strings.TrimSpace(dockerExec(t, ctx, labDir, "nix-store -qR "+shellEscape(storePath1)+" | grep numpy | head -n1"))
	numpyPath2 := strings.TrimSpace(dockerExec(t, ctx, labDir, "nix-store -qR "+shellEscape(storePath2)+" | grep numpy | head -n1"))
	if numpyPath1 == "" || numpyPath2 == "" {
		t.Fatalf("expected numpy in both closures")
	}
	if numpyPath1 != numpyPath2 {
		t.Fatalf("expected shared numpy path, got different paths\njob1: %s\njob2: %s", numpyPath1, numpyPath2)
	}

	pandasPath2 := strings.TrimSpace(dockerExec(t, ctx, labDir, "nix-store -qR "+shellEscape(storePath2)+" | grep pandas | head -n1"))
	if pandasPath2 == "" {
		t.Fatalf("expected pandas in job2 closure")
	}

	type jobResult struct {
		job string
		out string
		err error
	}

	results := make(chan jobResult, 2)
	jobCtx, cancelJobs := context.WithTimeout(context.Background(), time.Minute)
	defer cancelJobs()
	for _, job := range []string{"job1", "job2"} {
		job := job
		go func() {
			out, err := dockerExecE(jobCtx, labDir, "/tmp/pipedpeer-it/run-job.sh /tmp/pipedpeer-it/"+job)
			results <- jobResult{job: job, out: out, err: err}
		}()
	}

	for i := 0; i < 2; i++ {
		select {
		case res := <-results:
			if res.err != nil {
				t.Fatalf("%s failed: %v\noutput:\n%s\nstderr.log:\n%s",
					res.job,
					res.err,
					res.out,
					dockerExec(t, ctx, labDir, "cat /tmp/pipedpeer-it/"+res.job+"/stderr.log 2>/dev/null || true"),
				)
			}
		case <-jobCtx.Done():
			t.Fatalf("timed out waiting for job results within 1m\njob1 stderr:\n%s\njob2 stderr:\n%s\nprocesses:\n%s",
				dockerExec(t, ctx, labDir, "cat /tmp/pipedpeer-it/job1/stderr.log 2>/dev/null || true"),
				dockerExec(t, ctx, labDir, "cat /tmp/pipedpeer-it/job2/stderr.log 2>/dev/null || true"),
				dockerExec(t, ctx, labDir, "ps -ef | grep /tmp/pipedpeer-it | grep -v grep || true"),
			)
		}
	}

	start1, err := strconv.ParseInt(strings.TrimSpace(dockerExec(t, ctx, labDir, "cat /tmp/pipedpeer-it/job1/started_at_ns")), 10, 64)
	if err != nil {
		t.Fatalf("failed to parse job1 start timestamp: %v", err)
	}
	end1, err := strconv.ParseInt(strings.TrimSpace(dockerExec(t, ctx, labDir, "cat /tmp/pipedpeer-it/job1/finished_at_ns")), 10, 64)
	if err != nil {
		t.Fatalf("failed to parse job1 end timestamp: %v", err)
	}
	start2, err := strconv.ParseInt(strings.TrimSpace(dockerExec(t, ctx, labDir, "cat /tmp/pipedpeer-it/job2/started_at_ns")), 10, 64)
	if err != nil {
		t.Fatalf("failed to parse job2 start timestamp: %v", err)
	}
	end2, err := strconv.ParseInt(strings.TrimSpace(dockerExec(t, ctx, labDir, "cat /tmp/pipedpeer-it/job2/finished_at_ns")), 10, 64)
	if err != nil {
		t.Fatalf("failed to parse job2 end timestamp: %v", err)
	}

	if !(start1 < end2 && start2 < end1) {
		t.Fatalf("jobs did not overlap in time: job1=[%d,%d], job2=[%d,%d]", start1, end1, start2, end2)
	}

}

func runCmd(t *testing.T, ctx context.Context, dir string, name string, args ...string) string {
	t.Helper()
	out, err := runCmdE(ctx, dir, name, args...)
	if err != nil {
		t.Fatalf("command failed: %s %s\noutput:\n%s\nerror: %v", name, strings.Join(args, " "), out, err)
	}
	return out
}

func runCmdE(ctx context.Context, dir string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}

func dockerExec(t *testing.T, ctx context.Context, labDir, script string) string {
	t.Helper()
	out, err := dockerExecE(ctx, labDir, script)
	if err != nil {
		t.Fatalf("docker exec failed\nscript:\n%s\noutput:\n%s\nerror: %v", script, out, err)
	}
	return out
}

func dockerExecE(ctx context.Context, labDir, script string) (string, error) {
	return runCmdE(ctx, labDir, "docker", "compose", "exec", "-T", "worker-1", "sh", "-lc", script)
}

func shellEscape(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}
