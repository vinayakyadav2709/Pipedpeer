package execution

import (
	"strings"
	"testing"
)

func TestBuildRunCommandIsolated(t *testing.T) {
	cmd := BuildRunCommand("/nix/store/abc/bin/run", "/tmp/pipedpeer/jobs/test", true)
	if !strings.Contains(cmd, "bwrap") {
		t.Fatalf("expected bwrap in isolated command")
	}
	if !strings.Contains(cmd, "--unshare-net") {
		t.Fatalf("expected network namespace isolation")
	}
}

func TestBuildRunCommandNonIsolated(t *testing.T) {
	cmd := BuildRunCommand("/nix/store/abc/bin/run", "/tmp/pipedpeer/jobs/test", false)
	if strings.Contains(cmd, "bwrap") {
		t.Fatalf("did not expect bwrap in non-isolated command")
	}
	if !strings.Contains(cmd, "mkdir -p") || !strings.Contains(cmd, "cd") {
		t.Fatalf("expected simple workdir setup command")
	}
}

func TestBuildDetachedRemoteCommand(t *testing.T) {
	remoteCmd := BuildDetachedRemoteCommand("echo hi", "/tmp/pipedpeer/jobs/test", "/nix/store/path", "job-1")
	for _, token := range []string{"nohup", "JOB_NAME", "STORE_PATH", "DONE_PATH", "EXIT_CODE_PATH", "STDOUT", "STDERR"} {
		if !strings.Contains(remoteCmd, token) {
			t.Fatalf("expected token %q in remote command", token)
		}
	}
}
