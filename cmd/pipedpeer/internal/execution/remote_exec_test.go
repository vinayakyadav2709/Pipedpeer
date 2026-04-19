package execution

import (
	"strings"
	"testing"
)

func TestBuildRunCommandIsolated(t *testing.T) {
	cmd := BuildRunCommand("/nix/store/abc/bin/run", "/tmp/pipedpeer/jobs/test", true, "script.py", nil, nil)
	if !strings.Contains(cmd, "bwrap") {
		t.Fatalf("expected bwrap in isolated command")
	}
}

func TestBuildRunCommandNonIsolated(t *testing.T) {
	cmd := BuildRunCommand("/nix/store/abc/bin/run", "/tmp/pipedpeer/jobs/test", false, "script.py", nil, nil)
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

func TestBuildRunCommandWithScriptArgs(t *testing.T) {
	cmd := BuildRunCommand("/nix/store/abc/bin/run", "/tmp/pipedpeer/jobs/test", false, "train.py", []string{"--epochs", "10", "--lr", "0.001"}, nil)
	if !strings.Contains(cmd, "'--epochs'") {
		t.Fatalf("expected --epochs in command, got: %s", cmd)
	}
	if !strings.Contains(cmd, "'10'") {
		t.Fatalf("expected 10 in command, got: %s", cmd)
	}
	if !strings.Contains(cmd, "'--lr'") {
		t.Fatalf("expected --lr in command, got: %s", cmd)
	}
	if !strings.Contains(cmd, "'0.001'") {
		t.Fatalf("expected 0.001 in command, got: %s", cmd)
	}
}

func TestBuildRunCommandWithEnvVars(t *testing.T) {
	cmd := BuildRunCommand("/nix/store/abc/bin/run", "/tmp/pipedpeer/jobs/test", true, "script.py", nil, []string{"API_KEY=secret123", "DEBUG=1"})
	if !strings.Contains(cmd, "API_KEY") || !strings.Contains(cmd, "secret123") {
		t.Fatalf("expected API_KEY=secret123 in bwrap env, got: %s", cmd)
	}
	if !strings.Contains(cmd, "DEBUG") || !strings.Contains(cmd, "'1'") {
		t.Fatalf("expected DEBUG=1 in bwrap env, got: %s", cmd)
	}
}
