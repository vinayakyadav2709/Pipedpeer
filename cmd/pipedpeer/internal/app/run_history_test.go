package app

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunCreatesJobHistoryArtifacts(t *testing.T) {
	tmp := t.TempDir()
	mockBin := filepath.Join(tmp, "mockbin")
	if err := os.MkdirAll(mockBin, 0755); err != nil {
		t.Fatalf("mkdir mockbin: %v", err)
	}

	nixScript := `#!/bin/sh
set -eu
if [ "$1" = "build" ]; then
  store="$PWD/mock-store"
  mkdir -p "$store/bin"
  cat > "$store/bin/run" <<'EOF'
#!/bin/sh
echo remote-run-ok
EOF
  chmod +x "$store/bin/run"
  ln -sfn "$store" "$PWD/result"
  exit 0
fi
if [ "$1" = "copy" ]; then
  exit 0
fi
echo "unexpected nix args: $*" >&2
exit 1
`
	if err := writeExec(filepath.Join(mockBin, "nix"), nixScript); err != nil {
		t.Fatalf("write mock nix: %v", err)
	}

	sshScript := `#!/bin/sh
set -eu
args="$*"
if printf '%s' "$args" | grep -q 'tar -C'; then
	tmpd="$(mktemp -d)"
	mkdir -p "$tmpd/models"
	echo 'tree-model-bytes' > "$tmpd/models/model.save"
	tar -C "$tmpd" -cf - .
	rm -rf "$tmpd"
	exit 0
fi
echo remote-stdout-line
echo remote-stderr-line >&2
`
	if err := writeExec(filepath.Join(mockBin, "ssh"), sshScript); err != nil {
		t.Fatalf("write mock ssh: %v", err)
	}

	script := filepath.Join(tmp, "train.py")
	content := "import numpy\nfrom sklearn.tree import DecisionTreeClassifier\nfrom joblib import dump\n\nX = [[0], [1], [2], [3]]\ny = [0, 0, 1, 1]\nmodel = DecisionTreeClassifier(max_depth=2)\nmodel.fit(X, y)\ndump(model, 'models/model.save')\nprint('trained tree saved')\n"
	if err := os.WriteFile(script, []byte(content), 0644); err != nil {
		t.Fatalf("write script: %v", err)
	}

	xdg := filepath.Join(tmp, "xdg")
	t.Setenv("XDG_DATA_HOME", xdg)
	t.Setenv("PATH", mockBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	err := Run(Options{
		ScriptPath: script,
		Remote:     "root@localhost:2221",
		TargetID:   "node-test-1",
		Detach:     false,
		Isolate:    true,
	})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	jobsDir := filepath.Join(xdg, "pipedpeer", "jobs")
	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		t.Fatalf("read jobs dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 job directory, got %d", len(entries))
	}

	jobDir := filepath.Join(jobsDir, entries[0].Name())

	mustExist := []string{"metadata.json", "script.py", "flake.nix", "run_command.sh", "stdout.log", "stderr.log", "received_manifest.json"}
	for _, f := range mustExist {
		if _, err := os.Stat(filepath.Join(jobDir, f)); err != nil {
			t.Fatalf("expected file %s: %v", f, err)
		}
	}

	if _, err := os.Stat(filepath.Join(jobDir, "received", "models", "model.save")); err != nil {
		t.Fatalf("expected received output artifact copy: %v", err)
	}

	if _, err := os.Stat(filepath.Join(tmp, "models", "model.save")); err != nil {
		t.Fatalf("expected synced local model file in workspace: %v", err)
	}

	metaRaw, err := os.ReadFile(filepath.Join(jobDir, "metadata.json"))
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	var meta map[string]any
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}

	if got := meta["status"]; got != "succeeded" {
		t.Fatalf("expected status succeeded, got %v", got)
	}
	if got := meta["target_id"]; got != "node-test-1" {
		t.Fatalf("expected target_id node-test-1, got %v", got)
	}
	if got := meta["received_files"]; got == nil {
		t.Fatalf("expected received_files in metadata")
	}

	stdoutRaw, err := os.ReadFile(filepath.Join(jobDir, "stdout.log"))
	if err != nil {
		t.Fatalf("read stdout.log: %v", err)
	}
	stderrRaw, err := os.ReadFile(filepath.Join(jobDir, "stderr.log"))
	if err != nil {
		t.Fatalf("read stderr.log: %v", err)
	}
	if !strings.Contains(string(stdoutRaw), "remote-stdout-line") {
		t.Fatalf("stdout.log missing expected line")
	}
	if !strings.Contains(string(stderrRaw), "remote-stderr-line") {
		t.Fatalf("stderr.log missing expected line")
	}
}

func writeExec(path, content string) error {
	if err := os.WriteFile(path, []byte(content), 0755); err != nil {
		return err
	}
	return os.Chmod(path, 0755)
}
