package daemonapi

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// The job learns its node id and the tightest CPU quota on its cgroup chain.
func TestJobInfoEnv(t *testing.T) {
	root := t.TempDir()
	write := func(rel, content string) {
		dir := filepath.Join(root, rel)
		os.MkdirAll(dir, 0o755)
		os.WriteFile(filepath.Join(dir, "cpu.max"), []byte(content), 0o644)
	}
	write("user.slice", "400000 100000\n")
	write("user.slice/app.scope", "150000 100000\n")
	write("user.slice/app.scope/daemon", "max 100000\n")

	cores := cpuLimitCores(root, "/user.slice/app.scope/daemon")
	if cores != 1.5 {
		t.Fatalf("cores = %v, want 1.5 (the scope's quota, tighter than the slice's)", cores)
	}
	env := jobInfoEnv(nil, "node-abc", cores)
	for _, want := range []string{"PIPEDPEER_NODE_ID=node-abc", "PIPEDPEER_CPU_LIMIT=1.5"} {
		if !slices.Contains(env, want) {
			t.Errorf("env %v lacks %s", env, want)
		}
	}

	if c := cpuLimitCores(root, "/other.slice"); c != 0 {
		t.Errorf("unlimited chain cores = %v, want 0", c)
	}
	env = jobInfoEnv([]string{"PIPEDPEER_NODE_ID=mine"}, "node-abc", 0)
	if len(env) != 0 {
		t.Errorf("env = %v, want nothing (submitter's id kept, no limit omitted)", env)
	}
}
