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

// E6: a 2-CPU quota on a 16-core host ran numpy's BLAS with 16 threads. The
// pools are capped at the limit, a value already set is kept, and an
// unlimited job (or one allowed every core) gets nothing.
func TestBLASThreadsFollowTheJobsCPULimit(t *testing.T) {
	env := blasThreadEnv([]string{"MKL_NUM_THREADS=1"}, 1.5, 16)
	if !slices.Equal(env, []string{"OMP_NUM_THREADS=2", "OPENBLAS_NUM_THREADS=2"}) {
		t.Errorf("capped job: %v", env)
	}
	if env := blasThreadEnv(nil, 0, 16); env != nil {
		t.Errorf("unlimited job: %v", env)
	}
	if env := blasThreadEnv(nil, 16, 16); env != nil {
		t.Errorf("limit = every core: %v", env)
	}
}
