package daemonapi

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestApplyMemLimit covers the only memory bound the kernel enforces.
//
// Everything else about memory here is a forecast: admission checks an
// estimate made before the job started, and the 40%-of-free-RAM chunk rule
// bounds one payload. Neither can stop a job that simply allocates more than
// it said it would, and until this existed nothing could — one job's runaway
// allocation was every other job on the node's problem.
//
// This test covers the decision and the shape of the config crun receives.
// Kernel enforcement is a property of the cgroup tree, not of this function;
// it is covered by TestPrepareEnforcesMemoryLimit in internal/cgroups, which
// skips itself when the machine has no delegated hierarchy.
func TestApplyMemLimit(t *testing.T) {
	base := func() ociConfig {
		return ociConfig{Linux: &ociLinux{
			Namespaces: []ociNamespace{{Type: "pid"}},
		}}
	}

	t.Run("applied when a limit is set and cgroups allow it", func(t *testing.T) {
		cfg := base()
		if !applyMemLimit(&cfg, 512<<20, "/user.slice/scope/pipedpeer", true, "job-1") {
			t.Fatal("limit not applied")
		}
		if cfg.Linux.Resources == nil || cfg.Linux.Resources.Memory == nil {
			t.Fatal("no memory resources in the bundle")
		}
		if got := cfg.Linux.Resources.Memory.Limit; got != 512<<20 {
			t.Errorf("limit = %d, want %d", got, 512<<20)
		}
		// Swap must match: otherwise a job over its cap swaps instead of
		// failing, and thrashing takes the node's other jobs with it.
		if got := cfg.Linux.Resources.Memory.Swap; got != 512<<20 {
			t.Errorf("swap = %d, want it pinned to the limit", got)
		}
		if want := "/user.slice/scope/pipedpeer/job-1"; cfg.Linux.CgroupsPath != want {
			t.Errorf("cgroupsPath = %q, want %q", cfg.Linux.CgroupsPath, want)
		}
	})

	t.Run("declined rather than failing when cgroups are unavailable", func(t *testing.T) {
		// The alternative is what shipped first: crun answers "open
		// `memory.max` for writing: No such file or directory" and the job
		// dies, which is a worse outcome than the unbounded memory the cap
		// was meant to prevent.
		cfg := base()
		if applyMemLimit(&cfg, 512<<20, "", false, "job-2") {
			t.Fatal("claimed to apply a limit with no delegated cgroup")
		}
		if cfg.Linux.Resources != nil || cfg.Linux.CgroupsPath != "" {
			t.Error("bundle carries cgroup settings crun cannot honour")
		}
	})

	t.Run("no limit means no resources block", func(t *testing.T) {
		cfg := base()
		if applyMemLimit(&cfg, 0, "/scope", true, "job-3") {
			t.Fatal("applied a limit that was never asked for")
		}
		if cfg.Linux.Resources != nil {
			t.Error("empty resources block should be omitted entirely")
		}
	})

	t.Run("serialises to what the OCI runtime expects", func(t *testing.T) {
		cfg := base()
		applyMemLimit(&cfg, 1<<30, "/scope", true, "job-4")
		raw, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{`"resources"`, `"memory"`, `"limit":1073741824`, `"cgroupsPath"`} {
			if !strings.Contains(string(raw), want) {
				t.Errorf("bundle JSON missing %s:\n%s", want, raw)
			}
		}

		// And the reverse: a bundle without a limit must not carry an empty
		// resources object, which some runtimes read as "cap at zero".
		plain := base()
		raw, err = json.Marshal(plain)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), `"resources"`) {
			t.Errorf("uncapped bundle still declares resources:\n%s", raw)
		}
	})
}
