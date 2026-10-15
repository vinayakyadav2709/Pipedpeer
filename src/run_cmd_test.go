package main

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Flags after the script belong to the script, as with python:
// `pipedpeer run -v t.py --epochs 3` gives t.py "--epochs 3".
func TestRunPassesScriptFlagsThrough(t *testing.T) {
	cmd := newRunCmd()
	if err := cmd.Flags().Parse([]string{"-v", "t.py", "--epochs", "3", "-v"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got, want := cmd.Flags().Args(), []string{"t.py", "--epochs", "3", "-v"}; !slices.Equal(got, want) {
		t.Fatalf("args = %q, want %q", got, want)
	}
	if v, _ := cmd.Flags().GetBool("verbose"); !v {
		t.Fatal("-v before the script did not set --verbose")
	}
}

// A missing script fails first, before any daemon start or placement
// (a typo used to reach three remote nodes before "file not found").
func TestRunMissingScriptFailsFirst(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "04_torch_ddp.y")
	cmd := newRunCmd()
	cmd.SetArgs([]string{"--remote", "--gpu", "force", missing})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "no such file") {
		t.Fatalf("err = %v, want a no-such-file error", err)
	}
	if err := checkScript(t.TempDir()); err == nil || !strings.Contains(err.Error(), "directory") {
		t.Fatalf("directory: err = %v, want a not-a-script error", err)
	}
}

// A torch script without --gpu builds the CUDA closure only when some
// healthy machine can use a GPU.
func TestInferGPUNeedsAGPUSomewhere(t *testing.T) {
	cpu := clusterNode{NodeID: "a", State: "healthy", Capabilities: map[string]string{"gpu": "none"}}
	deadGPU := clusterNode{NodeID: "b", State: "unreachable", Capabilities: map[string]string{"gpu": "nvidia"}}
	liveGPU := clusterNode{NodeID: "c", State: "healthy", Capabilities: map[string]string{"gpu": "nvidia"}}
	if got, _ := inferGPU(false, []clusterNode{cpu, deadGPU}, nil); got {
		t.Error("no healthy GPU anywhere: want the CPU environment")
	}
	// The environment's only GPU build is CUDA: an AMD card (a lab container
	// sees the host's APU iGPU) gets the CPU one.
	amd := clusterNode{NodeID: "d", State: "healthy", Capabilities: map[string]string{"gpu": "amd"}}
	if got, _ := inferGPU(false, []clusterNode{cpu, amd}, nil); got {
		t.Error("only an AMD GPU: want the CPU environment")
	}
	if got, _ := inferGPU(false, []clusterNode{cpu, liveGPU}, nil); !got {
		t.Error("a healthy peer has a GPU: want GPU preferred")
	}
	if got, _ := inferGPU(true, nil, nil); !got {
		t.Error("this machine has a GPU: want GPU preferred")
	}
	if got, _ := inferGPU(false, nil, errors.New("down")); !got {
		t.Error("node list failed: want the old preference kept")
	}
}

// The environment follows the node it runs on: under a GPU preference a
// CPU-only target gets the CPU build, never the cu126 wheel set.
func TestEnvGPUFollowsTheTarget(t *testing.T) {
	nv := map[string]string{"gpu": "nvidia"}
	none := map[string]string{"gpu": "none"}
	if !envGPU(true, true, 0, "self", "x", none) {
		t.Error("--gpu force must build the GPU environment")
	}
	if envGPU(false, false, 0, "self", "x", nv) {
		t.Error("no GPU wanted: want the CPU environment")
	}
	if !envGPU(false, true, 0, "self", "x", nv) {
		t.Error("preferred and the target has an NVIDIA GPU: want CUDA")
	}
	if envGPU(false, true, 0, "self", "x", none) {
		t.Error("preferred but the target is CPU-only: want the CPU environment")
	}
}
