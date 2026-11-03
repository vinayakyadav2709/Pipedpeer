package warmup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestArgsCPUTorchWithoutNVIDIA(t *testing.T) {
	if got := strings.Join(Args("c.py", false), " "); got != "run c.py --check-only --gpu off" {
		t.Fatal(got)
	}
	if got := strings.Join(Args("c.py", true), " "); got != "run c.py --check-only" {
		t.Fatal(got)
	}
}

// TestLogEndsInDone: the detached run writes its output, then "done" (C19).
func TestLogEndsInDone(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	exe := filepath.Join(t.TempDir(), "fake")
	if err := os.WriteFile(exe, []byte("#!/bin/sh\necho \"built in 1s: $*\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := Start(exe, "v1", false); err != nil {
		t.Fatal(err)
	}
	var body []byte
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		body, _ = os.ReadFile(LogPath())
		if strings.HasSuffix(string(body), "done\n") {
			break
		}
	}
	if !strings.HasSuffix(string(body), "done\n") || !strings.Contains(string(body), "--gpu off") {
		t.Fatalf("log:\n%s", body)
	}
	if Running() {
		t.Fatal("finished warm-up still reported running")
	}
	if !Done("v1", false) {
		t.Fatalf("finished warm-up not Done for its own build:\n%s", body)
	}
}

const tailCPU = " torch CPU)"

// header is the new header; the human text comes first, the marker last.
func header(build, torch string) string {
	return "2026-11-03T10:00:00Z building the common environment (numpy, pandas, scikit-learn, scipy, " +
		"matplotlib, seaborn, tqdm, torch " + torch + ") build=" + build + " torch=" + torch + "\n"
}

// oldHeader is what builds before the marker wrote (case 7.9).
const oldHeader = "2026-11-03T10:00:00Z building the common environment (numpy, pandas, scikit-learn, scipy, matplotlib, seaborn, tqdm," + tailCPU + "\n"

// setLog writes the log (or none when body is nil) with the given age.
func setLog(t *testing.T, body *string, age time.Duration) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	if body == nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(LogPath()), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(LogPath(), []byte(*body), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Now().Add(-age)
	if err := os.Chtimes(LogPath(), mt, mt); err != nil {
		t.Fatal(err)
	}
}

func strp(s string) *string { return &s }

// TestWarmupStateTable is docs/peer-cache-cases.md §7, rows 7.1-7.9.
func TestWarmupStateTable(t *testing.T) {
	cases := []struct {
		name    string
		body    *string
		age     time.Duration
		running bool
		done    bool // Done("v1", false)
		doneNv  bool // Done("v1", true); only checked for 7.4
		check   string
	}{
		{"7.1 no file", nil, 0, false, false, false, ""},
		{"7.2 header v1 CPU, done", strp(header("v1", "CPU") + "done\n"), 0, false, true, false, ""},
		{"7.3 same, asked with another build", strp(header("v1", "CPU") + "done\n"), 0, false, false, false, "other"},
		{"7.4 same, asked with nvidia", strp(header("v1", "CPU") + "done\n"), 0, false, false, false, "nvidia"},
		{"7.5 last failed", strp(header("v1", "CPU") + "failed (exit 1); details above\n"), 0, false, false, false, ""},
		{"7.6 progress, modified now", strp(header("v1", "CPU") + "built numpy\n"), 0, true, false, false, ""},
		{"7.7 progress, modified 2 h ago", strp(header("v1", "CPU") + "built numpy\n"), 2 * time.Hour, false, false, false, ""},
		{"7.8 empty file", strp(""), 0, false, false, false, ""},
		{"7.9 old header without build=, done", strp(oldHeader + "done\n"), 0, false, false, false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setLog(t, c.body, c.age)
			if got := Running(); got != c.running {
				t.Errorf("Running() = %v, want %v", got, c.running)
			}
			switch c.check {
			case "other":
				if !Done("v1", false) {
					t.Errorf("precondition: the log is not Done for its own build")
				}
				if Done("v2", false) {
					t.Errorf("Done(v2) = true for a v1 build")
				}
			case "nvidia":
				if !Done("v1", false) {
					t.Errorf("precondition: the log is not Done for its own build")
				}
				if Done("v1", true) {
					t.Errorf("Done(v1, nvidia) = true for a CPU build")
				}
			default:
				if got := Done("v1", false); got != c.done {
					t.Errorf("Done(v1, false) = %v, want %v", got, c.done)
				}
			}
		})
	}
}

// TestStartSkipsWhenDone: a finished build for this version and torch kind is
// left alone; Start returns nil without launching (no exe needed) or rewriting
// the log.
func TestStartSkipsWhenDone(t *testing.T) {
	body := header("v1", "CPU") + "done\n"
	setLog(t, &body, 0)
	if err := Start("/nonexistent/pipedpeer", "v1", false); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(LogPath())
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Fatalf("log rewritten:\n%s", got)
	}
}
