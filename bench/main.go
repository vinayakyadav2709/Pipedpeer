package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type Result struct {
	Sandbox   string  `json:"sandbox"`
	TaskName  string  `json:"task_name"`
	Iteration int     `json:"iteration"`
	TotalMs   float64 `json:"total_ms"`
	ExitCode  int     `json:"exit_code"`
}

// systemBinds exposes just enough of the host to run /bin/sh. Binding all of
// / read-only does not work: bwrap then cannot create /work on a read-only
// root. Without these the sandbox has no shell at all, every run exits
// non-zero immediately, and the timings measure how fast the sandbox fails
// rather than how fast it starts.
func systemBinds() []string {
	var out []string
	for _, dir := range []string{"/usr", "/etc", "/bin", "/sbin", "/lib", "/lib64"} {
		fi, err := os.Lstat(dir)
		if err != nil {
			continue
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			// Merged-/usr systems: /bin -> usr/bin and friends.
			target, err := os.Readlink(dir)
			if err != nil {
				continue
			}
			out = append(out, "--symlink", target, dir)
			continue
		}
		out = append(out, "--ro-bind", dir, dir)
	}
	return out
}

type TaskDef struct {
	name   string
	script string
}

func main() {
	baseDir, _ := os.MkdirTemp("", "sandbox-bench-*")
	defer os.RemoveAll(baseDir)

	tasks := []TaskDef{
		{"trivial", "echo hello > /dev/null"},
		{"medium", "i=0; while [ $i -lt 1000 ]; do i=$((i+1)); done"},
		{"python", "python3 -c \"import sys; print('hello')\""},
	}

	warmup := 5
	iterations := 20

	var allResults []Result

	fmt.Println("===== Sandbox Benchmark =====")
	fmt.Printf("Warmup: %d, Measured: %d per sandbox per task\n\n", warmup, iterations)

	for _, task := range tasks {
		fmt.Printf("--- Task: %s ---\n", task.name)
		fmt.Printf("  Script: %s\n\n", task.script)

		// bwrap
		workDir := filepath.Join(baseDir, "bwrap-"+task.name)
		os.MkdirAll(filepath.Join(workDir, "home"), 0755)

		for i := 0; i < warmup+iterations; i++ {
			start := time.Now()
			args := append([]string{
				"bwrap", "--die-with-parent",
				"--unshare-pid", "--unshare-ipc", "--unshare-uts",
				"--dev", "/dev", "--proc", "/proc", "--tmpfs", "/tmp",
			}, systemBinds()...)
			args = append(args,
				"--bind", workDir, "/work", "--bind", workDir+"/home", "/home/root",
				"--chdir", "/work",
				"--setenv", "HOME", "/home/root",
				"--setenv", "PATH", "/usr/local/bin:/usr/bin:/bin",
				"--", "/bin/sh", "-c", task.script,
			)
			cmd := exec.Command(args[0], args[1:]...)
			cmd.Run()
			elapsed := time.Since(start).Seconds() * 1000

			r := Result{
				Sandbox: "bwrap", TaskName: task.name, Iteration: i,
				TotalMs: elapsed, ExitCode: cmd.ProcessState.ExitCode(),
			}
			allResults = append(allResults, r)
			if i >= warmup {
				fmt.Printf("  bwrap [%2d]: %8.3f ms\n", i-warmup, elapsed)
			}
		}

		// crun
		crunWorkDir := filepath.Join(baseDir, "crun-"+task.name)
		os.MkdirAll(filepath.Join(crunWorkDir, "home"), 0755)
		os.MkdirAll(filepath.Join(crunWorkDir, "rootfs"), 0755)

		for i := 0; i < warmup+iterations; i++ {
			cid := fmt.Sprintf("bench-%s-%d", task.name, i)

			config := map[string]interface{}{
				"ociVersion": "1.0.2",
				"process": map[string]interface{}{
					"terminal": false,
					"user":     map[string]int{"uid": 0, "gid": 0},
					"args":     []string{"/bin/sh", "-c", task.script},
					"env": []string{
						"PATH=/usr/local/bin:/usr/bin:/bin",
						"HOME=/home/root",
					},
					"cwd": "/work",
				},
				"root": map[string]interface{}{
					"path": "rootfs", "readonly": true,
				},
				"hostname": "pipedpeer",
				"mounts": []map[string]interface{}{
					// Same reason as the bwrap --ro-bind above: the bundle
					// rootfs is empty, so the shell has to be mounted in.
					{"destination": "/usr", "type": "bind", "source": "/usr", "options": []string{"rbind", "ro"}},
					{"destination": "/bin", "type": "bind", "source": "/bin", "options": []string{"rbind", "ro"}},
					{"destination": "/lib", "type": "bind", "source": "/lib", "options": []string{"rbind", "ro"}},
					{"destination": "/lib64", "type": "bind", "source": "/lib64", "options": []string{"rbind", "ro"}},
					{"destination": "/etc", "type": "bind", "source": "/etc", "options": []string{"rbind", "ro"}},
					{"destination": "/proc", "type": "proc", "source": "proc"},
					{"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "noexec"}},
					{"destination": "/tmp", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "nodev"}},
					{"destination": "/work", "type": "bind", "source": crunWorkDir, "options": []string{"rbind", "rw"}},
					{"destination": "/home/root", "type": "bind", "source": crunWorkDir + "/home", "options": []string{"rbind", "rw"}},
				},
				"linux": map[string]interface{}{
					"namespaces": []map[string]string{
						{"type": "pid"}, {"type": "ipc"}, {"type": "uts"}, {"type": "mount"},
					},
				},
			}

			configJSON, _ := json.MarshalIndent(config, "", "  ")
			os.WriteFile(crunWorkDir+"/config.json", configJSON, 0644)

			exec.Command("crun", "delete", "-f", cid).Run()

			start := time.Now()
			cmd := exec.Command("crun", "run", "--bundle", crunWorkDir, cid)
			cmd.Run()
			elapsed := time.Since(start).Seconds() * 1000

			exec.Command("crun", "delete", "-f", cid).Run()

			r := Result{
				Sandbox: "crun", TaskName: task.name, Iteration: i,
				TotalMs: elapsed, ExitCode: cmd.ProcessState.ExitCode(),
			}
			allResults = append(allResults, r)
			if i >= warmup {
				fmt.Printf("  crun  [%2d]: %8.3f ms\n", i-warmup, elapsed)
			}
		}
		fmt.Println()
	}

	// Summary
	fmt.Println("\n===== Summary =====")
	for _, task := range tasks {
		var bwrapVals, crunVals []float64
		for _, r := range allResults {
			if r.TaskName != task.name || r.Iteration < warmup {
				continue
			}
			if r.Sandbox == "bwrap" {
				bwrapVals = append(bwrapVals, r.TotalMs)
			} else {
				crunVals = append(crunVals, r.TotalMs)
			}
		}

		bAvg, bMin, bMax := stats(bwrapVals)
		cAvg, cMin, cMax := stats(crunVals)

		fmt.Printf("\nTask: %s\n", task.name)
		fmt.Printf("  bwrap:  avg=%8.3f  min=%8.3f  max=%8.3f (ms)\n", bAvg, bMin, bMax)
		fmt.Printf("  crun:   avg=%8.3f  min=%8.3f  max=%8.3f (ms)\n", cAvg, cMin, cMax)
		fmt.Printf("  diff:   %+.3f ms (%+.2f%%)\n", cAvg-bAvg, (cAvg-bAvg)/bAvg*100)
	}

	// Save report
	bad := 0
	for _, r := range allResults {
		if r.Iteration >= warmup && r.ExitCode != 0 {
			bad++
		}
	}
	if bad > 0 {
		fmt.Fprintf(os.Stderr,
			"\nrefusing to report: %d measured runs exited non-zero, so these\n"+
				"timings are the cost of failing, not the cost of starting a sandbox.\n", bad)
		os.Exit(1)
	}

	writeReport(allResults, tasks, warmup)
}

func stats(samples []float64) (avg, min, max float64) {
	if len(samples) == 0 {
		return
	}
	min = samples[0]
	max = samples[0]
	sum := 0.0
	for _, v := range samples {
		sum += v
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	avg = sum / float64(len(samples))
	return
}

func writeReport(allResults []Result, tasks []TaskDef, warmup int) {
	var sb strings.Builder
	sb.WriteString("# Sandbox Benchmark Comparison\n\n")
	sb.WriteString(fmt.Sprintf("Date: %s\n\n", time.Now().Format(time.RFC3339)))
	sb.WriteString("## Methodology\n")
	sb.WriteString(fmt.Sprintf("- Warmup: %d iterations, Measured: 20 iterations per sandbox per task\n", warmup))
	sb.WriteString("- Measures total wall-clock time from sandbox start to process exit\n")
	sb.WriteString("- All tests on same machine, same conditions\n\n")

	sb.WriteString("## Results\n\n")
	sb.WriteString("| Task | Metric | bwrap (ms) | crun (ms) | Diff (ms) | Diff (%) |\n")
	sb.WriteString("|---|---|---|---|---|---|\n")

	for _, task := range tasks {
		var bwrapVals, crunVals []float64
		for _, r := range allResults {
			if r.TaskName != task.name || r.Iteration < warmup {
				continue
			}
			if r.Sandbox == "bwrap" {
				bwrapVals = append(bwrapVals, r.TotalMs)
			} else {
				crunVals = append(crunVals, r.TotalMs)
			}
		}
		bAvg, bMin, bMax := stats(bwrapVals)
		cAvg, cMin, cMax := stats(crunVals)
		diff := cAvg - bAvg
		pct := diff / bAvg * 100

		sb.WriteString(fmt.Sprintf("| %s | Average | %.3f | %.3f | %+.3f | %+.2f%% |\n", task.name, bAvg, cAvg, diff, pct))
		sb.WriteString(fmt.Sprintf("| %s | Minimum | %.3f | %.3f | %+.3f | |\n", "", bMin, cMin, cMin-bMin))
		sb.WriteString(fmt.Sprintf("| %s | Maximum | %.3f | %.3f | %+.3f | |\n", "", bMax, cMax, cMax-bMax))
	}

	sb.WriteString("\n## Raw Data (all measured iterations)\n\n")
	sb.WriteString("| Task | Iter | bwrap (ms) | crun (ms) |\n")
	sb.WriteString("|---|---|---|---|\n")
	for _, task := range tasks {
		var bRuns, cRuns []Result
		for _, r := range allResults {
			if r.TaskName != task.name || r.Iteration < warmup {
				continue
			}
			if r.Sandbox == "bwrap" {
				bRuns = append(bRuns, r)
			} else {
				cRuns = append(cRuns, r)
			}
		}
		for i := range bRuns {
			bVal := bRuns[i].TotalMs
			cVal := cRuns[i].TotalMs
			sb.WriteString(fmt.Sprintf("| %s | %d | %.3f | %.3f |\n", task.name, i, bVal, cVal))
		}
	}

	jsonData, _ := json.MarshalIndent(allResults, "", "  ")
	os.WriteFile("bench-results.json", jsonData, 0644)

	report := sb.String()
	os.WriteFile("bench-results.md", []byte(report), 0644)
	fmt.Print("\n\nReport saved to bench-results.md, raw data to bench-results.json\n")
}
