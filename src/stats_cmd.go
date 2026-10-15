package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// printStatus is the body of `pipedpeer status`: the machines, peers with
// no direct path, running tasks, the last runs and the warm-up step.
func printStatus(w io.Writer, port int) {
	fmt.Fprintln(w)
	nodes, err := fetchClusterNodes(port)
	switch {
	case err != nil:
		fmt.Fprintf(w, "could not list the machines: %v\n", err)
	case len(nodes) == 0:
		fmt.Fprintln(w, "no machines known yet")
	default:
		printNodeTable(w, nodes)
	}
	printUnreachable(w, port)

	if tasks, err := fetchClusterTasks(port); err != nil {
		fmt.Fprintf(os.Stderr, "could not list running tasks: %v\n", err)
	} else if len(tasks) > 0 {
		fmt.Fprintln(w, "\nRunning:")
		for _, t := range tasks {
			job := t.JobName
			if job == "" {
				job = "-"
			}
			gpu := ""
			if t.GPUIndex >= 0 {
				gpu = "  gpu " + strconv.Itoa(t.GPUIndex)
			}
			fmt.Fprintf(w, "  %-24s %-20s %-10s %s%s\n", t.Hostname, truncate(job, 20), t.State,
				t.Age.Truncate(time.Second), gpu)
		}
	}

	fmt.Fprintln(w, "\nRecent runs:")
	if err := listJobs(w, 3); err != nil {
		fmt.Fprintf(os.Stderr, "could not list recent runs: %v\n", err)
	}

	if line := warmupLine(); line != "" {
		fmt.Fprintf(w, "\nWarm-up: %s\n", line)
	}
}

// warmupLine is the last line of the environment warm-up log when it was
// written within the hour, else "".
func warmupLine() string {
	path := filepath.Join(userdir.State(), "warmup.log")
	info, err := os.Stat(path)
	if err != nil || time.Since(info.ModTime()) > time.Hour {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not read %s: %v\n", path, err)
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}
