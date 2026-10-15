// Package runrec is the run record: one list of timed steps per run (C24),
// appended by every producer (CLI, daemon, shim) and rendered two ways — a
// one-line simple view and a detailed per machine x step view — plus CSV and
// JSON for benchmarks. Shim work gets its own steps, never folded into
// "script": lumping it in is how the O(n^2) _pool_send bug hid.
package runrec

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Step is one timed piece of a run. Machine is the node id (key fingerprint)
// of the machine the step ran on; Chunk is -1 for steps that belong to no
// chunk. Name is one of pickle, compress, send, queue, compute, receive,
// unpickle, env_build, env_send, ddp_sync, script.
type Step struct {
	Machine string `json:"machine"`
	Node    string `json:"node"`
	Chunk   int    `json:"chunk"`
	Name    string `json:"step"`
	StartNS int64  `json:"start_ns"`
	DurNS   int64  `json:"dur_ns"`
	Bytes   int64  `json:"bytes,omitempty"`
}

// Record is one run's steps. Names (additive) maps a Machine id to a display
// name (hostname) for the views; ids without a name show shortened.
type Record struct {
	RunID    string            `json:"run_id"`
	TotalNS  int64             `json:"total_ns"`
	ScriptNS int64             `json:"script_ns"`
	Emulated bool              `json:"emulated,omitempty"`
	Names    map[string]string `json:"names,omitempty"`
	Steps    []Step            `json:"steps"`
}

// Merge is one record for a run made of several (a DDP ring's ranks): their
// steps and names together, the longest total and script time.
func Merge(recs ...Record) Record {
	out := Record{Names: map[string]string{}}
	for _, r := range recs {
		if out.RunID == "" {
			out.RunID = r.RunID
		}
		out.TotalNS = max(out.TotalNS, r.TotalNS)
		out.ScriptNS = max(out.ScriptNS, r.ScriptNS)
		out.Emulated = out.Emulated || r.Emulated
		for k, v := range r.Names {
			out.Names[k] = v
		}
		out.Steps = append(out.Steps, r.Steps...)
	}
	return out
}

// File is the record's name in a job's history dir, beside receipt.json.
const File = "steps.json"

var (
	mu    sync.Mutex
	steps []Step
)

// Add appends a step to this process's record (CLI side). Goroutine-safe.
func Add(s Step) {
	mu.Lock()
	steps = append(steps, s)
	mu.Unlock()
}

// Steps returns a copy of every step Add recorded in this process.
func Steps() []Step {
	mu.Lock()
	defer mu.Unlock()
	return append([]Step(nil), steps...)
}

// Save writes r as steps.json into jobDir. A run made in the emulated lab
// (PIPEDPEER_EMULATED=1, set by scripts/lab-emu.sh) is labelled emulated.
func Save(jobDir string, r Record) error {
	if os.Getenv("PIPEDPEER_EMULATED") == "1" {
		r.Emulated = true
	}
	b, err := json.MarshalIndent(r, "", " ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(jobDir, File+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(jobDir, File))
}

// Load reads jobDir's steps.json. A run without one returns an error
// wrapping fs.ErrNotExist.
func Load(jobDir string) (Record, error) {
	var r Record
	b, err := os.ReadFile(filepath.Join(jobDir, File))
	if err != nil {
		return r, err
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return r, fmt.Errorf("%s: %w", filepath.Join(jobDir, File), err)
	}
	return r, nil
}

// label is a machine's display name: its Names entry, else the id shortened.
func (r Record) label(machine string) string {
	if n := r.Names[machine]; n != "" {
		return n
	}
	if machine == "" {
		return "?"
	}
	if len(machine) > 12 {
		return machine[:12]
	}
	return machine
}

func isNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }

func secs(ns int64) float64 { return float64(ns) / float64(time.Second) }
