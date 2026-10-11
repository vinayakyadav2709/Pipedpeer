package jobhistory

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// Candidate is one node the coordinator considered for a run, with what it
// saw when it chose: enough to answer "why that node?" after the fact.
type Candidate struct {
	NodeID string `json:"node_id"`
	Host   string `json:"host,omitempty"`
	// Source is where the coordinator heard of the node: the local daemon's
	// store, LAN discovery, the registry, its cache, or itself.
	Source   string  `json:"source,omitempty"`
	Score    float64 `json:"score,omitempty"`
	Chosen   bool    `json:"chosen,omitempty"`
	Rejected bool    `json:"rejected,omitempty"`
	Reason   string  `json:"reason,omitempty"`
	// The inputs the score is made of.
	CPUPercent    float64 `json:"cpu_percent,omitempty"`
	MemPercent    float64 `json:"mem_percent,omitempty"`
	ActiveJobs    int     `json:"active_jobs,omitempty"`
	Cores         int     `json:"cores,omitempty"`
	FreeCores     float64 `json:"free_cores,omitempty"`
	AvailMemBytes int64   `json:"avail_mem_bytes,omitempty"`
	GPU           string  `json:"gpu,omitempty"`
	Health        float64 `json:"health,omitempty"`
}

// WriteExplanation says how a run's node was chosen.
func WriteExplanation(w io.Writer, r Record) {
	switch {
	case len(r.Candidates) == 0 && (r.PlacementSource == "self" || r.PlacementSource == ""):
		fmt.Fprintln(w, "Ran on this machine: no placement was made.")
		fmt.Fprintln(w, "(`pipedpeer run` uses this machine unless given --remote or --host; pool work can still spill to peers.)")
		return
	case len(r.Candidates) == 0:
		fmt.Fprintf(w, "Placed by %s on %s", r.PlacementSource, r.RunHost)
		if r.PlacementReason != "" {
			fmt.Fprintf(w, " (%s)", r.PlacementReason)
		}
		fmt.Fprintln(w, ".")
		fmt.Fprintln(w, "No candidate list was recorded for this run.")
		return
	}

	strategy := r.Strategy
	if strategy == "" {
		strategy = "smart"
	}
	eligible := 0
	for _, c := range r.Candidates {
		if !c.Rejected {
			eligible++
		}
	}
	fmt.Fprintf(w, "Strategy: %s. %d node(s) considered, %d eligible.\n", strategy, len(r.Candidates), eligible)
	if r.EstimatedMemBytes > 0 {
		fmt.Fprintf(w, "The job was estimated to need %s (%s estimate).\n", humanBytes(r.EstimatedMemBytes), r.EstimationTier)
	}
	fmt.Fprintln(w)

	rows := append([]Candidate(nil), r.Candidates...)
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Chosen != rows[j].Chosen {
			return rows[i].Chosen
		}
		if rows[i].Rejected != rows[j].Rejected {
			return !rows[i].Rejected
		}
		return rows[i].Score > rows[j].Score
	})
	fmt.Fprintf(w, "   %-10s %-14s %-10s %6s %5s %5s %5s %9s %10s  %s\n",
		"NODE", "HOST", "SOURCE", "SCORE", "CPU%", "MEM%", "JOBS", "FREE CPU", "FREE MEM", "")
	for _, c := range rows {
		mark := "  "
		if c.Chosen {
			mark = "→ "
		}
		score := fmt.Sprintf("%.3f", c.Score)
		note := ""
		if c.Rejected {
			score = "-"
			note = "rejected: " + c.Reason
		}
		freeCPU := "-"
		if c.Cores > 0 {
			freeCPU = fmt.Sprintf("%.1f/%d", c.FreeCores, c.Cores)
		}
		fmt.Fprintf(w, " %s%-10s %-14s %-10s %6s %5.0f %5.0f %5d %9s %10s  %s\n",
			mark, short(c.NodeID), trunc(c.Host, 14), c.Source, score,
			c.CPUPercent, c.MemPercent, c.ActiveJobs, freeCPU, humanBytes(c.AvailMemBytes), note)
	}
	if r.PlacementReason != "" {
		fmt.Fprintf(w, "\nChosen: %s\n", r.PlacementReason)
	}
	if strategy == "smart" {
		fmt.Fprintln(w, "\nThe score is mostly headroom (idle CPU and memory, fewer running jobs),")
		fmt.Fprintln(w, "then capability (cores, clock, free RAM), times the node's health.")
	}
}

func short(id string) string { return id[:min(8, len(id))] }

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func humanBytes(b int64) string {
	if b <= 0 {
		return "-"
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return strings.TrimSuffix(fmt.Sprintf("%.1f", float64(b)/float64(div)), ".0") + string("KMGTPE"[exp]) + "i"
}
