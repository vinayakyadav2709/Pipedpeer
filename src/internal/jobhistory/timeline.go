package jobhistory

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// Stage is one timed step of a run.
//
// A run's total says that it was slow; the stages say where. Most of what
// separates a cold run from a warm one - the Nix build, the closure import -
// happens outside the script, and the script's own timer never sees it.
type Stage struct {
	Name string `json:"name"`
	// Where ran it: "local" for the submitting machine, else the node.
	Where string `json:"where,omitempty"`
	// At is when the stage started (RFC 3339, UTC).
	At string `json:"at"`
	Ms int64  `json:"ms"`
}

// Timeline collects stages as they happen.
type Timeline struct {
	Stages []Stage
}

// Add records a stage that started at start and took d.
func (t *Timeline) Add(name, where string, start time.Time, d time.Duration) {
	if t == nil {
		return
	}
	t.Stages = append(t.Stages, Stage{
		Name:  name,
		Where: where,
		At:    start.UTC().Format(time.RFC3339Nano),
		Ms:    d.Milliseconds(),
	})
}

// Begin starts a stage and returns the function that ends it.
func (t *Timeline) Begin(name, where string) func() {
	start := time.Now()
	return func() { t.Add(name, where, start, time.Since(start)) }
}

// Total is the wall time from the first stage's start to the last one's end.
func Total(stages []Stage) time.Duration {
	first, last, ok := span(stages)
	if !ok {
		return 0
	}
	return last.Sub(first)
}

func span(stages []Stage) (first, last time.Time, ok bool) {
	for _, s := range stages {
		at, err := time.Parse(time.RFC3339Nano, s.At)
		if err != nil {
			continue
		}
		end := at.Add(time.Duration(s.Ms) * time.Millisecond)
		if !ok || at.Before(first) {
			first = at
		}
		if !ok || end.After(last) {
			last = end
		}
		ok = true
	}
	return first, last, ok
}

// Summary is a one-line account of the stages, for the end of a run.
func Summary(stages []Stage) string {
	parts := make([]string, 0, len(stages))
	for _, s := range stages {
		parts = append(parts, fmt.Sprintf("%s %s", s.Name, fmtMs(s.Ms)))
	}
	return strings.Join(parts, " · ")
}

// WriteWaterfall draws the stages as bars on a shared time axis, so overlap
// and gaps show as plainly as durations.
func WriteWaterfall(w io.Writer, stages []Stage, width int) {
	first, last, ok := span(stages)
	if !ok {
		fmt.Fprintln(w, "no timeline recorded for this job")
		return
	}
	if width < 10 {
		width = 10
	}
	total := last.Sub(first)
	scale := float64(width) / float64(max(total, time.Millisecond))

	nameW, whereW := len("STAGE"), len("WHERE")
	for _, s := range stages {
		nameW = max(nameW, len(s.Name))
		whereW = max(whereW, len(s.Where))
	}
	fmt.Fprintf(w, "%-*s  %-*s  %8s  %8s  %s\n", nameW, "STAGE", whereW, "WHERE", "START", "TIME", "")
	for _, s := range stages {
		at, err := time.Parse(time.RFC3339Nano, s.At)
		if err != nil {
			continue
		}
		off := at.Sub(first)
		lead := int(float64(off) * scale)
		bar := int(float64(time.Duration(s.Ms)*time.Millisecond) * scale)
		if bar < 1 {
			bar = 1
		}
		if lead+bar > width {
			lead = max(0, width-bar)
		}
		fmt.Fprintf(w, "%-*s  %-*s  %8s  %8s  %s%s\n", nameW, s.Name, whereW, s.Where,
			"+"+fmtMs(off.Milliseconds()), fmtMs(s.Ms),
			strings.Repeat(" ", lead), strings.Repeat("█", bar))
	}
	fmt.Fprintf(w, "%-*s  %-*s  %8s  %8s\n", nameW, "total", whereW, "", "", fmtMs(total.Milliseconds()))
}

func fmtMs(ms int64) string {
	switch {
	case ms < 1000:
		return fmt.Sprintf("%dms", ms)
	case ms < 60_000:
		return fmt.Sprintf("%.1fs", float64(ms)/1000)
	default:
		return fmt.Sprintf("%dm%02ds", ms/60_000, (ms/1000)%60)
	}
}
