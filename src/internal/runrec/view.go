package runrec

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

// Place is one machine's share of a run's work.
type Place struct {
	Machine string
	Share   float64
}

// Summary is the simple view: how long the run took, how much of that was
// the script itself and how much was pipedpeer, and where the work ran.
type Summary struct {
	Total, Script, Overhead time.Duration
	Where                   []Place
}

// Simple splits r into script time and pipedpeer overhead. Work shares come
// from compute steps per machine; a run with none (nothing distributed) is
// credited to the machines its script steps ran on.
// ponytail: shares weigh compute time only, not the script's own local work
// between offloads; weigh that too if the shares mislead on mixed runs.
func Simple(r Record) Summary {
	script := r.ScriptNS
	if script == 0 {
		for _, s := range r.Steps {
			if s.Name == "script" {
				script += s.DurNS
			}
		}
	}
	s := Summary{Total: time.Duration(r.TotalNS), Script: time.Duration(script)}
	s.Overhead = max(0, s.Total-s.Script)

	by := map[string]int64{}
	var sum int64
	for _, want := range []string{"compute", "script"} {
		for _, st := range r.Steps {
			if st.Name == want && st.DurNS > 0 {
				by[r.label(st.Machine)] += st.DurNS
				sum += st.DurNS
			}
		}
		if sum > 0 {
			break
		}
	}
	for m, ns := range by {
		s.Where = append(s.Where, Place{Machine: m, Share: float64(ns) / float64(sum)})
	}
	sort.Slice(s.Where, func(i, j int) bool {
		if s.Where[i].Share != s.Where[j].Share {
			return s.Where[i].Share > s.Where[j].Share
		}
		return s.Where[i].Machine < s.Where[j].Machine
	})
	return s
}

// WriteSimple prints the one-line view, e.g.
// "pipedpeer: 41.2 s total — script 38.0 s, pipedpeer 3.2 s — ran on yeet 60%, fedora 40%".
func WriteSimple(w io.Writer, s Summary) {
	line := fmt.Sprintf("pipedpeer: %.1f s total — script %.1f s, pipedpeer %.1f s",
		s.Total.Seconds(), s.Script.Seconds(), s.Overhead.Seconds())
	if len(s.Where) > 0 {
		parts := make([]string, len(s.Where))
		for i, p := range s.Where {
			parts[i] = fmt.Sprintf("%s %.0f%%", p.Machine, p.Share*100)
		}
		line += " — ran on " + strings.Join(parts, ", ")
	}
	fmt.Fprintln(w, line)
}

// WriteDetailed prints per machine x step totals, then the slowest 10 chunks.
func WriteDetailed(w io.Writer, r Record) {
	s := Simple(r)
	WriteSimple(w, s)
	if r.Emulated {
		fmt.Fprintln(w, "(emulated lab run)")
	}

	type key struct{ machine, step string }
	type agg struct {
		n          int
		ns, nbytes int64
	}
	totals := map[key]*agg{}
	type ckey struct {
		machine string
		chunk   int
	}
	chunks := map[ckey]int64{}
	for _, st := range r.Steps {
		k := key{r.label(st.Machine), st.Name}
		a := totals[k]
		if a == nil {
			a = &agg{}
			totals[k] = a
		}
		a.n++
		a.ns += st.DurNS
		a.nbytes += st.Bytes
		if st.Chunk >= 0 {
			chunks[ckey{k.machine, st.Chunk}] += st.DurNS
		}
	}
	keys := make([]key, 0, len(totals))
	for k := range totals {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].machine != keys[j].machine {
			return keys[i].machine < keys[j].machine
		}
		return keys[i].step < keys[j].step
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "machine\tstep\tcount\ttotal\tbytes")
	for _, k := range keys {
		a := totals[k]
		fmt.Fprintf(tw, "%s\t%s\t%d\t%.3f s\t%s\n", k.machine, k.step, a.n, secs(a.ns), humanBytes(a.nbytes))
	}
	_ = tw.Flush() // writes to w; errors surface on w's own writes

	if len(chunks) == 0 {
		return
	}
	ck := make([]ckey, 0, len(chunks))
	for k := range chunks {
		ck = append(ck, k)
	}
	sort.Slice(ck, func(i, j int) bool {
		if chunks[ck[i]] != chunks[ck[j]] {
			return chunks[ck[i]] > chunks[ck[j]]
		}
		return ck[i].chunk < ck[j].chunk
	})
	if len(ck) > 10 {
		ck = ck[:10]
	}
	fmt.Fprintln(w, "\nslowest chunks:")
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "chunk\tmachine\ttotal")
	for _, k := range ck {
		fmt.Fprintf(tw, "%d\t%s\t%.3f s\n", k.chunk, k.machine, secs(chunks[k]))
	}
	_ = tw.Flush()
}

func humanBytes(n int64) string {
	switch {
	case n == 0:
		return "-"
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// CSVHeader is the column order of WriteCSV.
var CSVHeader = []string{"run_id", "machine", "node", "chunk", "step", "start_ns", "dur_ns", "bytes", "emulated"}

// WriteCSV writes one row per step.
func WriteCSV(w io.Writer, r Record) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(CSVHeader); err != nil {
		return err
	}
	for _, s := range r.Steps {
		if err := cw.Write([]string{r.RunID, s.Machine, s.Node, strconv.Itoa(s.Chunk), s.Name,
			strconv.FormatInt(s.StartNS, 10), strconv.FormatInt(s.DurNS, 10),
			strconv.FormatInt(s.Bytes, 10), strconv.FormatBool(r.Emulated)}); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// WriteJSON writes the Record.
func WriteJSON(w io.Writer, r Record) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", " ")
	return enc.Encode(r)
}
