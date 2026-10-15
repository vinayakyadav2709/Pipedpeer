package runrec

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

// ExportAll writes every job's record (and receipt, if any) started at or
// after since into dir: <run id>.csv (steps) and <run id>.json (job
// metadata, record, receipt) per run, plus runs.csv with one line per run.
// Runs without a step record (made before M1, or failed before running) are
// skipped and counted in one warning line. Returns the number exported.
func ExportAll(dir string, since time.Time) (int, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(jobhistory.BaseDir())
	if err != nil {
		if isNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	runsF, err := os.Create(filepath.Join(dir, "runs.csv"))
	if err != nil {
		return 0, err
	}
	defer runsF.Close()
	runs := csv.NewWriter(runsF)
	if err := runs.Write([]string{"run_id", "started_at", "script", "status", "emulated",
		"total_s", "script_s", "overhead_s", "steps", "where"}); err != nil {
		return 0, err
	}

	n, noRecord := 0, 0
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, e := range entries {
		jobDir := filepath.Join(jobhistory.BaseDir(), e.Name())
		if !e.IsDir() || !jobhistory.IsRecordDir(jobDir) {
			continue
		}
		job, err := jobhistory.ReadRecordAt(jobDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: export: skipping %s: unreadable metadata: %v\n", jobDir, err)
			continue
		}
		if !since.IsZero() {
			started, err := time.Parse(time.RFC3339Nano, job.StartedAt)
			if err != nil {
				fmt.Fprintf(os.Stderr, "warning: export: skipping %s: bad started_at %q\n", jobDir, job.StartedAt)
				continue
			}
			if started.Before(since) {
				continue
			}
		}
		rec, err := Load(jobDir)
		if err != nil {
			if isNotExist(err) {
				noRecord++
			} else {
				fmt.Fprintf(os.Stderr, "warning: export: skipping %s: %v\n", jobDir, err)
			}
			continue
		}
		if rec.RunID == "" {
			rec.RunID = job.ID
		}
		receipt, err := os.ReadFile(filepath.Join(jobDir, "receipt.json"))
		if err != nil && !isNotExist(err) {
			fmt.Fprintf(os.Stderr, "warning: export: %s: receipt unreadable, exported without it: %v\n", jobDir, err)
		}
		if err := exportOne(dir, job, rec, receipt); err != nil {
			return n, err
		}

		sum := Simple(rec)
		where := make([]string, len(sum.Where))
		for i, p := range sum.Where {
			where[i] = fmt.Sprintf("%s %.0f%%", p.Machine, p.Share*100)
		}
		if err := runs.Write([]string{rec.RunID, job.StartedAt, job.ScriptPath, job.Status,
			strconv.FormatBool(rec.Emulated), fmtSecs(sum.Total), fmtSecs(sum.Script), fmtSecs(sum.Overhead),
			strconv.Itoa(len(rec.Steps)), strings.Join(where, "; ")}); err != nil {
			return n, err
		}
		n++
	}
	if noRecord > 0 {
		fmt.Fprintf(os.Stderr, "warning: export: skipped %d run(s) with no step record\n", noRecord)
	}
	runs.Flush()
	if err := runs.Error(); err != nil {
		return n, err
	}
	return n, runsF.Close()
}

func exportOne(dir string, job jobhistory.Record, rec Record, receipt []byte) error {
	base := filepath.Join(dir, rec.RunID)
	f, err := os.Create(base + ".csv")
	if err != nil {
		return err
	}
	if err := WriteCSV(f, rec); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	out := struct {
		Job     jobhistory.Record `json:"job"`
		Record  Record            `json:"record"`
		Receipt json.RawMessage   `json:"receipt,omitempty"`
	}{job, rec, nil}
	if json.Valid(receipt) {
		out.Receipt = receipt
	} else if len(receipt) > 0 {
		fmt.Fprintf(os.Stderr, "warning: export: %s receipt is not valid JSON, exported without it\n", rec.RunID)
	}
	b, err := json.MarshalIndent(out, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(base+".json", b, 0o644)
}

func fmtSecs(d time.Duration) string { return strconv.FormatFloat(d.Seconds(), 'f', 3, 64) }
