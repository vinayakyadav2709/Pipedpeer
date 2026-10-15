package runrec

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

func fake() Record {
	s := int64(time.Second)
	return Record{
		RunID: "r1", TotalNS: 41_200 * int64(time.Millisecond), ScriptNS: 38 * s,
		Names: map[string]string{"k-yeet": "yeet", "k-fedora": "fedora"},
		Steps: []Step{
			{Machine: "k-yeet", Chunk: -1, Name: "env_build", DurNS: 2 * s},
			{Machine: "k-yeet", Chunk: 0, Name: "pickle", DurNS: s / 10, Bytes: 2048},
			{Machine: "k-yeet", Chunk: 0, Name: "compute", DurNS: 6 * s},
			{Machine: "k-fedora", Node: "k-fedora", Chunk: 1, Name: "compute", DurNS: 4 * s},
			{Machine: "k-fedora", Chunk: 1, Name: "send", DurNS: s / 2, Bytes: 4 << 20},
			{Machine: "k-yeet", Chunk: -1, Name: "script", DurNS: 38 * s},
		},
	}
}

func TestSimpleSplitsScriptAndOverhead(t *testing.T) {
	s := Simple(fake())
	if s.Script != 38*time.Second || s.Overhead != 3200*time.Millisecond {
		t.Fatalf("script %v overhead %v", s.Script, s.Overhead)
	}
	if len(s.Where) != 2 || s.Where[0].Machine != "yeet" || s.Where[0].Share != 0.6 || s.Where[1].Machine != "fedora" {
		t.Fatalf("where %+v", s.Where)
	}
	var b bytes.Buffer
	WriteSimple(&b, s)
	want := "pipedpeer: 41.2 s total — script 38.0 s, pipedpeer 3.2 s — ran on yeet 60%, fedora 40%\n"
	if b.String() != want {
		t.Fatalf("got %q", b.String())
	}

	// No compute steps: the script's machine gets the whole run; no
	// ScriptNS: the script steps are summed.
	r := fake()
	r.ScriptNS = 0
	r.Steps = []Step{{Machine: "k-yeet", Chunk: -1, Name: "script", DurNS: int64(time.Second)}}
	s = Simple(r)
	if s.Script != time.Second || len(s.Where) != 1 || s.Where[0].Share != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestDetailedShowsStepsAndSlowChunks(t *testing.T) {
	var b bytes.Buffer
	WriteDetailed(&b, fake())
	out := b.String()
	for _, want := range []string{"machine", "fedora", "send", "4.0 MB", "slowest chunks:"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
	if i, j := strings.Index(out, "6.100 s"), strings.Index(out, "4.500 s"); i < 0 || j < 0 || i > j {
		t.Fatalf("chunks not slowest first:\n%s", out)
	}
}

func TestCSVAndJSONRoundTrip(t *testing.T) {
	r := fake()
	var b bytes.Buffer
	if err := WriteJSON(&b, r); err != nil {
		t.Fatal(err)
	}
	var back Record
	if err := json.Unmarshal(b.Bytes(), &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, r) {
		t.Fatalf("json round trip:\n%+v\n%+v", back, r)
	}
	if !strings.Contains(b.String(), `"step": "compute"`) || !strings.Contains(b.String(), `"steps"`) {
		t.Fatalf("json tags: %s", b.String())
	}

	b.Reset()
	if err := WriteCSV(&b, r); err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(&b).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows[0], CSVHeader) || len(rows) != len(r.Steps)+1 {
		t.Fatalf("rows %v", rows)
	}
	for i, row := range rows[1:] {
		chunk, _ := strconv.Atoi(row[3])
		start, _ := strconv.ParseInt(row[5], 10, 64)
		dur, _ := strconv.ParseInt(row[6], 10, 64)
		nb, _ := strconv.ParseInt(row[7], 10, 64)
		got := Step{Machine: row[1], Node: row[2], Chunk: chunk, Name: row[4], StartNS: start, DurNS: dur, Bytes: nb}
		if got != r.Steps[i] || row[0] != "r1" {
			t.Fatalf("csv row %d: %+v vs %+v", i, got, r.Steps[i])
		}
	}
}

func TestAddSteps(t *testing.T) {
	Add(Step{Name: "env_build", Chunk: -1})
	got := Steps()
	got[0].Name = "changed"
	if Steps()[0].Name != "env_build" {
		t.Fatal("Steps must return a copy")
	}
}

func TestSaveLoadExportAll(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("PIPEDPEER_EMULATED", "1")
	job, dir, err := jobhistory.NewRecord("/x/main.py", "", "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := jobhistory.NewRecord("/x/old.py", "", "", false, false); err != nil { // no steps.json
		t.Fatal(err)
	}
	if _, err := Load(dir); !isNotExist(err) {
		t.Fatalf("Load without a record: %v", err)
	}
	r := fake()
	r.RunID = job.ID
	if err := Save(dir, r); err != nil {
		t.Fatal(err)
	}
	got, err := Load(dir)
	if err != nil || !got.Emulated || len(got.Steps) != len(r.Steps) {
		t.Fatalf("load: %+v %v", got, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "receipt.json"), []byte(`{"remote_items":3}`), 0o644); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(t.TempDir(), "export")
	n, err := ExportAll(out, time.Now().Add(-time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("exported %d, %v", n, err)
	}
	b, err := os.ReadFile(filepath.Join(out, job.ID+".json"))
	if err != nil || !strings.Contains(string(b), `"remote_items": 3`) || !strings.Contains(string(b), `"emulated": true`) {
		t.Fatalf("json export: %s %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(out, job.ID+".csv")); err != nil {
		t.Fatal(err)
	}
	runs, err := os.ReadFile(filepath.Join(out, "runs.csv"))
	if err != nil || strings.Count(string(runs), "\n") != 2 || !strings.Contains(string(runs), "yeet 60%") {
		t.Fatalf("runs.csv: %s %v", runs, err)
	}
	if n, err := ExportAll(out, time.Now().Add(time.Hour)); err != nil || n != 0 {
		t.Fatalf("since filter: %d %v", n, err)
	}
}
