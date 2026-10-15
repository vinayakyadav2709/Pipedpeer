package resourceest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

func TestTier3ImportBaseline(t *testing.T) {
	// With just torch + pandas imports, should estimate ~2.3GB + python baseline
	req := Estimate(EstimateOpts{
		Imports: []string{"torch", "pandas"},
	})

	if req.Tier != "import" {
		t.Fatalf("expected tier=import, got %s", req.Tier)
	}

	expectedMin := int64(2*GB + 300*MB + 100*MB) // torch + pandas + python
	if req.MemBytes < expectedMin {
		t.Fatalf("expected at least %d bytes, got %d", expectedMin, req.MemBytes)
	}
}

func TestTier2FileSizeEstimate(t *testing.T) {
	// Create a 1MB temp data file
	dir := t.TempDir()
	dataFile := filepath.Join(dir, "data.csv")
	data := make([]byte, 1*MB)
	os.WriteFile(dataFile, data, 0644)

	req := Estimate(EstimateOpts{
		DataFilePaths: []string{dataFile},
		Imports:       []string{"pandas"},
	})

	if req.Tier != "filesize" {
		t.Fatalf("expected tier=filesize, got %s", req.Tier)
	}

	// 1MB × 3 + pandas(300MB) + python(100MB) = ~403MB
	expectedMin := int64(1*MB*3 + 300*MB + 100*MB)
	if req.MemBytes < expectedMin {
		t.Fatalf("expected at least %d bytes, got %d", expectedMin, req.MemBytes)
	}
}

func TestTier0UserOverride(t *testing.T) {
	req := Estimate(EstimateOpts{
		UserMemBytes: 10 * GB,
		Imports:      []string{"torch"}, // should be ignored
	})

	if req.Tier != "user" {
		t.Fatalf("expected tier=user, got %s", req.Tier)
	}
	if req.MemBytes != 10*GB {
		t.Fatalf("expected 10GB, got %d", req.MemBytes)
	}
}

func TestFallbackDefault(t *testing.T) {
	// No imports, no files, no history → default 512MB
	req := Estimate(EstimateOpts{})
	if req.MemBytes != defaultEstimate {
		t.Fatalf("expected default %d, got %d", defaultEstimate, req.MemBytes)
	}
}

func TestParseMemString(t *testing.T) {
	cases := []struct {
		input    string
		expected int64
	}{
		{"10GB", 10 * GB},
		{"10G", 10 * GB},
		{"512MB", 512 * MB},
		{"512M", 512 * MB},
		{"2048KB", 2048 * KB},
		{"2048K", 2048 * KB},
		{"", 0},
		{"abc", 0},
	}

	for _, tc := range cases {
		got := ParseMemString(tc.input)
		// go-humanize may interpret GB as 10^9 vs GiB as 2^30. Accept either.
		if tc.expected > 0 && got == 0 {
			t.Errorf("ParseMemString(%q) = %d, want non-zero ~%d", tc.input, got, tc.expected)
		} else if tc.expected == 0 && got != 0 {
			t.Errorf("ParseMemString(%q) = %d, want 0", tc.input, got)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	// go-humanize uses IEC units (GiB, MiB, KiB) with IBytes
	cases := []struct {
		input int64
	}{
		{10 * GB},
		{512 * MB},
		{100 * KB},
	}

	for _, tc := range cases {
		got := FormatBytes(tc.input)
		if got == "" {
			t.Errorf("FormatBytes(%d) returned empty string", tc.input)
		}
		// Verify it contains a unit suffix (not just a number)
		hasUnit := false
		for _, suffix := range []string{"GiB", "MiB", "KiB", "B"} {
			if len(got) > len(suffix) {
				if got[len(got)-len(suffix):] == suffix {
					hasUnit = true
					break
				}
			}
		}
		if !hasUnit {
			t.Errorf("FormatBytes(%d) = %q, expected unit suffix", tc.input, got)
		}
	}
}

func TestDirSizeCalculation(t *testing.T) {
	dir := t.TempDir()
	// Create 3 files of 100 bytes each
	for i := 0; i < 3; i++ {
		f := filepath.Join(dir, "file"+string(rune('a'+i))+".txt")
		os.WriteFile(f, make([]byte, 100), 0644)
	}

	req := Estimate(EstimateOpts{
		DataFilePaths: []string{dir},
		Imports:       []string{"numpy"},
	})

	if req.Tier != "filesize" {
		t.Fatalf("expected tier=filesize, got %s", req.Tier)
	}
	// 300 bytes × 3 + numpy(200MB) + python(100MB) ≈ 300MB
	if req.MemBytes < 200*MB {
		t.Fatalf("estimate too low: %d", req.MemBytes)
	}
}

// Gap 1+4: Historical estimation uses QueryPeakMem from job history
func TestTier1HistoricalEstimation(t *testing.T) {
	// Point job history at a temp dir
	tmpDir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", tmpDir)

	scriptPath := "/absolute/path/to/train.py"

	// Create 3 fake job history records with PeakMemBytes
	peaks := []int64{2 * GB, 3 * GB, 1 * GB} // max = 3GB
	for i, peak := range peaks {
		jobID := "100000000" + string(rune('0'+i))
		jobDir := filepath.Join(tmpDir, "pipedpeer", "jobs", jobID)
		os.MkdirAll(jobDir, 0755)
		rec := jobhistory.Record{
			ID:           jobID,
			Status:       "succeeded",
			ScriptPath:   scriptPath,
			PeakMemBytes: peak,
		}
		b, _ := json.Marshal(rec)
		os.WriteFile(filepath.Join(jobDir, "metadata.json"), b, 0644)
	}

	// QueryPeakMem should find these
	found := jobhistory.QueryPeakMem(scriptPath, 5)
	if len(found) != 3 {
		t.Fatalf("expected 3 peaks, got %d", len(found))
	}

	// Estimate should use Tier 1 (historical)
	req := Estimate(EstimateOpts{
		ScriptPath: scriptPath,
		Imports:    []string{"torch"}, // should be ignored in favor of historical
	})

	if req.Tier != "historical" {
		t.Fatalf("expected tier=historical, got %s", req.Tier)
	}

	// Newest first: 1, 3, 2 GiB; the 3 GiB run is one run old, so it counts
	// 0.8x: 2.4 GiB x 1.2 headroom.
	if want := hist(3 * GB * peakDecay); req.MemBytes != want {
		t.Fatalf("expected %d (3 GiB x 0.8 x 1.2), got %d", want, req.MemBytes)
	}
}

// Gap 4: QueryPeakMem with no matching scripts
func TestQueryPeakMemNoMatch(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", tmpDir)

	// Create a job with a different script path
	jobDir := filepath.Join(tmpDir, "pipedpeer", "jobs", "999999999")
	os.MkdirAll(jobDir, 0755)
	rec := jobhistory.Record{
		ID:           "999999999",
		ScriptPath:   "/other/script.py",
		PeakMemBytes: 5 * GB,
	}
	b, _ := json.Marshal(rec)
	os.WriteFile(filepath.Join(jobDir, "metadata.json"), b, 0644)

	// Query for a different script → empty
	peaks := jobhistory.QueryPeakMem("/my/train.py", 5)
	if len(peaks) != 0 {
		t.Fatalf("expected 0 peaks for non-matching script, got %d", len(peaks))
	}
}

// writeRuns writes history records for script, oldest first, each with args
// and a peak.
func writeRuns(t *testing.T, script string, runs []jobhistory.Record) {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("XDG_DATA_HOME", tmp)
	for i, r := range runs {
		r.ID = fmt.Sprintf("2000000%03d", i)
		r.ScriptPath = script
		dir := filepath.Join(tmp, "pipedpeer", "jobs", r.ID)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(r)
		if err := os.WriteFile(filepath.Join(dir, "metadata.json"), b, 0644); err != nil {
			t.Fatal(err)
		}
	}
}

// W10-EST: one big-argv run used to set the estimate for every argv of the
// script (HEAD: 1.2 x 8 GiB = 9.6 GiB for a 200 MiB run).
func TestHistoricalKeysOnArgs(t *testing.T) {
	script := "/abs/train.py"
	small := []string{"--rows", "1000"}
	writeRuns(t, script, []jobhistory.Record{
		{Args: small, PeakMemBytes: 200 * MB},
		{Args: small, PeakMemBytes: 200 * MB},
		{Args: []string{"--rows", "1e9"}, PeakMemBytes: 8 * GB},
	})
	before := int64(float64(slices.Max(jobhistory.QueryPeakMem(script, maxHistoryRuns))) * historyBuffer)
	got := Estimate(EstimateOpts{ScriptPath: script, Args: small})
	t.Logf("small-argv estimate: before %s, after %s", FormatBytes(before), FormatBytes(got.MemBytes))
	if got.Tier != "historical" || got.MemBytes != 240*MB {
		t.Fatalf("small argv: got %s (%s), want 240 MiB historical", FormatBytes(got.MemBytes), got.Tier)
	}
	big := Estimate(EstimateOpts{ScriptPath: script, Args: []string{"--rows", "1e9"}})
	if want := hist(8 * GB); big.MemBytes != want {
		t.Fatalf("big argv: got %d, want %d", big.MemBytes, want)
	}
	// An argv never seen falls back to every run of the script, decayed:
	// newest 8 GiB counts in full.
	other := Estimate(EstimateOpts{ScriptPath: script, Args: []string{"--rows", "5"}})
	if want := hist(8 * GB); other.MemBytes != want {
		t.Fatalf("unseen argv: got %d, want %d", other.MemBytes, want)
	}
}

func TestHistoricalPeaksDecay(t *testing.T) {
	script := "/abs/etl.py"
	writeRuns(t, script, []jobhistory.Record{
		{PeakMemBytes: 10 * GB}, // oldest of four: counts 0.8^3 = 0.512
		{PeakMemBytes: 1 * GB},
		{PeakMemBytes: 1 * GB},
		{PeakMemBytes: 1 * GB}, // newest
	})
	got := Estimate(EstimateOpts{ScriptPath: script})
	want := hist(10 * GB * 0.512)
	if d := got.MemBytes - want; d < -MB || d > MB {
		t.Fatalf("got %s, want ~%s", FormatBytes(got.MemBytes), FormatBytes(want))
	}
}

func TestAdmitIdleFallback(t *testing.T) {
	cases := []struct {
		req, avail int64
		idle, want bool
	}{
		{0, 1, false, true},
		{1 * GB, 2 * GB, false, true},
		{4 * GB, 2 * GB, false, false}, // busy node: the estimate holds
		{4 * GB, 2 * GB, true, true},   // idle node: admit, the cap catches overruns
		{4 * GB, 0, true, false},       // nothing free at all
	}
	for _, c := range cases {
		if got := Admit(c.req, c.avail, c.idle, "test"); got != c.want {
			t.Errorf("Admit(%d, %d, %v) = %v, want %v", c.req, c.avail, c.idle, got, c.want)
		}
	}
}

// hist is the historical estimate for a decayed peak of p bytes.
func hist(p float64) int64 { return int64(p * historyBuffer) }
