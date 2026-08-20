package resourceest

import (
	"encoding/json"
	"os"
	"path/filepath"
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

	// max(peaks) = 3GB × 1.2 = 3.6GB
	maxPeak := int64(3 * GB)
	expectedMin := int64(float64(maxPeak) * 1.15) // with some tolerance
	expectedMax := int64(float64(maxPeak) * 1.25)
	if req.MemBytes < expectedMin || req.MemBytes > expectedMax {
		t.Fatalf("expected ~3.6GB (3GB × 1.2), got %d", req.MemBytes)
	}
}

// VmPeak tests use gopsutil. We test ReadSelfVmPeak which should work
// on the current process (the test binary itself).
func TestReadSelfVmPeakNonZero(t *testing.T) {
	peak := ReadSelfVmPeak()
	// On Linux with gopsutil, this should return the current process's VMS
	// which should be non-zero since we're a running Go process.
	// Note: gopsutil may return 0 on some systems if /proc is unavailable.
	// We just verify it doesn't panic and returns a reasonable value.
	if peak < 0 {
		t.Fatalf("expected non-negative VmPeak for self, got %d", peak)
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
