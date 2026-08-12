package resourceest

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dustin/go-humanize"

	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/pythondeps"
)

// ResourceReq describes the estimated resource requirements for a job.
type ResourceReq struct {
	MemBytes int64  `json:"mem_bytes"` // estimated memory in bytes
	Tier     string `json:"tier"`      // "user", "historical", "filesize", "import"
}

// Known library memory baselines in bytes.
var libBaselines = map[string]int64{
	"torch":      2 * GB,
	"tensorflow": 3 * GB,
	"keras":      1 * GB,
	"pandas":     300 * MB,
	"numpy":      200 * MB,
	"scipy":      400 * MB,
	"sklearn":    500 * MB,
	"matplotlib": 200 * MB,
	"opencv":     300 * MB,
	"PIL":        150 * MB,
	"sqlalchemy": 100 * MB,
	"celery":     100 * MB,
	"fastapi":    80 * MB,
	"django":     150 * MB,
	"flask":      60 * MB,
}

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB

	pythonBaseline  = 100 * MB // base Python interpreter
	defaultEstimate = 512 * MB // absolute fallback
	fileMultiplier  = 3        // data files expand ~3x in memory
	historyBuffer   = 1.2      // 20% headroom over historical peak
	maxHistoryRuns  = 5        // look at last N runs
)

// Estimate returns the estimated resource requirements for a job.
// Priority: userOverride > historical > filesize > import baselines.
func Estimate(opts EstimateOpts) ResourceReq {
	// Tier 0: User override
	if opts.UserMemBytes > 0 {
		return ResourceReq{MemBytes: opts.UserMemBytes, Tier: "user"}
	}

	// Tier 1: Historical peak from previous runs of same script
	if opts.ScriptPath != "" {
		peaks := queryHistoricalPeaks(opts.ScriptPath)
		if len(peaks) > 0 {
			maxPeak := peaks[len(peaks)-1] // sorted ascending
			estimated := int64(float64(maxPeak) * historyBuffer)
			if estimated > 0 {
				return ResourceReq{MemBytes: estimated, Tier: "historical"}
			}
		}
	}

	// Tier 2: File size based
	if len(opts.DataFilePaths) > 0 {
		totalSize := sumFileSizes(opts.DataFilePaths)
		if totalSize > 0 {
			libBase := importBaseline(opts.Imports)
			estimated := totalSize*fileMultiplier + libBase + pythonBaseline
			return ResourceReq{MemBytes: estimated, Tier: "filesize"}
		}
	}

	// Tier 3: Import-based baseline
	if len(opts.Imports) > 0 {
		libBase := importBaseline(opts.Imports)
		if libBase > 0 {
			return ResourceReq{MemBytes: libBase + pythonBaseline, Tier: "import"}
		}
	}

	// Absolute fallback
	return ResourceReq{MemBytes: defaultEstimate, Tier: "import"}
}

// EstimateOpts configures the estimator.
type EstimateOpts struct {
	ScriptPath    string   // path to the script (for historical lookup)
	DataFilePaths []string // paths to data files being synced
	Imports       []string // detected Python imports
	UserMemBytes  int64    // user-provided override (0 = auto)
}

// EstimateFromScript is a convenience that scans imports automatically.
func EstimateFromScript(scriptPath string, dataFiles []string, userMem int64) ResourceReq {
	imports := pythondeps.ExtractImports(scriptPath)
	return Estimate(EstimateOpts{
		ScriptPath:    scriptPath,
		DataFilePaths: dataFiles,
		Imports:       imports,
		UserMemBytes:  userMem,
	})
}

// importBaseline sums the known library baselines for the given imports.
func importBaseline(imports []string) int64 {
	var total int64
	seen := make(map[string]bool)
	for _, imp := range imports {
		imp = strings.ToLower(imp)
		if seen[imp] {
			continue
		}
		seen[imp] = true
		if baseline, ok := libBaselines[imp]; ok {
			total += baseline
		}
	}
	return total
}

// sumFileSizes returns the total size of all given files.
func sumFileSizes(paths []string) int64 {
	var total int64
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		if info.IsDir() {
			total += dirSize(p)
		} else {
			total += info.Size()
		}
	}
	return total
}

// dirSize recursively sums file sizes in a directory.
func dirSize(path string) int64 {
	var total int64
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

// queryHistoricalPeaks returns the peak memory values from the last N runs
// of the same script, sorted ascending.
func queryHistoricalPeaks(scriptPath string) []int64 {
	absPath, err := filepath.Abs(scriptPath)
	if err != nil {
		absPath = scriptPath
	}

	peaks := jobhistory.QueryPeakMem(absPath, maxHistoryRuns)
	sort.Slice(peaks, func(i, j int) bool { return peaks[i] < peaks[j] })
	return peaks
}

// ParseMemString parses a human-readable memory string like "10G", "512M", "2048K".
// Returns bytes. Returns 0 if unparseable.
func ParseMemString(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}

	// go-humanize ParseBytes handles: "10GB", "512MB", "2048KB", "10G", "512M", etc.
	bytes, err := humanize.ParseBytes(s)
	if err != nil {
		return 0
	}
	return int64(bytes)
}

// FormatBytes returns a human-readable byte string using go-humanize.
func FormatBytes(b int64) string {
	return humanize.IBytes(uint64(b))
}
