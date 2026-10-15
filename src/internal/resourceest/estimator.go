package resourceest

import (
	"log"
	"os"
	"path/filepath"
	"slices"
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
	peakDecay       = 0.8      // each older run's peak counts 0.8x the next newer one
)

// Estimate returns the estimated resource requirements for a job.
// Priority: userOverride > historical > filesize > import baselines.
func Estimate(opts EstimateOpts) ResourceReq {
	// Tier 0: User override
	if opts.UserMemBytes > 0 {
		return ResourceReq{MemBytes: opts.UserMemBytes, Tier: "user"}
	}

	// Tier 1: Historical peak from previous runs of the same script, with the
	// same arguments when there are any (`train.py --rows 1e6` and
	// `train.py --rows 1e3` are different jobs); runs with other arguments
	// only when this argv has never run.
	if opts.ScriptPath != "" {
		if est := historicalEstimate(opts.ScriptPath, opts.Args); est > 0 {
			return ResourceReq{MemBytes: est, Tier: "historical"}
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
	Args          []string // the script's argv after the script path
	DataFilePaths []string // paths to data files being synced
	Imports       []string // detected Python imports
	UserMemBytes  int64    // user-provided override (0 = auto)
}

// EstimateFromScript is a convenience that scans imports automatically.
func EstimateFromScript(scriptPath string, dataFiles []string, userMem int64) ResourceReq {
	return EstimateForRun(scriptPath, nil, dataFiles, userMem)
}

// EstimateForRun is EstimateFromScript keyed on the script's arguments too.
func EstimateForRun(scriptPath string, args, dataFiles []string, userMem int64) ResourceReq {
	imports := pythondeps.ExtractImports(scriptPath)
	return Estimate(EstimateOpts{
		ScriptPath:    scriptPath,
		Args:          args,
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

// historicalEstimate is 1.2x the largest decayed peak of the last runs of
// scriptPath with exactly these args, or of any args when none match; 0 when
// the script never recorded a peak. Older peaks decay by run (newest counts
// in full, each older one peakDecay times less), so one heavy run long ago
// stops holding every later run to its size.
func historicalEstimate(scriptPath string, args []string) int64 {
	absPath, err := filepath.Abs(scriptPath)
	if err != nil {
		log.Printf("resourceest: no absolute path for %s (%v); looking up history by it as given", scriptPath, err)
		absPath = scriptPath
	}
	// The last 50 runs of the script: enough to find this argv's last few
	// among other argvs, bounded so a long history stays cheap to scan.
	runs := jobhistory.QueryRuns(absPath, 50)
	var same, anyArgs []int64
	for _, r := range runs {
		if len(same) < maxHistoryRuns && slices.Equal(r.Args, args) {
			same = append(same, r.PeakMemBytes)
		}
		if len(anyArgs) < maxHistoryRuns {
			anyArgs = append(anyArgs, r.PeakMemBytes)
		}
	}
	peaks := same
	if len(peaks) == 0 {
		peaks = anyArgs
	}
	return int64(decayedPeak(peaks) * historyBuffer)
}

// decayedPeak is max over newest-first peaks of peak_k * peakDecay^k.
func decayedPeak(newestFirst []int64) float64 {
	var best float64
	w := 1.0
	for _, p := range newestFirst {
		best = max(best, float64(p)*w)
		w *= peakDecay
	}
	return best
}

// Admit reports whether a job estimated at required bytes may start where
// available bytes are free. An estimate that does not fit is still admitted
// when nothing else runs there (idle) and some memory is free: the estimate is a forecast that can
// overshoot, and refusing an idle node stalls a job that may well fit, while
// the job's memory cap catches a real overrun. where names the node for the
// log line.
func Admit(required, available int64, idle bool, where string) bool {
	if required <= 0 || available >= required {
		return true
	}
	if !idle || available <= 0 {
		return false
	}
	log.Printf("resourceest: admitting a job estimated at %s on idle %s with %s available: estimates overshoot, the job's memory cap still holds",
		FormatBytes(required), where, FormatBytes(available))
	return true
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
