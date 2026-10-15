package daemonapi

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/nixgen"
)

// TestDiLoCoRanksMatchShim: the daemon's interval table (dilocoRanks) and
// the shim's (_DILOCO_RANKS) are one rule. An auto ring takes the lead's H,
// an explicit --ddp-sync fast ring the shim's, so a drift between the two
// would validate one and run the other.
func TestDiLoCoRanksMatchShim(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	if err := nixgen.WriteShimDir(dir); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, "-c",
		"import json; from pipedpeer_shim import ddp; print(json.dumps(ddp._DILOCO_RANKS, separators=(',', ':')))")
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("reading the shim's table: %v: %s", err, out)
	}
	var want [][2]int
	for _, r := range dilocoRanks {
		want = append(want, [2]int{r.upTo, r.every})
	}
	w, _ := json.Marshal(want)
	if got := strings.TrimSpace(string(out)); got != string(w) {
		t.Errorf("shim _DILOCO_RANKS = %s, daemon dilocoRanks = %s", got, w)
	}
}
