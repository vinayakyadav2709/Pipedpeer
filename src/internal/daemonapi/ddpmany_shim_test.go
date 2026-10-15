package daemonapi

import (
	"encoding/json"
	"fmt"
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

// TestShimDiLoCoRegrowFollowsTheTable: a join that grows a DiLoCo ring
// moves it to the table's interval for the new size (FIX-DILOCO-JOIN). Past
// the table an auto ring goes back to exact sync; an explicit --ddp-sync
// fast keeps the last row's interval and warns.
func TestShimDiLoCoRegrowFollowsTheTable(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	if err := nixgen.WriteShimDir(dir); err != nil {
		t.Fatal(err)
	}
	last := dilocoRanks[len(dilocoRanks)-1]
	script := fmt.Sprintf(`
from pipedpeer_shim import ddp_diloco as d
from pipedpeer_shim.ddp_fast import _Fast
class Ring(_Fast):
    def __init__(self, auto):
        self.fast, self.auto_sync, self.sync_tuned, self.rank = True, auto, %[1]d, 0
d._diloco_use_every(%[1]d)
r = Ring(True)
r._diloco_regrow(3)
assert r.fast and d._DILOCO_FIXED_H == %[2]d, d._DILOCO_FIXED_H
r._diloco_regrow(%[3]d)
assert not r.fast and r.sync_tuned is None
d._diloco_use_every(%[1]d)
r = Ring(False)
r._diloco_regrow(%[3]d)
assert r.fast and d._DILOCO_FIXED_H == %[4]d, d._DILOCO_FIXED_H
print("REGROW-OK")
`, dilocoEvery(2), dilocoEvery(3), last.upTo+1, last.every)
	cmd := exec.Command(python, "-c", script)
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir, "PIPEDPEER_DILOCO_H=")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "REGROW-OK") {
		t.Fatalf("%v\n%s", err, out)
	}
	if !strings.Contains(string(out), "syncs every step again") {
		t.Errorf("the auto ring went exact without saying so:\n%s", out)
	}
	if !strings.Contains(string(out), "validated for up to") {
		t.Errorf("the explicit fast ring past the table did not warn:\n%s", out)
	}
}
