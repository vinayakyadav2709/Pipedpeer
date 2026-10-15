package runrec

import "testing"

// E6: a --ddp 2 run printed no "ran on" line because each rank's record is
// saved alone; merged, the ring's machines share the script time.
func TestMergedRankRecordsNameEveryMachine(t *testing.T) {
	a := Record{RunID: "r0", TotalNS: 10e9, ScriptNS: 8e9, Names: map[string]string{"ka": "yeet"},
		Steps: []Step{{Machine: "ka", Name: "script", Chunk: -1, DurNS: 8e9}}}
	b := Record{RunID: "r1", TotalNS: 11e9, ScriptNS: 9e9, Names: map[string]string{"kb": "fedora"},
		Steps: []Step{{Machine: "kb", Name: "script", Chunk: -1, DurNS: 8e9}}}
	m := Merge(a, b)
	if m.RunID != "r0" || m.TotalNS != 11e9 || m.ScriptNS != 9e9 || len(m.Steps) != 2 {
		t.Fatalf("merged %+v", m)
	}
	s := Simple(m)
	if len(s.Where) != 2 || s.Where[0].Share != 0.5 {
		t.Fatalf("ran on %+v, want yeet and fedora at 50%% each", s.Where)
	}
	for _, p := range s.Where {
		if p.Machine != "yeet" && p.Machine != "fedora" {
			t.Errorf("machine %q is not named", p.Machine)
		}
	}
}
