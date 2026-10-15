package heartbeat

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMachineKeyCombinesMachineIDAndBootID(t *testing.T) {
	dir := t.TempDir()
	file := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	missing := filepath.Join(dir, "missing")
	midA, midB := file("mid-a", "aaaa"), file("mid-b", "bbbb")
	boot1, boot2 := file("boot-1", "1111"), file("boot-2", "2222")

	// Two containers on one host: same boot id, own machine-ids -> differ.
	if machineKey(midA, boot1) == machineKey(midB, boot1) {
		t.Fatal("containers with distinct machine-ids got one key")
	}
	// Cloned PCs: same machine-id, different boots -> differ.
	if machineKey(midA, boot1) == machineKey(midA, boot2) {
		t.Fatal("cloned machines with distinct boot ids got one key")
	}
	// Same inputs -> same key (two daemons on one machine find each other).
	if k := machineKey(midA, boot1); k == "" || k != machineKey(midA, boot1) {
		t.Fatalf("key not stable: %q", k)
	}
	// Either part alone still gives a key; neither gives "unknown".
	if machineKey(missing, boot1) == "" || machineKey(midA, missing) == "" {
		t.Fatal("one missing part must not make the key unknown")
	}
	if k := machineKey(missing, missing); k != "" {
		t.Fatalf("no parts: key %q, want empty", k)
	}
}
