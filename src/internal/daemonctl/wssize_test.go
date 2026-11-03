package daemonctl

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckWorkspaceSize(t *testing.T) {
	cases := []struct {
		name    string
		files   map[string]int // path -> bytes
		ignore  string
		limit   int64
		total   int64
		tooBig  bool
		inMsg   []string // must appear, in this order
		notInMs []string
	}{
		{"small project passes", map[string]int{"job.py": 10, "lib/a.py": 20}, "", 100, 30, false, nil, nil},
		{"over the limit refuses and names the big entry", map[string]int{"job.py": 10, "data/x.bin": 500, "src/a": 50}, "", 100, 560, true, []string{"data", "src"}, nil},
		{"ignore file with dir line brings it under", map[string]int{"job.py": 10, "data/x.bin": 500}, "data/\n", 100, 10, false, nil, nil},
		{"anchored ignore skips only the top-level dir", map[string]int{"build/a": 500, "sub/build/b": 7}, "/build\n", 100, 7, false, nil, nil},
		{"glob ignore", map[string]int{"a.log": 500, "b.py": 5}, "*.log\n", 100, 5, false, nil, nil},
		{".git and node_modules are not counted", map[string]int{".git/pack": 900, "node_modules/x/y": 900, "j.py": 1}, "", 100, 1, false, nil, nil},
		{"exactly at the limit passes", map[string]int{"a": 100}, "", 100, 100, false, nil, nil},
		{"biggest first, at most five listed", map[string]int{"a": 10, "b": 20, "c": 30, "d": 40, "e": 50, "f": 60, "g": 70}, "", 100, 280, true, []string{"g", "f", "e", "d", "c"}, []string{"bytes  b", "bytes  a"}},
		{"message names both remedies", map[string]int{"a": 500}, "", 100, 500, true, []string{".pipedpeerignore", "--allow-large-workspace"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			for p, n := range c.files {
				full := filepath.Join(dir, p)
				if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(full, make([]byte, n), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if c.ignore != "" {
				if err := os.WriteFile(filepath.Join(dir, ".pipedpeerignore"), []byte(c.ignore), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			total, _, err := EstimateWorkspace(dir)
			if err != nil {
				t.Fatal(err)
			}
			ign := int64(len(c.ignore))
			if c.ignore != "" && !strings.Contains(c.ignore, "xx") && total != c.total+ign {
				t.Errorf("total = %d, want %d (+%d for the ignore file)", total, c.total, ign)
			} else if c.ignore == "" && total != c.total {
				t.Errorf("total = %d, want %d", total, c.total)
			}
			err = CheckWorkspaceSize(dir, c.limit)
			var big *WorkspaceTooLargeError
			if got := errors.As(err, &big); got != c.tooBig {
				t.Fatalf("too large = %v (%v), want %v", got, err, c.tooBig)
			}
			if err == nil {
				return
			}
			msg := err.Error()
			pos := 0
			for _, s := range c.inMsg {
				i := strings.Index(msg[pos:], s)
				if i < 0 {
					t.Fatalf("message lacks %q in order:\n%s", s, msg)
				}
				pos += i + len(s)
			}
			for _, s := range c.notInMs {
				if strings.Contains(msg, s) {
					t.Errorf("message has %q:\n%s", s, msg)
				}
			}
		})
	}
}
