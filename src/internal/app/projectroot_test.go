package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindProjectRootHome(t *testing.T) {
	home := t.TempDir()
	outside := t.TempDir() // a different tree than $HOME
	mk := func(p string) string {
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cases := []struct {
		name   string
		marker string // dir given a .git, "" none
		script string
		want   string
	}{
		{"repo below home is the root", filepath.Join(home, "proj"), filepath.Join(home, "proj", "sub"), filepath.Join(home, "proj")},
		{"dotfiles repo at home is never a project", home, filepath.Join(home, "a", "b"), filepath.Join(home, "a", "b")},
		{"script directly in home keeps home", home, home, home},
		{"repo outside home unchanged", filepath.Join(outside, "r"), filepath.Join(outside, "r", "x"), filepath.Join(outside, "r")},
		{"ignore file at home is never a project", "", filepath.Join(home, "c"), filepath.Join(home, "c")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("HOME", home)
			script := mk(c.script)
			if c.marker != "" {
				mk(filepath.Join(mk(c.marker), ".git"))
			} else {
				os.WriteFile(filepath.Join(home, ".pipedpeerignore"), nil, 0o644)
			}
			defer os.RemoveAll(filepath.Join(c.marker, ".git"))
			defer os.Remove(filepath.Join(home, ".pipedpeerignore"))
			if got := findProjectRoot(script); got != c.want {
				t.Errorf("root = %s, want %s", got, c.want)
			}
		})
	}
}
