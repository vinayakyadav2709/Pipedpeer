package daemonctl

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkspaceTarFailureLeavesNothing(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name string
		ctx  context.Context
		dir  func(t *testing.T) string
	}{
		{"missing project dir", context.Background(), func(t *testing.T) string { return filepath.Join(t.TempDir(), "nope") }},
		{"cancelled before tar", cancelled, func(t *testing.T) string {
			d := t.TempDir()
			os.WriteFile(filepath.Join(d, "a.py"), []byte("x"), 0o644)
			return d
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dest := filepath.Join(t.TempDir(), "workspace.tar")
			if err := os.WriteFile(dest, []byte("partial"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := CreateWorkspaceTarContext(c.ctx, c.dir(t), dest, nil); err == nil {
				t.Fatal("want an error")
			}
			if _, err := os.Stat(dest); !os.IsNotExist(err) {
				t.Errorf("partial tar left behind: %v", err)
			}
		})
	}
}
