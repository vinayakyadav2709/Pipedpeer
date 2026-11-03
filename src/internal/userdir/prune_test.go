package userdir

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPruneScratch(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", home)
	root := filepath.Join(Cache(), "scratch")
	now := time.Now()
	age := func(p string, h int) {
		ts := now.Add(-time.Duration(h) * time.Hour)
		if err := os.Chtimes(p, ts, ts); err != nil {
			t.Fatal(err)
		}
	}
	mkdir := func(name string, h int, owner string) {
		d := filepath.Join(root, name)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "f"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if owner != "" {
			if err := os.WriteFile(filepath.Join(d, ownerFile), []byte(owner), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		age(d, h)
	}
	dead := "2147483000" // far above pid_max
	cases := []struct {
		name  string
		h     int
		owner string
		kept  bool
	}{
		{"build-old", 48, "", false},
		{"build-new", 1, "", true},
		{"warm-old-dead-owner", 48, dead, false},
		{"warm-old-live-owner", 48, fmt.Sprint(os.Getpid()), true},
		{"shards-old", 30, "", false},
		{"warm-task-old", 30, "", false},
		{"pool-old", 48, "", true},
		{"build-just-under", 23, "", true},
	}
	for _, c := range cases {
		mkdir(c.name, c.h, c.owner)
	}
	if err := os.WriteFile(filepath.Join(root, "build-file"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	age(filepath.Join(root, "build-file"), 48)

	var logged []string
	removed := PruneScratch(24*time.Hour, now, func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) })

	for _, c := range cases {
		_, err := os.Stat(filepath.Join(root, c.name))
		if c.kept != (err == nil) {
			t.Errorf("%s: kept=%v want %v", c.name, err == nil, c.kept)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "build-file")); err != nil {
		t.Errorf("plain file removed")
	}
	if len(removed) != 4 || len(logged) != 4 {
		t.Errorf("removed %v logged %v, want 4 each", removed, logged)
	}
	for _, l := range logged {
		if !strings.Contains(l, "scratch") {
			t.Errorf("log line lacks the path: %q", l)
		}
	}
}

func TestScratchMarksOwner(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	d, err := Scratch("build-*")
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(d, ownerFile))
	if err != nil || string(b) != fmt.Sprint(os.Getpid()) {
		t.Fatalf("owner file = %q, %v", b, err)
	}
}
