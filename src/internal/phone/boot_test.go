package phone

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// bootFixture is a Termux with its own HOME (where ~/.termux/boot lives) and
// the install record under PREFIX; no real service is touched.
func bootFixture(t *testing.T) {
	t.Helper()
	fakePhone(t)
	t.Setenv("HOME", t.TempDir())
}

// Case table for the boot script (P6): no script, same content, different
// content, F-Droid build, record for uninstall, unwritable HOME.
func TestWriteBootCases(t *testing.T) {
	const play = "googleplay.2026.10.09"
	const fdroid = "0.118.3"

	t.Run("1 no script: written and OK", func(t *testing.T) {
		bootFixture(t)
		var out bytes.Buffer
		if err := writeBoot(&out, play); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(bootPath())
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != bootScript {
			t.Errorf("content = %q, want %q", b, bootScript)
		}
		if !strings.Contains(out.String(), "boot") {
			t.Errorf("no OK line about the boot script: %q", out.String())
		}
		if fi, _ := os.Stat(bootPath()); fi.Mode().Perm() != 0o755 {
			t.Errorf("mode = %v, want 0755", fi.Mode().Perm())
		}
	})

	t.Run("2 same content: untouched, already", func(t *testing.T) {
		bootFixture(t)
		if err := writeBoot(&bytes.Buffer{}, play); err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
		if err := os.Chtimes(bootPath(), old, old); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := writeBoot(&out, play); err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(bootPath())
		if err != nil {
			t.Fatal(err)
		}
		if !fi.ModTime().Equal(old) {
			t.Errorf("mtime changed to %v: the script was rewritten", fi.ModTime())
		}
		if !strings.Contains(out.String(), "already") {
			t.Errorf("no 'already' line: %q", out.String())
		}
	})

	t.Run("3 different content: rewritten", func(t *testing.T) {
		bootFixture(t)
		if err := os.MkdirAll(filepath.Dir(bootPath()), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(bootPath(), []byte("#!/bin/sh\necho old\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := writeBoot(&bytes.Buffer{}, play); err != nil {
			t.Fatal(err)
		}
		b, _ := os.ReadFile(bootPath())
		if string(b) != bootScript {
			t.Errorf("content = %q, want the new script", b)
		}
	})

	t.Run("3b existing file with wrong mode: mode fixed to 0755", func(t *testing.T) {
		bootFixture(t)
		if err := os.MkdirAll(filepath.Dir(bootPath()), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(bootPath(), []byte(bootScript), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := writeBoot(&bytes.Buffer{}, play); err != nil {
			t.Fatal(err)
		}
		if fi, _ := os.Stat(bootPath()); fi.Mode().Perm() != 0o755 {
			t.Errorf("mode = %v, want 0755", fi.Mode().Perm())
		}
	})

	t.Run("4 F-Droid build: note printed", func(t *testing.T) {
		bootFixture(t)
		var out bytes.Buffer
		if err := writeBoot(&out, fdroid); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "Termux:Boot") {
			t.Errorf("no Termux:Boot note on a non-Play build: %q", out.String())
		}
	})

	t.Run("4b Play build: no note", func(t *testing.T) {
		bootFixture(t)
		var out bytes.Buffer
		if err := writeBoot(&out, play); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), "Termux:Boot") {
			t.Errorf("Termux:Boot note on the Play build: %q", out.String())
		}
	})

	t.Run("5 recorded so uninstall removes it", func(t *testing.T) {
		bootFixture(t)
		if err := writeBoot(&bytes.Buffer{}, play); err != nil {
			t.Fatal(err)
		}
		r, found := loadRecord()
		if !found || !slices.Contains(r.Paths, bootPath()) {
			t.Errorf("record Paths = %v (found %v), want %s", r.Paths, found, bootPath())
		}
	})

	t.Run("6 HOME unwritable: error returned, no panic", func(t *testing.T) {
		bootFixture(t)
		home := os.Getenv("HOME")
		// A file where ~/.termux should be makes MkdirAll fail.
		if err := os.WriteFile(filepath.Join(home, ".termux"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := writeBoot(&bytes.Buffer{}, play); err == nil {
			t.Error("want an error when the boot dir cannot be created")
		}
	})
}
