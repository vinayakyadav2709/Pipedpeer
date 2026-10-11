package app

import "testing"

func TestBuildUsesAPathFlakeRef(t *testing.T) {
	if got, want := flakeRef("x86_64-linux"), "path:.#packages.x86_64-linux.default"; got != want {
		t.Fatalf("flakeRef = %q, want %q", got, want)
	}
}
