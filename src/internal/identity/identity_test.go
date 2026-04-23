package identity

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetOrCreateGeneratesUUID(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("XDG_DATA_HOME", tmp)

	id, err := GetOrCreate()
	if err != nil {
		t.Fatalf("GetOrCreate failed: %v", err)
	}
	if id.NodeID == "" {
		t.Fatal("expected non-empty node ID")
	}
	// UUID v4 format: 8-4-4-4-12
	parts := strings.Split(id.NodeID, "-")
	if len(parts) != 5 {
		t.Fatalf("expected UUID format (5 groups), got: %s", id.NodeID)
	}
	if id.Arch == "" {
		t.Fatal("expected non-empty arch")
	}

	// Verify file was written
	path := filepath.Join(tmp, "pipedpeer", "node_identity.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("identity file not created: %v", err)
	}
}

func TestIdentityStableAcrossCalls(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("XDG_DATA_HOME", tmp)

	id1, err := GetOrCreate()
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	id2, err := GetOrCreate()
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}
	if id1.NodeID != id2.NodeID {
		t.Fatalf("expected same ID across calls, got %s and %s", id1.NodeID, id2.NodeID)
	}
}

func TestShortIDAndMatchesID(t *testing.T) {
	id := NodeIdentity{NodeID: "a1b2c3d4-5678-4abc-9def-0123456789ab"}
	if id.ShortID() != "a1b2c3d4" {
		t.Fatalf("expected a1b2c3d4, got %s", id.ShortID())
	}
	if !id.MatchesID("a1b2c3d4") {
		t.Fatal("expected short prefix match")
	}
	if !id.MatchesID(id.NodeID) {
		t.Fatal("expected full ID match")
	}
	if id.MatchesID("xxxxxxxx") {
		t.Fatal("should not match wrong ID")
	}
}
