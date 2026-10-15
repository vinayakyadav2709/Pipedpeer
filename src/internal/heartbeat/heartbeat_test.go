package heartbeat

import (
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/identity"
)

func TestCapabilitiesCarryRoleAndVersion(t *testing.T) {
	id := identity.NodeIdentity{NodeID: "n", Arch: "amd64", Hostname: "h"}

	Role, Version = "", ""
	caps := Capabilities(id)
	if _, ok := caps[RoleCapability]; ok {
		t.Fatalf("a full node must not advertise a role: %v", caps)
	}
	if _, ok := caps[VersionCapability]; ok {
		t.Fatalf("an unset version must not be advertised: %v", caps)
	}

	Role, Version = RolePoolOnly, "0.1.4-test"
	t.Cleanup(func() { Role, Version = "", "" })
	caps = Capabilities(id)
	if caps[RoleCapability] != RolePoolOnly || caps[VersionCapability] != "0.1.4-test" {
		t.Fatalf("role/version not advertised: %v", caps)
	}
}

func TestCollectLoadReturnsValues(t *testing.T) {
	load := CollectLoad(3, 1024*1024*100) // 3 jobs, 100MB reserved
	// On Linux: should return real values. On non-Linux: returns 0.
	// Either way, values should be non-negative.
	if load.CPUPercent < 0 || load.MemPercent < 0 {
		t.Fatalf("load values should be non-negative: cpu=%.1f mem=%.1f", load.CPUPercent, load.MemPercent)
	}
	if load.ActiveJobs != 3 {
		t.Fatalf("expected ActiveJobs=3, got %d", load.ActiveJobs)
	}
	if load.ReservedMemBytes != 1024*1024*100 {
		t.Fatalf("expected ReservedMemBytes=100MB, got %d", load.ReservedMemBytes)
	}
	if load.TotalCPUs < 1 {
		t.Fatalf("expected at least 1 CPU, got %d", load.TotalCPUs)
	}
}
