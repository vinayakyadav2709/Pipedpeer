package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/ddpplace"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
)

// TestGPULessSubmitterIsLeftOutOfTheRing is the venue's GPU step: a college PC
// with no GPU submits, and the ring must be the two GPU laptops only. The PC
// is a healthy node in its own /v1/nodes, so it is a candidate like any other;
// only the device-kind rule keeps it out.
func TestGPULessSubmitterIsLeftOutOfTheRing(t *testing.T) {
	// The fake daemon serves /v1/nodes; every node points back at it, and its
	// /v1/bench 404s, so placement falls back to advertised cores at once.
	var port int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/nodes" {
			http.NotFound(w, r)
			return
		}
		ep := "root@127.0.0.1:22"
		gpu := registry.LoadInfo{GPUModel: "RTX 3060", TotalCPUs: 8, AvailableMemBytes: 8 << 30}
		_ = json.NewEncoder(w).Encode([]registry.NodeRecord{
			{NodeID: "pc-submitter", SSHEndpoint: ep, DaemonPort: port, State: "healthy",
				Load: registry.LoadInfo{TotalCPUs: 32, AvailableMemBytes: 16 << 30}},
			{NodeID: "laptop-a", SSHEndpoint: ep, DaemonPort: port, State: "healthy", Load: gpu},
			{NodeID: "laptop-b", SSHEndpoint: ep, DaemonPort: port, State: "healthy", Load: gpu},
		})
	}))
	defer srv.Close()
	_, p, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ = strconv.Atoi(p)

	for _, tc := range []struct {
		name               string
		require, preferGPU bool
	}{
		{"gpu force", true, true},
		{"gpu prefer", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ring, _, _ := ddpChooseRing(ddpRunOptions{
				DaemonPort: port, Nodes: 3, RequireGPU: tc.require, PreferGPU: tc.preferGPU,
			}, registry.NodeRecord{})
			if len(ring) != 2 {
				t.Fatalf("ring has %d members, want the 2 GPU laptops: %v", len(ring), ring)
			}
			for _, n := range ring {
				if n.NodeID == "pc-submitter" {
					t.Errorf("the GPU-less submitter is in the GPU ring")
				}
			}
		})
	}

	// The same rule on bare candidates, so a change to it fails here even
	// if ring placement is reworked around it.
	got := ddpOneDeviceKind([]ddpplace.Candidate{
		{NodeID: "pc-submitter", Cores: 32},
		{NodeID: "laptop-a", HasGPU: true, Cores: 8},
		{NodeID: "laptop-b", HasGPU: true, Cores: 8},
	}, true)
	if len(got) != 2 || got[0].NodeID == "pc-submitter" || got[1].NodeID == "pc-submitter" {
		t.Errorf("ddpOneDeviceKind kept %v, want laptop-a and laptop-b", got)
	}

	// eligible count must agree with the ring, or --ddp asks for a rank the
	// ring cannot place.
	if n := ddpEligibleCount(port, true, 0, resourceest.ParseMemString("1G")); n != 2 {
		t.Errorf("ddpEligibleCount = %d under --gpu force, want 2", n)
	}
}
