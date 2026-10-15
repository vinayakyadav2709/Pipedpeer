package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// TestDDPEligibleAdmitsIdleNodesUnderTheEstimate (FIX-WORLD1): an idle node
// whose grantable memory is under the estimate counts, as single-node
// placement admits it; a busy one is turned down with a reason the CLI prints.
func TestDDPEligibleAdmitsIdleNodesUnderTheEstimate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node := func(id string, avail int64, jobs int) registry.NodeRecord {
			return registry.NodeRecord{NodeID: id, SSHEndpoint: "root@127.0.0.1:22", State: "healthy",
				Load: registry.LoadInfo{TotalCPUs: 2, AvailableMemBytes: avail, ActiveJobs: jobs}}
		}
		_ = json.NewEncoder(w).Encode([]registry.NodeRecord{
			node("lead", 1<<30, 0), node("idle", 600<<20, 0), node("busy", 600<<20, 1),
		})
	}))
	defer srv.Close()
	_, p, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(p)

	n, refused := ddpEligibleRanks(port, false, 0, 900<<20, "")
	if n != 2 {
		t.Errorf("eligible = %d, want 2 (lead + idle)", n)
	}
	if len(refused) != 1 || refused[0].node != "busy" || !refused[0].busy {
		t.Fatalf("refused = %+v, want busy only, marked busy", refused)
	}
	if line := ddpRefusalLine(refused); !strings.Contains(line, "busy: memory: need 900 MiB") {
		t.Errorf("refusal line %q names neither node nor need", line)
	}
}
