package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// A rank on an internet peer must sync to the lead through the peer's own
// forwarder: our LAN address is not on its network (real run: every rank 1
// failed "No route to host" in 7 s) and 127.0.0.1 there is the peer itself.
func TestARankOnAnInternetPeerSyncsThroughItsOwnForwarder(t *testing.T) {
	const selfMachine = "boot-a"
	lead := registry.NodeRecord{NodeID: "node-a", SSHEndpoint: "root@127.0.0.1:22", DaemonPort: 4242,
		Capabilities: map[string]string{heartbeat.MachineCapability: selfMachine}}

	// B's daemon: it knows the lead at its own forwarder port 41999.
	bList := []registry.NodeRecord{
		{NodeID: "node-b", SSHEndpoint: "root@127.0.0.1:22", DaemonPort: 4242},
		{NodeID: "node-a", SSHEndpoint: "root@127.0.0.1:22", DaemonPort: 41999},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/nodes" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(bList)
	}))
	defer srv.Close()
	_, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	b := registry.NodeRecord{NodeID: "node-b", SSHEndpoint: "root@127.0.0.1:22", DaemonPort: port,
		Capabilities: map[string]string{heartbeat.MachineCapability: "boot-b"}}

	got, err := ddpLeadAddrFor(b, lead, selfMachine, ddpFetchNodesAt)
	if err != nil || got != "127.0.0.1:41999" {
		t.Fatalf("internet peer: got %q, %v; want B's forwarder to the lead 127.0.0.1:41999", got, err)
	}

	// B with no path to the lead: refuse with a reason, never hand it an
	// address that cannot work.
	bList = bList[:1]
	if got, err := ddpLeadAddrFor(b, lead, selfMachine, ddpFetchNodesAt); err == nil {
		t.Fatalf("B without the lead: got %q, want an error", got)
	}

	// A LAN peer and a rank on this machine keep the shared lead address and
	// cost no request.
	noFetch := func(string, int) ([]registry.NodeRecord, error) {
		t.Fatal("asked a node that reaches the lead directly")
		return nil, nil
	}
	lan := registry.NodeRecord{NodeID: "node-c", SSHEndpoint: "root@192.168.1.7:22", DaemonPort: 4242}
	for _, n := range []registry.NodeRecord{lan, lead} {
		if got, err := ddpLeadAddrFor(n, lead, selfMachine, noFetch); err != nil || got != "" {
			t.Errorf("%s: got %q, %v; want the shared address", n.NodeID, got, err)
		}
	}
}

// A native torch.distributed script connects rank to rank on its own ports,
// which a forwarder does not carry: a node reached over the internet link is
// turned down with a reason (so a ring left with one rank falls back with the
// FIX-WORLD1 warning), and the ring never picks it. The shim's own sync is
// unaffected.
func TestNativeDDPTakesNoNodeBehindAForwarder(t *testing.T) {
	self := map[string]string{heartbeat.MachineCapability: "boot-a"}
	far := map[string]string{heartbeat.MachineCapability: "boot-b", "hostname": "fedora"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			_ = json.NewEncoder(w).Encode(map[string]any{"capabilities": self})
			return
		}
		_ = json.NewEncoder(w).Encode([]registry.NodeRecord{
			{NodeID: "node-a", SSHEndpoint: "root@127.0.0.1:22", State: "healthy", Capabilities: self},
			{NodeID: "node-b", SSHEndpoint: "root@127.0.0.1:22", State: "healthy", Capabilities: far},
		})
	}))
	defer srv.Close()
	_, p, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(p)

	if n, refused := ddpEligibleRanks(port, false, 0, 0, "", false); n != 2 || len(refused) != 0 {
		t.Fatalf("shim sync: eligible %d, refused %+v; want both nodes", n, refused)
	}
	n, refused := ddpEligibleRanks(port, false, 0, 0, "", true)
	if n != 1 || len(refused) != 1 || refused[0].node != "node-b" ||
		!strings.Contains(refused[0].reason, "fedora is reached over the internet link") {
		t.Fatalf("native: eligible %d, refused %+v; want node-b turned down for its internet link", n, refused)
	}
	o := ddpRunOptions{Native: true, SelfMachine: "boot-a"}
	if !ddpExcluded(o, registry.NodeRecord{NodeID: "node-b", SSHEndpoint: "root@127.0.0.1:22", Capabilities: far}) {
		t.Error("the native ring may pick a node behind a forwarder")
	}
	if ddpExcluded(o, registry.NodeRecord{NodeID: "node-c", SSHEndpoint: "root@192.168.1.7:22"}) {
		t.Error("the native ring refuses a LAN node")
	}
}
