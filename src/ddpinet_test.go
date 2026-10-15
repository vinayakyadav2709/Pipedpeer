package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
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
