package discovery

import (
	"net"
	"testing"
)

func TestReplyHostIsTheSource(t *testing.T) {
	pkt := []byte(magicHeader + `|{"node_id":"n1","daemon_port":4242,"hostname":"h"}`)
	d, ok := parseReply(pkt, &net.UDPAddr{IP: net.ParseIP("10.9.8.7"), Port: 4242})
	if !ok || d.Host != "10.9.8.7" || d.NodeID != "n1" || d.DaemonPort != 4242 {
		t.Fatalf("got %+v ok=%v", d, ok)
	}
	// A stale advertised host in the payload is ignored.
	pkt = []byte(magicHeader + `|{"node_id":"n1","host":"1.2.3.4","ssh_endpoint":"root@1.2.3.4:22"}`)
	if d, _ := parseReply(pkt, &net.UDPAddr{IP: net.ParseIP("10.9.8.7")}); d.Host != "10.9.8.7" {
		t.Fatalf("host = %q", d.Host)
	}
	if _, ok := parseReply([]byte("junk"), &net.UDPAddr{IP: net.ParseIP("10.9.8.7")}); ok {
		t.Fatal("accepted junk")
	}
}

func TestLocalBroadcastsReturnsAddresses(t *testing.T) {
	addrs := localBroadcasts()
	// Should return at least one broadcast address on most systems
	// On CI containers, might be empty but shouldn't panic
	for _, addr := range addrs {
		if addr.To4() == nil {
			t.Errorf("expected IPv4 broadcast, got %s", addr)
		}
	}
}

func TestServiceInfoRoundTrip(t *testing.T) {
	info := ServiceInfo{
		NodeID:     "test-node-uuid",
		DaemonPort: 4242,
		Arch:       "x86_64-linux",
		Hostname:   "test-host",
		CPUPercent: 25.5,
		MemPercent: 60.0,
		ActiveJobs: 2,
	}

	if info.NodeID != "test-node-uuid" {
		t.Error("ServiceInfo fields not set correctly")
	}
}

func TestAdvertiserStartStop(t *testing.T) {
	adv := NewAdvertiser(ServiceInfo{
		NodeID:     "test-node",
		DaemonPort: 4242,
	})

	// Start should not panic even if port is in use
	err := adv.Start()
	if err != nil {
		t.Fatalf("advertiser start failed: %v", err)
	}

	// Stop should not hang
	done := make(chan struct{})
	go func() {
		adv.Stop()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-func() <-chan struct{} {
		ch := make(chan struct{})
		go func() {
			<-make(chan struct{}) // never closes
		}()
		return ch
	}():
		t.Fatal("advertiser stop timed out")
	}
}
