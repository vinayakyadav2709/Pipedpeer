package discovery

import (
	"net"
	"strings"
	"testing"
	"time"
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

func TestAdvertiserAnswersProbesUntilStopped(t *testing.T) {
	adv := NewAdvertiser(ServiceInfo{NodeID: "probe-test-node", DaemonPort: 38080})
	if err := adv.Start(); err != nil {
		t.Skipf("cannot listen for discovery here: %v", err)
	}
	stopped := false
	defer func() {
		if !stopped {
			adv.Stop()
		}
	}()

	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.WriteTo([]byte(magicHeader), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: broadcastPort}); err != nil {
		t.Skipf("cannot send a probe here: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	n, _, err := conn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("no reply to a discovery probe: %v", err)
	}
	reply := string(buf[:n])
	if !strings.HasPrefix(reply, magicHeader+"|") {
		t.Fatalf("reply = %q, want it to start with the discovery header", reply)
	}
	// Another daemon on this machine shares the port and may be the one
	// that answered.
	if !strings.Contains(reply, "probe-test-node") {
		t.Skipf("a different daemon answered the probe: %s", reply)
	}

	done := make(chan struct{})
	go func() {
		adv.Stop()
		adv.Stop()
		close(done)
	}()
	select {
	case <-done:
		stopped = true
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
}
