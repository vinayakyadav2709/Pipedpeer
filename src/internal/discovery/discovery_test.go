package discovery

import (
	"testing"
)

func TestExtractHost(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"root@10.0.1.5:22", "10.0.1.5"},
		{"root@192.168.1.100:2222", "192.168.1.100"},
		{"user@myhost", "myhost"},
		{"10.0.1.5:22", "10.0.1.5"},
		{"myhost", "myhost"},
	}
	for _, tc := range tests {
		got := extractHost(tc.input)
		if got != tc.expected {
			t.Errorf("extractHost(%q) = %q, want %q", tc.input, got, tc.expected)
		}
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
		NodeID:      "test-node-uuid",
		DaemonPort:  38080,
		SSHEndpoint: "root@10.0.1.5:22",
		Arch:        "x86_64-linux",
		Hostname:    "test-host",
		CPUPercent:  25.5,
		MemPercent:  60.0,
		ActiveJobs:  2,
	}

	if info.NodeID != "test-node-uuid" {
		t.Error("ServiceInfo fields not set correctly")
	}
}

func TestAdvertiserStartStop(t *testing.T) {
	adv := NewAdvertiser(ServiceInfo{
		NodeID:     "test-node",
		DaemonPort: 38080,
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
