package discovery

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// DiscoveredNode is a node found via LAN discovery.
type DiscoveredNode struct {
	NodeID      string `json:"node_id"`
	DaemonAddr  string `json:"daemon_addr"`   // "10.0.1.5:38080"
	SSHEndpoint string `json:"ssh_endpoint"`  // "root@10.0.1.5:22"
	Arch        string `json:"arch"`
}

// ServiceInfo is what each daemon advertises on the network.
// For now we use a simple UDP broadcast approach (can be upgraded to mDNS later).
type ServiceInfo struct {
	NodeID      string `json:"node_id"`
	DaemonPort  int    `json:"daemon_port"`
	SSHEndpoint string `json:"ssh_endpoint"`
	Arch        string `json:"arch"`
	Hostname    string `json:"hostname"`
	CPUPercent  float64 `json:"cpu_percent"`
	MemPercent  float64 `json:"memory_percent"`
	ActiveJobs  int    `json:"active_jobs"`
}

const (
	broadcastPort = 38099
	magicHeader   = "PIPEDPEER_DISC_V1"
)

// Advertiser broadcasts this node's presence on the LAN.
type Advertiser struct {
	info   ServiceInfo
	stopCh chan struct{}
	doneCh chan struct{}
}

// NewAdvertiser creates a LAN advertiser.
func NewAdvertiser(info ServiceInfo) *Advertiser {
	return &Advertiser{
		info:   info,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start begins broadcasting presence and responding to discovery requests.
func (a *Advertiser) Start() error {
	conn, err := net.ListenPacket("udp4", fmt.Sprintf(":%d", broadcastPort))
	if err != nil {
		// Port might be in use (multiple nodes on same machine in tests)
		// Try with a random port as fallback
		conn, err = net.ListenPacket("udp4", ":0")
		if err != nil {
			return fmt.Errorf("listen discovery: %w", err)
		}
	}

	go a.respondLoop(conn)
	return nil
}

func (a *Advertiser) respondLoop(conn net.PacketConn) {
	defer close(a.doneCh)
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		select {
		case <-a.stopCh:
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			continue
		}

		msg := string(buf[:n])
		if msg != magicHeader {
			continue
		}

		// Respond with our info
		data, _ := json.Marshal(a.info)
		response := magicHeader + "|" + string(data)
		_, _ = conn.WriteTo([]byte(response), addr)
	}
}

// Stop halts the advertiser.
func (a *Advertiser) Stop() {
	close(a.stopCh)
	<-a.doneCh
}

// Discover finds nodes on the local network.
// Sends a broadcast probe and collects responses.
// Timeout controls how long to wait for responses (default 1s).
func Discover(timeout time.Duration) []DiscoveredNode {
	if timeout == 0 {
		timeout = 1 * time.Second
	}

	conn, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		return nil
	}
	defer conn.Close()

	// Send broadcast
	broadcastAddr := &net.UDPAddr{IP: net.IPv4bcast, Port: broadcastPort}
	_, _ = conn.WriteTo([]byte(magicHeader), broadcastAddr)

	// Also try all local subnet broadcasts
	for _, bcast := range localBroadcasts() {
		_, _ = conn.WriteTo([]byte(magicHeader), &net.UDPAddr{IP: bcast, Port: broadcastPort})
	}

	// Collect responses
	var mu sync.Mutex
	var results []DiscoveredNode
	seen := make(map[string]bool)

	deadline := time.Now().Add(timeout)
	buf := make([]byte, 4096)

	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			continue
		}

		msg := string(buf[:n])
		if len(msg) <= len(magicHeader)+1 {
			continue
		}
		if msg[:len(magicHeader)] != magicHeader {
			continue
		}

		jsonPart := msg[len(magicHeader)+1:]
		var info ServiceInfo
		if err := json.Unmarshal([]byte(jsonPart), &info); err != nil {
			continue
		}

		mu.Lock()
		if !seen[info.NodeID] {
			seen[info.NodeID] = true
			results = append(results, DiscoveredNode{
				NodeID:      info.NodeID,
				DaemonAddr:  fmt.Sprintf("%s:%d", extractHost(info.SSHEndpoint), info.DaemonPort),
				SSHEndpoint: info.SSHEndpoint,
				Arch:        info.Arch,
			})
		}
		mu.Unlock()
	}

	return results
}

// DiscoverAsNodeRecords wraps Discover and returns registry.NodeRecord slice
// suitable for the coordinator. Probes each discovered node's daemon for
// capabilities and load info.
func DiscoverAsNodeRecords(timeout time.Duration) []registry.NodeRecord {
	discovered := Discover(timeout)
	var records []registry.NodeRecord
	client := &http.Client{Timeout: 2 * time.Second}

	for _, d := range discovered {
		// Try to get full info from daemon health endpoint
		resp, err := client.Get(fmt.Sprintf("http://%s/health", d.DaemonAddr))
		rec := registry.NodeRecord{
			NodeID:      d.NodeID,
			SSHEndpoint: d.SSHEndpoint,
			Capabilities: map[string]string{"arch": d.Arch},
			State:        "healthy",
			HealthScore:  0.5, // provisional score — discovered nodes get lower confidence
		}
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				rec.HealthScore = 0.7 // validated via health check
			}
		}
		records = append(records, rec)
	}
	return records
}

// localBroadcasts returns broadcast addresses for all local interfaces.
func localBroadcasts() []net.IP {
	var addrs []net.IP
	ifaces, err := net.Interfaces()
	if err != nil {
		return addrs
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagBroadcast == 0 {
			continue
		}
		ifAddrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range ifAddrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok || ipNet.IP.To4() == nil {
				continue
			}
			// Calculate broadcast: IP | ~mask
			ip := ipNet.IP.To4()
			mask := ipNet.Mask
			bcast := make(net.IP, 4)
			for i := range ip {
				bcast[i] = ip[i] | ^mask[i]
			}
			addrs = append(addrs, bcast)
		}
	}
	return addrs
}

func extractHost(sshEndpoint string) string {
	// Parse "root@10.0.1.5:22" → "10.0.1.5"
	// Also handles "10.0.1.5:22" → "10.0.1.5"
	hostPart := sshEndpoint
	for i, c := range sshEndpoint {
		if c == '@' {
			hostPart = sshEndpoint[i+1:]
			break
		}
	}
	for j, c := range hostPart {
		if c == ':' {
			return hostPart[:j]
		}
	}
	return hostPart
}
