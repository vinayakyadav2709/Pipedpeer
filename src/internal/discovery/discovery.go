package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// DiscoveredNode is a node found via LAN discovery.
type DiscoveredNode struct {
	NodeID      string `json:"node_id"`
	DaemonAddr  string `json:"daemon_addr"` // "10.0.1.5:38080"
	DaemonPort  int    `json:"daemon_port"`
	SSHEndpoint string `json:"ssh_endpoint"` // "root@10.0.1.5:22"
	Arch        string `json:"arch"`
	Hostname    string `json:"hostname"`
}

// ServiceInfo is what each daemon advertises on the network.
// For now we use a simple UDP broadcast approach (can be upgraded to mDNS later).
type ServiceInfo struct {
	NodeID      string  `json:"node_id"`
	DaemonPort  int     `json:"daemon_port"`
	SSHEndpoint string  `json:"ssh_endpoint"`
	Arch        string  `json:"arch"`
	Hostname    string  `json:"hostname"`
	CPUPercent  float64 `json:"cpu_percent"`
	MemPercent  float64 `json:"memory_percent"`
	ActiveJobs  int     `json:"active_jobs"`
}

const (
	broadcastPort = 38099
	// multicastGroup carries probes between daemons on the same host. A UDP
	// broadcast sent to a port that several sockets share is delivered by the
	// kernel to only ONE of them, so co-located daemons (a lab, containers on
	// one machine, several devices behind one router) would never see each
	// other. Multicast with SO_REUSEADDR delivers every probe to all joined
	// sockets; broadcast stays for discovery across hosts.
	multicastGroup = "239.255.0.99"
	magicHeader    = "PIPEDPEER_DISC_V1"
)

// listenConfig reuses the port so several daemons on one host can share the
// discovery address. Without REUSEADDR/REUSEPORT the second daemon fails to
// bind and silently falls back to a random port, missing probes.
var listenConfig = net.ListenConfig{
	Control: func(network, address string, c syscall.RawConn) error {
		var serr error
		err := c.Control(func(fd uintptr) {
			for _, opt := range []int{unix.SO_REUSEADDR, unix.SO_REUSEPORT} {
				if e := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, opt, 1); e != nil {
					serr = e
					return
				}
			}
		})
		if serr != nil {
			return serr
		}
		return err
	},
}

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
	// Two listeners, one responder loop each: the multicast group catches
	// probes from same-host daemons, the broadcast port catches probes from
	// other machines. Both reuse the port so they never conflict.
	a.doneCh = make(chan struct{}, 2)

	mcast := &net.UDPAddr{IP: net.ParseIP(multicastGroup), Port: broadcastPort}
	conn, err := listenConfig.ListenPacket(context.Background(), "udp4", mcast.String())
	if err != nil {
		return fmt.Errorf("listen multicast: %w", err)
	}
	go a.respondLoop(conn)

	bc, err := listenConfig.ListenPacket(context.Background(), "udp4", fmt.Sprintf(":%d", broadcastPort))
	if err != nil {
		// Same-host share is handled by multicast; a free broadcast port is
		// best-effort (another daemon already holds it and responds on it).
		// Stop() waits on doneCh with a bounded timeout, so dropping the
		// second listener just means one fewer response path.
		a.doneCh <- struct{}{}
		return nil
	}
	go a.respondLoop(bc)
	return nil
}

func (a *Advertiser) respondLoop(conn net.PacketConn) {
	defer conn.Close()
	defer func() {
		select {
		case a.doneCh <- struct{}{}:
		default:
		}
	}()

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

		// Respond with our info, back to whoever probed.
		data, _ := json.Marshal(a.info)
		response := magicHeader + "|" + string(data)
		_, _ = conn.WriteTo([]byte(response), addr)
	}
}

// Stop halts the advertiser.
func (a *Advertiser) Stop() {
	select {
	case <-a.stopCh:
		return
	default:
	}
	close(a.stopCh)
	time.Sleep(600 * time.Millisecond)
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

	// Same-host daemons: probe the multicast group too.
	_, _ = conn.WriteTo([]byte(magicHeader), &net.UDPAddr{IP: net.ParseIP(multicastGroup), Port: broadcastPort})

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
				DaemonPort:  info.DaemonPort,
				SSHEndpoint: info.SSHEndpoint,
				Arch:        info.Arch,
			})
		}
		mu.Unlock()
	}

	return results
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
