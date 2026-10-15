package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/netaddr"
	"golang.org/x/sys/unix"
)

// DiscoveredNode is a node found via LAN discovery.
type DiscoveredNode struct {
	NodeID     string `json:"node_id"`
	Host       string `json:"host"` // where the reply came from, never what the node claims
	DaemonPort int    `json:"daemon_port"`
	Arch       string `json:"arch"`
	Hostname   string `json:"hostname"`
}

// ServiceInfo is what each daemon advertises on the network.
// For now we use a simple UDP broadcast approach (can be upgraded to mDNS later).
type ServiceInfo struct {
	NodeID     string  `json:"node_id"`
	DaemonPort int     `json:"daemon_port"`
	Arch       string  `json:"arch"`
	Hostname   string  `json:"hostname"`
	CPUPercent float64 `json:"cpu_percent"`
	MemPercent float64 `json:"memory_percent"`
	ActiveJobs int     `json:"active_jobs"`
}

const (
	broadcastPort = 4242
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

// OffEnv turns LAN discovery off when set to "1": the daemon neither answers
// probes nor sends them. Test daemons set it. A daemon a test starts on a
// developer's laptop otherwise answered the lab's probes and showed up in a
// real cluster as a placeable "yeet" node (and found the lab's nodes itself).
const OffEnv = "PIPEDPEER_NO_LAN_DISCOVERY"

// Off reports whether LAN discovery is turned off by OffEnv.
func Off() bool { return os.Getenv(OffEnv) == "1" }

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
	if Off() {
		log.Printf("[discovery] LAN discovery off (%s=1): not answering probes", OffEnv)
		return nil
	}
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
	if Off() {
		return nil
	}
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
		n, src, err := conn.ReadFrom(buf)
		if err != nil {
			continue
		}
		d, ok := parseReply(buf[:n], src)
		if !ok {
			continue
		}
		mu.Lock()
		if !seen[d.NodeID] {
			seen[d.NodeID] = true
			results = append(results, d)
		}
		mu.Unlock()
	}

	return results
}

// localBroadcasts returns broadcast addresses for all local interfaces.
func localBroadcasts() []net.IP {
	var addrs []net.IP
	ifaces, err := netaddr.Interfaces()
	if err != nil {
		return addrs
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagBroadcast == 0 {
			continue
		}
		ifAddrs, err := netaddr.Addrs(iface)
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

// parseReply turns one probe reply into a node. The host is the packet's
// source address: a daemon advertises the address it had at start-up, which is
// stale after a network change.
func parseReply(pkt []byte, src net.Addr) (DiscoveredNode, bool) {
	msg := string(pkt)
	if len(msg) <= len(magicHeader)+1 || msg[:len(magicHeader)] != magicHeader {
		return DiscoveredNode{}, false
	}
	var info ServiceInfo
	if err := json.Unmarshal([]byte(msg[len(magicHeader)+1:]), &info); err != nil {
		return DiscoveredNode{}, false
	}
	udp, ok := src.(*net.UDPAddr)
	if !ok || info.NodeID == "" {
		return DiscoveredNode{}, false
	}
	return DiscoveredNode{
		NodeID:     info.NodeID,
		Host:       udp.IP.String(),
		DaemonPort: info.DaemonPort,
		Arch:       info.Arch,
		Hostname:   info.Hostname,
	}, true
}
