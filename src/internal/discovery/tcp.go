package discovery

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// DiscoverTCP finds daemons by probing the local subnet's /health endpoints
// directly over HTTP. It exists for networks where the UDP discovery dies
// silently — APs with client isolation or multicast filtering drop broadcast
// packets without error, so Discover returns nothing. Cost is bounded: at
// most 254 addresses per subnet (docker bridges are often /16; daemons get
// low addresses so a first-slice cap always covers them), probed 64 at a
// time, and the scan stops as soon as any daemon answers.
func DiscoverTCP(daemonPort int, timeout time.Duration) []DiscoveredNode {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	deadline := time.Now().Add(timeout)

	self := map[string]bool{}
	for _, ip := range localIPs() {
		self[ip.String()] = true
	}

	seenJob := map[string]bool{}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 64)
	var mu sync.Mutex
	var results []DiscoveredNode
	seenNode := map[string]bool{}
	found := atomic.Bool{}

	for _, sub := range localSubnets() {
		base := binary.BigEndian.Uint32(sub.ip.To4())
		for i := uint32(1); i <= 254; i++ {
			ip := make(net.IP, 4)
			binary.BigEndian.PutUint32(ip, base+i)
			if self[ip.String()] || seenJob[ip.String()] {
				continue
			}
			seenJob[ip.String()] = true
			wg.Add(1)
			go func(ip string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				if found.Load() || time.Now().After(deadline) {
					return
				}
				d := probeHost(ip, daemonPort)
				if d == nil {
					return
				}
				mu.Lock()
				defer mu.Unlock()
				// Record every hit from probes already in flight — one scan
				// finds the whole sitting cluster — but stop issuing new
				// probes once something answered.
				if !seenNode[d.NodeID] {
					seenNode[d.NodeID] = true
					results = append(results, *d)
				}
				found.Store(true)
			}(ip.String())
		}
	}
	wg.Wait()
	return results
}

// probeHost asks one address whether a pipedpeer daemon answers there.
// Returns nil for anything that is not a pipedpeer daemon (connection
// refused, timeout, foreign HTTP service, malformed body).
func probeHost(ip string, port int) *DiscoveredNode {
	client := &http.Client{Timeout: 700 * time.Millisecond}
	resp, err := client.Get(fmt.Sprintf("http://%s:%d/health", ip, port))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var h struct {
		NodeID       string            `json:"node_id"`
		Capabilities map[string]string `json:"capabilities"`
	}
	if json.NewDecoder(resp.Body).Decode(&h) != nil || h.NodeID == "" {
		return nil
	}
	return &DiscoveredNode{
		NodeID:      h.NodeID,
		SSHEndpoint: fmt.Sprintf("root@%s:22", ip),
		DaemonPort:  port,
		Arch:        h.Capabilities["arch"],
		Hostname:    h.Capabilities["hostname"],
	}
}

// subnet is one local IPv4 network to sweep.
type subnet struct {
	ip   net.IP // an address inside the subnet (the interface address)
	mask net.IPMask
}

// localSubnets lists the IPv4 networks of every up, non-loopback interface.
func localSubnets() []subnet {
	var out []subnet
	ifaces, err := net.Interfaces()
	if err != nil {
		return out
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok || ipNet.IP.To4() == nil {
				continue
			}
			out = append(out, subnet{ip: ipNet.IP.To4(), mask: ipNet.Mask})
		}
	}
	return out
}

func localIPs() []net.IP {
	var out []net.IP
	for _, s := range localSubnets() {
		out = append(out, s.ip)
	}
	return out
}
