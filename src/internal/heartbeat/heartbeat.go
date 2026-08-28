package heartbeat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"

	"github.com/pipedpeer/pipedpeer/internal/gpu"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/logging"
	"github.com/pipedpeer/pipedpeer/internal/natsbus"
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// JobCounterFunc returns the current number of active jobs.
type JobCounterFunc func() int

// ReservedMemFunc returns the total reserved memory in bytes.
type ReservedMemFunc func() int64

// Transport defines how heartbeats are sent.
type Transport string

const (
	TransportHTTP Transport = "http"
	TransportNATS Transport = "nats"
)

type Client struct {
	registryURL   string
	identity      identity.NodeIdentity
	sshEndpoint   string
	daemonPort    int
	interval      time.Duration
	httpClient    *http.Client
	stopCh        chan struct{}
	doneCh        chan struct{}
	stopOnce      sync.Once
	jobCounter    JobCounterFunc
	reservedMemFn ReservedMemFunc
	transport     Transport
	bus           *natsbus.Bus
}

// NewClient creates a heartbeat client using HTTP transport (backwards compatible).
func NewClient(registryURL string, id identity.NodeIdentity, sshEndpoint string, daemonPort int) *Client {
	return &Client{
		registryURL:   registryURL,
		identity:      id,
		sshEndpoint:   sshEndpoint,
		daemonPort:    daemonPort,
		interval:      10 * time.Second,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		jobCounter:    func() int { return 0 },
		reservedMemFn: func() int64 { return 0 },
		transport:     TransportHTTP,
	}
}

// NewNATSClient creates a heartbeat client using NATS transport.
func NewNATSClient(bus *natsbus.Bus, id identity.NodeIdentity, sshEndpoint string, daemonPort int) *Client {
	return &Client{
		identity:      id,
		sshEndpoint:   sshEndpoint,
		daemonPort:    daemonPort,
		interval:      10 * time.Second,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		jobCounter:    func() int { return 0 },
		reservedMemFn: func() int64 { return 0 },
		transport:     TransportNATS,
		bus:           bus,
	}
}

// SetJobCounter sets the function that returns the current active job count.
func (c *Client) SetJobCounter(fn JobCounterFunc) {
	c.jobCounter = fn
}

// SetReservedMemFn sets the function that returns total reserved memory.
func (c *Client) SetReservedMemFn(fn ReservedMemFunc) {
	c.reservedMemFn = fn
}

// Start registers with the registry and begins the heartbeat loop.
func (c *Client) Start() error {
	if err := c.register(); err != nil {
		// Registry may be unavailable — log warning but don't fail.
		// The daemon can still work via mDNS discovery.
		log := logging.WithComponent("heartbeat")
		log.Warn().Err(err).Str("node_id", c.identity.NodeID).Msg("registry registration failed (will retry)")
	}
	go c.loop()
	return nil
}

// SetInterval changes the heartbeat interval (for testing).
func (c *Client) SetInterval(d time.Duration) {
	c.interval = d
}

// Stop sends a deregister signal and stops the heartbeat loop.
// Safe to call multiple times.
func (c *Client) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		<-c.doneCh
	})
	_ = c.deregister()
}

// StopWithoutDeregister stops the heartbeat loop without sending deregister.
// Simulates a node crash. Safe to call multiple times.
func (c *Client) StopWithoutDeregister() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		<-c.doneCh
	})
}

// NixKeyCapability is where a node publishes the key it signs store paths
// with. Travels with the node record so a peer can decide to trust this
// cluster's signatures specifically, rather than switching signature checking
// off for the whole machine.
const NixKeyCapability = "nix_pubkey"

// Capabilities describes a node's static hardware: things that do not change
// between heartbeats, so the scheduler can rank raw capability separately from
// current load. Values are strings because they travel in NodeRecord.Capabilities.
func Capabilities(id identity.NodeIdentity) map[string]string {
	caps := map[string]string{
		"arch":     id.Arch,
		"hostname": id.Hostname,
	}
	// Which physical machine this is, so a daemon sharing one with a peer can
	// tell. Hostname will not do: containers get their own.
	if m := Machine(); m != "" {
		caps[MachineCapability] = m
	}

	// The key this node signs its store exports with, so peers can trust
	// what it sends without trusting everything. Derived from the node
	// identity, so a peer can check it against the fingerprint the
	// connection was authenticated under rather than believing the claim.
	if k, err := identity.Key(); err == nil {
		caps[NixKeyCapability] = nixsign.PublicKey(k)
	}

	// CPU: core count, model and clock. Cached — cpu.Info() shells into
	// /proc/cpuinfo and is called on every heartbeat.
	cores, model, mhz := cpuStatic()
	caps["cpu_cores"] = strconv.Itoa(cores)
	if model != "" {
		caps["cpu_model"] = model
	}
	if mhz > 0 {
		caps["cpu_mhz"] = strconv.FormatFloat(mhz, 'f', 0, 64)
	}

	if id.GPU.Name != "" {
		caps["gpu"] = string(id.GPU.Vendor)
		caps["gpu_name"] = id.GPU.Name
		caps["gpu_count"] = strconv.Itoa(id.GPU.Count)
		if id.GPU.MemoryBytes > 0 {
			caps["gpu_memory"] = strconv.FormatInt(id.GPU.MemoryBytes, 10)
		}
		if cc := gpu.ComputeCapability(); cc != "" {
			caps["gpu_compute_cap"] = cc
		}
	}
	return caps
}

var (
	cpuStaticOnce  sync.Once
	cpuStaticCores int
	cpuStaticModel string
	cpuStaticMHz   float64
)

func cpuStatic() (int, string, float64) {
	cpuStaticOnce.Do(func() {
		cpuStaticCores = runtime.NumCPU()
		infos, err := cpu.Info()
		if err != nil || len(infos) == 0 {
			return
		}
		cpuStaticModel = strings.TrimSpace(infos[0].ModelName)
		cpuStaticMHz = infos[0].Mhz

		// cpu.Info() returns one entry per physical package on some kernels and
		// one per logical CPU on others. runtime.NumCPU() is the reliable count,
		// so only the model and clock are taken from here.
	})
	return cpuStaticCores, cpuStaticModel, cpuStaticMHz
}

func (c *Client) buildNodeRecord() registry.NodeRecord {
	return registry.NodeRecord{
		NodeID:       c.identity.NodeID,
		SSHEndpoint:  c.sshEndpoint,
		DaemonPort:   c.daemonPort,
		Capabilities: Capabilities(c.identity),
		Load:         CollectLoad(c.jobCounter(), c.reservedMemFn()),
	}
}

func (c *Client) register() error {
	switch c.transport {
	case TransportNATS:
		return c.registerNATS()
	default:
		return c.registerHTTP()
	}
}

func (c *Client) heartbeat() error {
	switch c.transport {
	case TransportNATS:
		return c.heartbeatNATS()
	default:
		return c.heartbeatHTTP()
	}
}

func (c *Client) deregister() error {
	switch c.transport {
	case TransportNATS:
		return c.deregisterNATS()
	default:
		return c.deregisterHTTP()
	}
}

// --- HTTP transport ---

func (c *Client) registerHTTP() error {
	rec := c.buildNodeRecord()
	body, _ := json.Marshal(rec)
	resp, err := c.httpClient.Post(c.registryURL+"/v1/register", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("register returned %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) heartbeatHTTP() error {
	payload := struct {
		NodeID string            `json:"node_id"`
		Load   registry.LoadInfo `json:"load"`
	}{
		NodeID: c.identity.NodeID,
		Load:   CollectLoad(c.jobCounter(), c.reservedMemFn()),
	}
	body, _ := json.Marshal(payload)
	resp, err := c.httpClient.Post(c.registryURL+"/v1/heartbeat", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode == 404 {
		// Not registered — re-register
		return c.registerHTTP()
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("heartbeat returned %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) deregisterHTTP() error {
	payload := struct {
		NodeID string `json:"node_id"`
	}{NodeID: c.identity.NodeID}
	body, _ := json.Marshal(payload)
	resp, err := c.httpClient.Post(c.registryURL+"/v1/deregister", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// --- NATS transport ---

func (c *Client) registerNATS() error {
	rec := c.buildNodeRecord()
	return c.bus.PublishJSON("pipedpeer.registry.register", rec)
}

func (c *Client) heartbeatNATS() error {
	payload := struct {
		NodeID string            `json:"node_id"`
		Load   registry.LoadInfo `json:"load"`
	}{
		NodeID: c.identity.NodeID,
		Load:   CollectLoad(c.jobCounter(), c.reservedMemFn()),
	}
	return c.bus.PublishJSON("pipedpeer.registry.heartbeat", payload)
}

func (c *Client) deregisterNATS() error {
	payload := struct {
		NodeID string `json:"node_id"`
	}{NodeID: c.identity.NodeID}
	return c.bus.PublishJSON("pipedpeer.registry.deregister", payload)
}

func (c *Client) loop() {
	defer close(c.doneCh)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.heartbeat(); err != nil {
				log := logging.WithComponent("heartbeat")
				log.Warn().Err(err).Str("node_id", c.identity.NodeID).Msg("heartbeat failed")
			}
		case <-c.stopCh:
			return
		}
	}
}
