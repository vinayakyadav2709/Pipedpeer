package heartbeat

import (
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/shirou/gopsutil/v4/cpu"

	"github.com/pipedpeer/pipedpeer/internal/gpu"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/narpack"
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
)

// Role says what this node will do for the cluster: "" (a full node),
// RolePoolOnly or RoleIntroducerOnly. The daemon sets it at start from
// cluster.json; it travels as the "role" capability, and the coordinator, DDP
// eligibility and job admission skip any node that has one. Pool spill skips
// only introducer-only nodes.
var Role string

// Version is the build this daemon runs (main.version), advertised as the
// "version" capability so a peer on a different build can be named.
var Version string

// Roles a node can advertise. Absent means a full node.
const (
	RolePoolOnly       = "pool-only"
	RoleIntroducerOnly = "introducer-only"
)

// RoleCapability and VersionCapability are where Role and Version travel.
const (
	RoleCapability    = "role"
	VersionCapability = "version"
)

// NixKeyCapability is where a node publishes the key it signs store paths
// with. Travels with the node record so a peer can decide to trust this
// cluster's signatures specifically, rather than switching signature checking
// off for the whole machine.
const NixKeyCapability = "nix_pubkey"

// ClosureFormatCapability is where a node says which closure transfer
// formats it can receive.
//
// A sender needs this because the two formats are not interchangeable: an
// older node handed a signature-carrying archive would try to read it as an
// export stream and fail somewhere inside nix. Advertised rather than probed,
// so a peer that has not been upgraded keeps receiving what it understands.
const ClosureFormatCapability = "closure_formats"

// Capabilities describes a node's static hardware: things that do not change
// between heartbeats, so the scheduler can rank raw capability separately from
// current load. Values are strings because they travel in NodeRecord.Capabilities.
func Capabilities(id identity.NodeIdentity) map[string]string {
	caps := map[string]string{
		"arch":     id.Arch,
		"hostname": id.Hostname,
	}
	if Role != "" {
		caps[RoleCapability] = Role
	}
	if Version != "" {
		caps[VersionCapability] = Version
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

	// The transfer formats this node can receive. Signatures only survive
	// the second one, so a peer that sees it here can send a closure this
	// machine will accept with require-sigs left on.
	caps[ClosureFormatCapability] = narpack.FormatName

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
