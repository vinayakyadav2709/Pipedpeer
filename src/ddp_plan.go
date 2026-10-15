package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/ddpplace"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/say"
)

// ddpOneDeviceKind keeps the ring to a single kind of device.
//
// --gpu force already excluded CPU nodes, but --gpu prefer filtered nothing:
// every node went into admission ranked by a probe that measures CPU
// throughput, so a GPU box with a modest CPU was rejected as "too slow to
// contribute" while a CPU box with a fast one was admitted, and the ring came
// out mixed - which the code two lines below says the shim cannot balance.
// The preference was applied afterwards, as a sort of the survivors, which
// cannot put back a node admission had already dropped.
//
// Mixed is the wrong answer anyway at the sizes measured here: the same model
// and batch takes 1.5s on this GPU and 56.2s on this CPU, and with equal
// batches every rank waits for the slowest. So the choice is which kind, not
// how to blend them - GPUs when there are any, CPUs when there are not.
//
// This is what makes the ring "only the good GPUs, then only the good CPUs":
// the kind is decided here, and which of that kind are worth having is decided
// by the marginal-gain rule in ddpplace.
func ddpOneDeviceKind(cands []ddpplace.Candidate, preferGPU bool) []ddpplace.Candidate {
	if !preferGPU {
		return cands
	}
	var gpu []ddpplace.Candidate
	for _, c := range cands {
		if c.HasGPU {
			gpu = append(gpu, c)
		}
	}
	if len(gpu) > 0 {
		return gpu
	}
	// No accelerator anywhere. CPU is what is left, and a CPU ring beats no
	// ring - "prefer" is not "require".
	return cands
}

// ddpUneven reports whether the shares differ enough to be worth mentioning.
// Placement measures live machines, so identical hardware still returns
// slightly different numbers; the shim uses the same threshold before it
// reshards, and the two must agree or the run says one thing and does
// another.
func ddpUneven(w []float64) bool {
	if len(w) < 2 {
		return false
	}
	lo, hi := w[0], w[0]
	for _, x := range w {
		if x < lo {
			lo = x
		}
		if x > hi {
			hi = x
		}
	}
	return hi-lo >= ddpEvenTolerance
}

// ddpEvenTolerance is the spread below which shares count as equal. Shared
// with the shim through PIPEDPEER_DDP_WEIGHTS: change one and change both.
const ddpEvenTolerance = 0.02

// ddpSharePercents renders shares for a person.
func ddpSharePercents(w []float64) string {
	parts := make([]string, len(w))
	for i, x := range w {
		parts[i] = fmt.Sprintf("rank %d %.0f%%", i, 100*x)
	}
	return strings.Join(parts, ", ")
}

// ddpWeightList renders the ring's shares for the shim.
func ddpWeightList(w []float64) string {
	parts := make([]string, len(w))
	for i, x := range w {
		parts[i] = strconv.FormatFloat(x, 'f', 6, 64)
	}
	return strings.Join(parts, ",")
}

// ddpNormaliseWeights makes the ring's shares sum to one.
//
// Placement normalises over everything it admitted, but the ring is then
// truncated to --ddp N, and shares that sum to less than one would hand out
// less than the whole dataset - every epoch would silently train on a
// fraction of it. Anything unusable falls back to equal shares, which is what
// the ring did before shares existed.
func ddpNormaliseWeights(w []float64) []float64 {
	var total float64
	for _, x := range w {
		if x <= 0 {
			return nil
		}
		total += x
	}
	if total <= 0 {
		return nil
	}
	out := make([]float64, len(w))
	for i, x := range w {
		out[i] = x / total
	}
	return out
}

// ddpSharesNode reports whether ring member i is on a machine that is also
// hosting another rank.
// ddpDistinctMachines counts the physical machines a ring spans.
//
// Not the same as the number of ranks. Two daemons on one laptop are two
// nodes with two identities and one set of cores, and the ring's own verdict
// on whether it was worth forming divides the work by the number of ranks -
// which on a co-located ring says the machine is twice as fast as itself.
// Measured: a two-rank ring on one machine reported "the ring paid for
// itself" on a run that took 191s against 92.8s for the same machine alone.
//
// The machine key is the one the roommate memory accounting already uses, so
// a ring learns nothing new here - it just stops ignoring what the daemon
// knew. Nodes that do not report one are counted separately: an unknown
// machine is not evidence of sharing.
// ddpSelfMachine is this machine's key, as the daemon reports it.
//
// Empty when the daemon cannot be asked, which makes ddpForwardedLead treat
// every loopback node as forwarded. That is the safe direction: it refuses a
// ring rather than building one whose ranks all sync to themselves.
func ddpSelfMachine(daemonPort int) string {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", daemonPort))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var h struct {
		Capabilities map[string]string `json:"capabilities"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&h) != nil {
		return ""
	}
	return h.Capabilities[heartbeat.MachineCapability]
}

// ddpForwardedLead reports whether a node is only reachable through a local
// forwarder, and so cannot be the rank everybody else syncs to.
//
// A peer reached over the internet is registered at 127.0.0.1 on a port this
// daemon forwards, which is what keeps health polling, closure upload and
// gradient sync from having to know how the bytes travel. It also means its
// recorded address is meaningless anywhere else: 127.0.0.1 on another rank is
// that rank.
//
// The star sync has every rank post gradients to the lead's daemon, so the
// lead's address has to mean the same thing to all of them. A loopback
// address qualifies only when it really is this machine, which is decided by
// the machine key the roommate accounting already collects - by host alone
// the two cases are identical, and reading a forwarded peer as "this machine"
// hands every rank an address pointing at itself.
func ddpForwardedLead(n registry.NodeRecord, selfMachine string) bool {
	host := ddpExtractHost(n.SSHEndpoint)
	if host != "" && host != "127.0.0.1" && host != "localhost" && host != "::1" {
		return false
	}
	// Loopback. Safe only if this is the machine we are running on. A node
	// too old to report a key counts as forwarded: assuming otherwise is the
	// failure this exists to prevent.
	key := n.Capabilities[heartbeat.MachineCapability]
	return key == "" || key != selfMachine
}

// ddpLeadIndex picks the ring member to lead it: one every rank can reach,
// and a GPU node whenever one can lead, so a mixed set never puts the sync
// and the reference model on a CPU (Finding 22: the CPU lead built a
// smaller net and every weight sync was refused). -1 when nothing in the
// ring can lead, which has to be said rather than papered over: picking one
// anyway hangs every rank at the first barrier with no indication why.
func ddpLeadIndex(ring []registry.NodeRecord, selfMachine string) int {
	first := -1
	for i, n := range ring {
		if ddpForwardedLead(n, selfMachine) {
			continue
		}
		if ddpHasGPU(n) {
			return i
		}
		if first < 0 {
			first = i
		}
	}
	return first
}

// ddpToFront moves s[i] to the front, keeping the others in order. Moved
// rather than dropped: a node that cannot lead is still a good rank.
func ddpToFront[T any](s []T, i int) []T {
	if i <= 0 || i >= len(s) {
		return s
	}
	out := make([]T, 0, len(s))
	out = append(out, s[i])
	out = append(out, s[:i]...)
	return append(out, s[i+1:]...)
}

// ddpProgressOut is where the ring's progress lines go: stdout under -v,
// nowhere otherwise.
func ddpProgressOut() io.Writer {
	if say.Verbose {
		return os.Stdout
	}
	return io.Discard
}

func ddpDistinctMachines(ring []registry.NodeRecord) int {
	seen := map[string]bool{}
	unknown := 0
	for _, n := range ring {
		key := n.Capabilities[heartbeat.MachineCapability]
		if key == "" {
			unknown++
			continue
		}
		seen[key] = true
	}
	n := len(seen) + unknown
	if n < 1 {
		n = 1
	}
	return n
}

// ddpLocalRanks maps each rank on a node that hosts two or more ranks, other
// than the lead's node, to the comma list of that node's ranks: they average
// on their own daemon first and send the lead one sync per machine (U26).
// The lead's node needs no such step: its ranks reach the lead's daemon
// without crossing the network. Empty when no node qualifies.
func ddpLocalRanks(ring []registry.NodeRecord) map[int]string {
	byNode := map[string][]string{}
	for i, n := range ring {
		if i > 0 && n.NodeID != ring[0].NodeID {
			byNode[n.NodeID] = append(byNode[n.NodeID], strconv.Itoa(i))
		}
	}
	out := map[int]string{}
	for i, n := range ring {
		if ranks := byNode[n.NodeID]; i > 0 && len(ranks) >= 2 {
			out[i] = strings.Join(ranks, ",")
		}
	}
	return out
}

// ddpPeerURLs is every rank's sync URL as rank i reaches it, in rank order,
// for the butterfly reduce: rank k's daemon averages shard k, so the lead's
// link carries 2(N-1)/N of a gradient instead of N-1 (research R1 #6). Empty
// below 3 ranks, where the hub costs the same, and when any member is reached
// only through this machine's forwarder: that address means nothing on
// another rank, so such a ring stays on the hub.
func ddpPeerURLs(ring []registry.NodeRecord, i int, selfMachine string) string {
	if len(ring) < 3 || i < 0 || i >= len(ring) {
		return ""
	}
	urls := make([]string, len(ring))
	for j, n := range ring {
		if ddpForwardedLead(n, selfMachine) {
			return ""
		}
		host := ddpExtractHost(n.SSHEndpoint)
		if host == "" || host == "127.0.0.1" || host == "localhost" || host == "::1" {
			// This machine: loopback for a rank here, else the address of
			// ours that routes to that rank (as for the lead).
			host = "127.0.0.1"
			if other := ddpExtractHost(ring[i].SSHEndpoint); other != "" && other != "127.0.0.1" &&
				other != "localhost" && other != "::1" {
				host = localIPFor(other)
			}
		}
		urls[j] = fmt.Sprintf("http://%s/v1/ddp/sync", net.JoinHostPort(host, strconv.Itoa(n.DaemonPort)))
	}
	return strings.Join(urls, ",")
}

func ddpSharesNode(ring []registry.NodeRecord, i int) bool {
	for j, n := range ring {
		if j != i && n.NodeID == ring[i].NodeID {
			return true
		}
	}
	return false
}

// ddpSlots is how many ranks a node can host at once.
//
// One per accelerator: a two-GPU machine given one rank trains on one GPU and
// leaves the other idle for the whole run. On a CPU node it stays at one -
// two ranks there would divide the same cores between them and pay the
// gradient sync on top, which is slower than one rank using all of them.
//
// PIPEDPEER_DDP_RANKS_PER_NODE overrides it. That exists to exercise the
// mechanism on hardware with a single GPU, where the multi-accelerator case
// cannot otherwise be run at all; it is not a tuning knob.
func ddpSlots(n registry.NodeRecord) int {
	if v := os.Getenv("PIPEDPEER_DDP_RANKS_PER_NODE"); v != "" {
		if k, err := strconv.Atoi(v); err == nil && k > 0 {
			return k
		}
	}
	if len(n.Load.GPUs) > 1 {
		return len(n.Load.GPUs)
	}
	if c, err := strconv.Atoi(n.Capabilities["gpu_count"]); err == nil && c > 1 {
		return c
	}
	return 1
}

func ddpHasGPU(n registry.NodeRecord) bool {
	if n.Capabilities["gpu"] != "" && n.Capabilities["gpu"] != "none" {
		return true
	}
	return n.Load.GPUModel != "" || len(n.Load.GPUs) > 0
}

// ddpEligibleCount returns how many DDP ranks this cluster could host.
// GPU nodes count when they fit the VRAM requirement; CPU nodes count as a
// fallback when they fit the job's memory (plan C1: GPU preferred, CPU only
// when it fits). requireGPU (--gpu force) hard-excludes CPU nodes.
//
// Ranks, not nodes. This gate runs before placement and gives up on the whole
// idea when it sees fewer than two, so counting nodes made a single machine
// with two accelerators ineligible for the ring it is perfectly able to host -
// and the multi-rank placement below it could never be reached.
func ddpEligibleCount(daemonPort int, requireGPU bool, requiredGPUMem, requiredMem int64) int {
	return ddpEligibleCountExcept(daemonPort, requireGPU, requiredGPUMem, requiredMem, "")
}

// ddpExcluded reports whether n may not take a rank: a node with a role
// (pool-only, introducer-only: C5), this node under NoSelf (--remote), and
// any node on the machine --offload keeps work off, by its machine key.
func ddpExcluded(o ddpRunOptions, n registry.NodeRecord) bool {
	if n.Capabilities[ddpRoleCapability] != "" {
		return true
	}
	if o.NoSelf && o.SelfNodeID != "" && n.NodeID == o.SelfNodeID {
		return true
	}
	return heartbeat.SameMachine(n.Capabilities[heartbeat.MachineCapability], o.ExcludeMachine)
}

// ddpRoleCapability marks a node that takes no DDP rank: "pool-only" or
// "introducer-only" (C5; absent on a full node).
const ddpRoleCapability = "role"

// ddpEligibleCountExcept is ddpEligibleCount without the nodes on the machine
// whose key is exclude ("" excludes nothing).
func ddpEligibleCountExcept(daemonPort int, requireGPU bool, requiredGPUMem, requiredMem int64, exclude string) int {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", daemonPort))
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var all []registry.NodeRecord
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		return 0
	}
	count := 0
	for _, n := range all {
		if n.State != "healthy" || n.Capabilities[ddpRoleCapability] != "" {
			continue
		}
		if heartbeat.SameMachine(n.Capabilities[heartbeat.MachineCapability], exclude) {
			continue
		}
		gpuOK := ddpHasGPU(n) &&
			(requiredGPUMem <= 0 || n.Load.GPUMemBytes-n.Load.GPUMemUsedBytes >= requiredGPUMem)
		cpuOK := !requireGPU &&
			(requiredMem <= 0 || n.Load.AvailableMemBytes >= requiredMem)
		if gpuOK || cpuOK {
			count += ddpSlots(n)
		}
	}
	return count
}

func ddpExtractHost(endpoint string) string {
	s := endpoint
	if idx := strings.Index(s, "@"); idx >= 0 {
		s = s[idx+1:]
	}
	if idx := strings.Index(s, ":"); idx >= 0 {
		s = s[:idx]
	}
	return s
}

// errRingNotWorthIt says every ring available measures slower than a single
// machine, so the caller should run the job normally rather than fail.
var errRingNotWorthIt = errors.New("a single node is faster than any ring available")

// ddpChoosePeers picks the nodes that will take ranks 1..N-1.
//
// This used to be a gate followed by a sort: keep GPU nodes if the job wants a
// GPU, otherwise CPU nodes, order by reported CPU load, take the first N. Both
// halves are wrong on a real cluster. Reported load and advertised cores say
// nothing about a cap the peer's daemon is under - the local test cluster's
// workers all advertise 16 cores while held to 8, 2 and 1, a 13x spread that
// no advertisement reveals. And in synchronous training every rank waits for
// the slowest, so a node a fraction of the ring's speed does not add capacity,
// it sets the pace: including it can leave the run slower than leaving it out.
//
// Peers are now measured and admitted only while admitting them helps.
// The second return is each ring member's accelerator index on its node, so a
// machine hosting two ranks pins a different device for each. The third is
// each member's measured share of a step, which the shim turns into a shard
// and a batch size.
func ddpChooseRing(o ddpRunOptions, rank0 registry.NodeRecord) ([]registry.NodeRecord, []int, []float64) {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", o.DaemonPort))
	if err != nil {
		return nil, nil, nil
	}
	defer resp.Body.Close()
	var all []registry.NodeRecord
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		return nil, nil, nil
	}

	byID := map[string]registry.NodeRecord{}
	var cands []ddpplace.Candidate
	for _, n := range all {
		if n.State != "healthy" {
			continue
		}
		// GPU remains a hard requirement rather than something to trade off:
		// an all-reduce must not silently mix devices, and the probe measures
		// CPU throughput, so it cannot rank a GPU against a CPU anyway.
		if o.RequireGPU && !ddpHasGPU(n) {
			continue
		}
		if ddpExcluded(o, n) {
			continue
		}
		byID[n.NodeID] = n
		cands = append(cands, ddpplace.Candidate{
			NodeID:   n.NodeID,
			Host:     ddpExtractHost(n.SSHEndpoint),
			Port:     n.DaemonPort,
			Cores:    n.Load.TotalCPUs,
			MemBytes: n.Load.AvailableMemBytes,
			HasGPU:   ddpHasGPU(n),
			Slots:    ddpSlots(n),
		})
	}
	cands = ddpOneDeviceKind(cands, o.RequireGPU || o.PreferGPU)
	if len(cands) == 0 {
		return nil, nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// PIPEDPEER_DDP_EQUAL_BATCHES=1 gives every rank the same batch whatever
	// placement measured. It exists so the two can be compared on one rig -
	// "shares made this faster" is a claim that needs the other run to stand
	// against - and as an escape hatch if weighting ever misbehaves.
	proportional := os.Getenv("PIPEDPEER_DDP_EQUAL_BATCHES") != "1"

	plan := ddpplace.Select(ctx, cands, ddpplace.Options{
		Max:         o.Nodes,
		Token:       authtoken.Current(),
		ProbeMillis: 250,
		// The shim sizes each rank's shard and batch by its share, so a
		// slower device takes proportionally less and the ring stops running
		// at the slowest rank's pace. Without it the admission rule has to
		// exclude anything materially slower, because with equal batches it
		// would set the pace for everyone.
		ProportionalBatches: proportional,
		// An explicit --ddp N is the user's call: every rank they asked for
		// runs, the slower ones on a smaller share. Only the auto ring prunes.
		KeepAll: o.MaxRanks > 0,
		// No working-set size is threaded through here yet, so a node too
		// small for the model is still admitted and fails at run time rather
		// than being declined. ddpplace supports the gate; the estimate lives
		// in the caller and has not been plumbed down.
	})

	if !plan.Measured {
		say.Printf("      Note: no peer answered the throughput probe; placement is " +
			"using advertised core counts, which ignore load and any cap on the peer\n")
	}
	for _, r := range plan.Rejected {
		say.Printf("      Not using %s: %s\n", shortID(r.Candidate.NodeID), r.Reason)
	}

	// GPU peers first among the admitted, for the same reason the gate kept
	// them: a mixed ring is not something the shim can currently balance.
	sort.SliceStable(plan.Chosen, func(i, j int) bool {
		return plan.Chosen[i].Candidate.HasGPU && !plan.Chosen[j].Candidate.HasGPU
	})

	out := make([]registry.NodeRecord, 0, len(plan.Chosen))
	slots := make([]int, 0, len(plan.Chosen))
	weights := make([]float64, 0, len(plan.Chosen))
	for _, c := range plan.Chosen {
		n := byID[c.Candidate.NodeID]
		if n.NodeID == "" {
			continue
		}
		if n.DaemonPort == 0 {
			n.DaemonPort = o.DaemonPort
		}
		out = append(out, n)
		slots = append(slots, c.Slot)
		if proportional {
			weights = append(weights, c.Weight)
		}
	}
	// The coordinator's pick is kept only if measurement agrees it belongs.
	if len(out) == 0 && rank0.NodeID != "" {
		return []registry.NodeRecord{rank0}, []int{0}, []float64{1}
	}
	return out, slots, weights
}
