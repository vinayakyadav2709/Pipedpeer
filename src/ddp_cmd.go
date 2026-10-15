package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/ddpplace"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/app"
	"github.com/pipedpeer/pipedpeer/internal/coordinator"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
)

type ddpRunOptions struct {
	Ctx            context.Context // cancelling it stops every rank
	Nodes          int
	DaemonPort     int
	MasterPort     int
	ScriptPath     string
	ScriptArgs     []string
	JobName        string
	Isolate        bool
	RegistryURL    string
	PythonVersion  string
	Pkgs           []string
	Envs           []string
	GPUDevices     string
	RequireGPU     bool
	PreferGPU      bool
	RequiredGPUMem int64
	RequiredCores  int
	NoSelf         bool
	// ExcludeMachine keeps every rank off the machine with this key (run
	// --offload); SelfNodeID is this node, for NoSelf. runDDP fills it.
	ExcludeMachine string
	SelfNodeID     string
	Strategy       string
	MemOverride    string
	// MaxRanks caps the ring when ranks join mid-run (--ddp N); 0 lets every
	// eligible machine that comes back or turns up join.
	MaxRanks int
}

// runDDP launches a transparent multi-rank training run: rank 0 is placed by
// the coordinator, ranks 1..K-1 come from the daemon's healthy node store,
// and every rank gets RANK/WORLD_SIZE/MASTER_ADDR/MASTER_PORT plus
// PIPEDPEER_DDP=1, which makes the sitecustomize shim initialize the torch
// process group and wrap DistributedDataParallel without touching user code.
func runDDP(o ddpRunOptions) error {
	absScript, err := filepath.Abs(o.ScriptPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %v", err)
	}

	nodeID, err := identity.GetOrCreate()
	if err != nil {
		return err
	}
	if _, err := daemonctl.EnsureStarted(nodeID.NodeID, o.DaemonPort); err != nil {
		return err
	}

	userMem := resourceest.ParseMemString(o.MemOverride)
	resReq := resourceest.EstimateFromScript(o.ScriptPath, nil, userMem)

	rrFn := func(count int) int {
		url := fmt.Sprintf("http://127.0.0.1:%d/v1/roundrobin?count=%d", o.DaemonPort, count)
		resp, err := http.Get(url)
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		var result struct{ Index int }
		json.NewDecoder(resp.Body).Decode(&result)
		return result.Index
	}

	coord := coordinator.New(coordinator.Config{
		RegistryURL:      o.RegistryURL,
		SelfIdentity:     nodeID,
		SelfSSH:          fmt.Sprintf("root@%s:22", detectLocalIP()),
		SelfDaemon:       o.DaemonPort,
		SelfLoad:         heartbeat.CollectLoad(0, 0),
		JobName:          o.JobName,
		RequiredMemBytes: resReq.MemBytes,
		RequiredCores:    o.RequiredCores,
		RequiredGPUMem:   o.RequiredGPUMem,
		NoSelf:           o.NoSelf,
		ExcludeMachine:   o.ExcludeMachine,
		RequireGPU:       o.RequireGPU,
		PreferGPU:        o.PreferGPU,
		Strategy:         o.Strategy,
		RoundRobinFn:     rrFn,
	})

	// Waits like a single-node run does: a ring started straight after
	// another one (a rejoin run, say) sees the last run's memory and VRAM
	// still in the nodes' reported load for a few seconds, and one look
	// failed with a bare "no candidates available".
	ctx := o.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	wait := placementWait()
	decision := coord.FindNodeWithin(ctx, wait, time.Second, placementProgress("for rank 0 ", wait))
	rank0 := decision.ChosenNode
	if rank0.NodeID == "" {
		if why := decision.RejectionSummary(); why != "" {
			fmt.Print("[coordinator] nodes turned down:\n" + why)
		}
		return fmt.Errorf("no node eligible for rank 0: %s", decision.Reason)
	}
	if rank0.DaemonPort == 0 {
		rank0.DaemonPort = o.DaemonPort
	}

	// Rank 0 is a candidate like any other. The coordinator picks it for
	// availability rather than speed, and on an uneven cluster that put the
	// slowest machine in the ring's most important seat: every rank waits for
	// it, and it also serves as the rendezvous master.
	o.SelfNodeID = nodeID.NodeID
	ring, ringSlots, ringWeights := ddpChooseRing(o, rank0)
	if len(ring) < 2 {
		// Measurement says one machine beats any ring available. That is an
		// answer, not a failure: erroring out here left the user with nothing
		// to show for a perfectly runnable job, on the grounds that the
		// cluster was not worth using. The caller runs it on one node.
		return errRingNotWorthIt
	}
	if len(ring) < o.Nodes {
		// Fewer ranks than asked for, because more would be slower. Said out
		// loud: silently running a smaller ring than requested is the kind of
		// thing that should never be discovered from a log file.
		fmt.Printf("      Using %d rank(s) rather than %d: the rest would slow the ring down\n",
			len(ring), o.Nodes)
		o.Nodes = len(ring)
	}
	// The lead has to be a node every other rank can reach, and a peer
	// reached over the internet is registered at 127.0.0.1 on a port only
	// this daemon forwards. Reordering rather than dropping: such a node is
	// still a good rank, it just cannot be the one everybody syncs to.
	if reordered, ok := ddpLeadFirst(ring, ddpSelfMachine(o.DaemonPort)); ok {
		ring = reordered
	} else {
		return fmt.Errorf("no rank in this ring can lead it: every candidate is " +
			"reached through a local forwarder, so the address they would all " +
			"sync to points at themselves. A ring needs at least one node with " +
			"an address the others can use - this machine, or a peer on a " +
			"network they share")
	}

	rank0, peers := ring[0], ring[1:o.Nodes]
	ringSlots = ringSlots[:o.Nodes]
	ringWeights = ddpNormaliseWeights(ringWeights[:o.Nodes])

	masterAddr := ddpExtractHost(rank0.SSHEndpoint)
	if masterAddr == "" || masterAddr == "127.0.0.1" || masterAddr == "localhost" {
		// Rank 0 is this machine, and every other rank has to reach it. Which
		// of our addresses that is depends on where they are: asking the
		// route to a peer beats picking one address for everybody, which on a
		// machine that is on both a LAN and a Tailscale network picks the LAN
		// one and hangs the run.
		peerHost := ""
		if len(peers) > 0 {
			peerHost = ddpExtractHost(peers[0].SSHEndpoint)
		}
		masterAddr = localIPFor(peerHost)
	}
	masterPort := o.MasterPort
	if masterPort == 0 {
		// ponytail: fixed base port (torch convention 29500) so a firewall is
		// opened ONCE for the small range instead of per-run. The port is
		// negotiated live against the leader host: connection refused =
		// nothing listening = safe to claim; occupied = next in range. Two
		// simultaneous DDP runs whose leaders land on the same machine fit
		// as long as they stay inside the span — beyond that, pin an exact
		// --ddp-port. Future zero-firewall option: tunnel the rendezvous
		// through the daemons' already-open 38080 channel (~a day of work,
		// makes the orchestrator a relay, extra hop to debug).
		masterPort = ddpPickMasterPort(masterAddr, 29500, 10)
	}

	fmt.Printf("=== Distributed training: %d ranks ===\n", o.Nodes)
	fmt.Printf("rank 0: %s (%s:%d)\n", rank0.NodeID[:min(8, len(rank0.NodeID))], masterAddr, rank0.DaemonPort)
	for i, n := range peers {
		where := ""
		if ddpSharesNode(ring[:o.Nodes], i+1) {
			// Two ranks on one machine is unusual enough that not saying so
			// would read as a mistake in the node list.
			where = fmt.Sprintf(" device %d", ringSlots[i+1])
		}
		fmt.Printf("rank %d: %s (%s:%d)%s\n", i+1, n.NodeID[:min(8, len(n.NodeID))],
			ddpExtractHost(n.SSHEndpoint), n.DaemonPort, where)
	}
	if uneven := ddpUneven(ringWeights); uneven {
		fmt.Printf("shares: %s — measured, so a slower rank takes less rather "+
			"than setting the pace for everyone\n", ddpSharePercents(ringWeights))
	}
	if os.Getenv("PIPEDPEER_DDP_BACKEND") == "gloo" || os.Getenv("PIPEDPEER_DDP_BACKEND") == "nccl" {
		fmt.Printf("MASTER_ADDR=%s MASTER_PORT=%d backend=%s\n\n", masterAddr, masterPort, os.Getenv("PIPEDPEER_DDP_BACKEND"))
	} else {
		fmt.Printf("sync: daemon channel via %s:%d (no extra ports)\n\n", masterAddr, rank0.DaemonPort)
	}

	env, err := app.BuildEnvironment(absScript, app.EnvOptions{
		PythonVersion: o.PythonVersion,
		Pkgs:          o.Pkgs,
		Intercept:     true, // DDP transparency lives in the sitecustomize shim
		GPU:           o.RequireGPU || o.PreferGPU,
	}, func(step int, title string) {
		if step == 1 {
			fmt.Printf("[%d/7] %s\n", step, title)
		} else {
			fmt.Printf("\n[%d/7] %s\n", step, title)
		}
	})
	if err != nil {
		return err
	}
	defer env.Close()

	jobSet := ddpGroupName(o.JobName, time.Now())

	ranks := make([]registry.NodeRecord, 0, o.Nodes)
	ranks = append(ranks, rank0)
	ranks = append(ranks, peers...)
	nodesAtStart, err := ddpFetchNodes(o.DaemonPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ddp] cannot read the node list (%v); every machine that turns up later counts as new\n", err)
	}
	up := newDDPUpState(nodesAtStart)
	var ranksMu sync.Mutex

	errs := make(chan ddpRankResult)
	// launch starts rank i on n. A joining rank takes the ring's size and
	// shares from the lead daemon when it gets in, not from here.
	launch := func(ctx context.Context, i int, n registry.NodeRecord, join bool) {
		ranksMu.Lock()
		ring := append([]registry.NodeRecord(nil), ranks...)
		ranksMu.Unlock()
		world := len(ring)
		up.started(n.NodeID)
		go func() {
			// Ranks sharing a machine must not share its accelerator: both
			// taking device 0 is not two ranks of work, it is two ranks
			// queueing on one GPU. An explicit --gpu-id is the user overriding
			// that on purpose and is left alone.
			rankGPUDevices := o.GPUDevices
			if rankGPUDevices == "" && !join && ddpSharesNode(ring, i) {
				rankGPUDevices = strconv.Itoa(ringSlots[i])
			}
			rankEnvs := append([]string(nil), o.Envs...)
			rankEnvs = append(rankEnvs,
				"PIPEDPEER_DDP=1",
				fmt.Sprintf("PIPEDPEER_RANK=%d", i),
				fmt.Sprintf("PIPEDPEER_WORLD_SIZE=%d", world),
				fmt.Sprintf("RANK=%d", i),
				fmt.Sprintf("WORLD_SIZE=%d", world),
				fmt.Sprintf("MASTER_ADDR=%s", masterAddr),
				fmt.Sprintf("MASTER_PORT=%d", masterPort),
				// The address THIS rank would reach us at, which is not
				// necessarily the one another rank would use.
				fmt.Sprintf("PIPEDPEER_SUBMITTER=%s",
					net.JoinHostPort(localIPFor(ddpExtractHost(n.SSHEndpoint)),
						strconv.Itoa(o.DaemonPort))),
				// Sync rides the lead rank's daemon on the one port that
				// already works between every pair of nodes; ranks never
				// open sockets of their own (MASTER_* stays only for the
				// gloo escape hatch, PIPEDPEER_DDP_BACKEND=gloo).
				fmt.Sprintf("PIPEDPEER_DDP_SYNC=http://%s/v1/ddp/sync",
					net.JoinHostPort(masterAddr, strconv.Itoa(rank0.DaemonPort))),
				fmt.Sprintf("PIPEDPEER_DDP_GROUP=%s", jobSet),
				// How many machines, not how many ranks. The ring's
				// worth-it verdict compares against "this machine doing
				// every shard", and ranks that share a machine do not
				// multiply what it can do.
				fmt.Sprintf("PIPEDPEER_DDP_MACHINES=%d", ddpDistinctMachines(ring)),
				// Names for the start barrier's "waiting for rank 1 on gpu1".
				"PIPEDPEER_DDP_HOSTS="+ddpHostList(ring),
			)
			if join {
				rankEnvs = append(rankEnvs, "PIPEDPEER_DDP_JOIN=1")
			} else if len(ringWeights) == len(ring) {
				// Every rank gets the whole set of shares, not just its own:
				// a contiguous shard needs the offsets of everything before
				// it, and a rank that knew only its own size could not tell
				// where its slice began.
				rankEnvs = append(rankEnvs,
					"PIPEDPEER_DDP_WEIGHTS="+ddpWeightList(ringWeights))
			}
			// Every rank posts its gradients to the lead rank's daemon, so
			// every rank needs the credential that daemon requires. Without
			// this the sync is refused with 401 - and because the refusal
			// arrives before the request body has finished uploading, what
			// the user sees is a BrokenPipeError from deep inside urllib,
			// which names neither authentication nor the daemon. DDP was
			// simply broken on any cluster with a token set.
			if tok := authtoken.Current(); tok != "" {
				rankEnvs = append(rankEnvs, "PIPEDPEER_TOKEN="+tok)
			}
			opts := app.Options{
				ScriptPath:        absScript,
				DaemonHost:        ddpExtractHost(n.SSHEndpoint),
				DaemonPort:        n.DaemonPort,
				TargetID:          n.NodeID,
				JobName:           o.JobName,
				JobSet:            jobSet,
				Isolate:           o.Isolate,
				Intercept:         true,
				SkipBroadcast:     true,
				GPU:               o.PreferGPU || o.RequireGPU,
				GPUDevices:        rankGPUDevices,
				Mode:              "script",
				PythonVersion:     o.PythonVersion,
				Envs:              rankEnvs,
				Pkgs:              o.Pkgs,
				ScriptArgs:        o.ScriptArgs,
				ResultsDir:        filepath.Join(".", "results", fmt.Sprintf("ddp-rank-%d", i)),
				PlacementSource:   "ddp",
				EstimatedMemBytes: resReq.MemBytes,
				EstimationTier:    resReq.Tier,
			}
			err := app.RunTask(ctx, env, app.Task{Options: opts, Script: absScript, Label: fmt.Sprintf("rank-%d", i)})
			up.ended(n.NodeID, err != nil && ctx.Err() == nil, time.Now())
			errs <- ddpRankResult{rank: i, err: err}
		}()
	}
	// Each starting rank can be stopped on its own: one that missed the start
	// barrier may still be receiving its closure when the ring has finished.
	stopRank := make([]context.CancelFunc, len(ranks))
	for i, n := range ranks {
		ctx, cancel := context.WithCancel(o.Ctx)
		defer cancel()
		stopRank[i] = cancel
		launch(ctx, i, n, false)
	}

	hosts := make([]string, len(ranks))
	usageNodes := make([]app.UsageNode, len(ranks))
	for i, n := range ranks {
		hosts[i] = ddpHostName(n)
		usageNodes[i] = app.UsageNode{Name: hosts[i], Host: ddpExtractHost(n.SSHEndpoint), Port: n.DaemonPort}
	}
	// ponytail: ranks that join later are not sampled; add them in start() if the demo needs it.
	usage := app.StartUsage(usageNodes)
	leadHost := ddpExtractHost(rank0.SSHEndpoint)
	backend := os.Getenv("PIPEDPEER_DDP_BACKEND")
	daemonSync := backend != "gloo" && backend != "nccl"

	// Machines that come back, or turn up, join the ring. Only the daemon
	// sync can grow: torch's own mesh is fixed at init.
	added := make(chan ddpAdded)
	startRanks := len(ranks) // ranks from here on are joiners
	watchCtx, stopWatch := context.WithCancel(o.Ctx)
	defer stopWatch()
	if daemonSync {
		gpuKind := o.RequireGPU || o.PreferGPU
		gpuRing := gpuKind && ddpHasGPU(rank0)
		g := &ddpGrower{
			st:         up,
			daemonPort: o.DaemonPort,
			leadHost:   leadHost,
			leadPort:   rank0.DaemonPort,
			group:      jobSet,
			maxRanks:   o.MaxRanks,
			out:        os.Stdout,
			fetch:      ddpFetchNodes,
			check:      ddpJoinCheck,
			want: func(n registry.NodeRecord) bool {
				if ddpExcluded(o, n) {
					return false
				}
				// One kind of device per ring, as at the start.
				return !gpuKind || ddpHasGPU(n) == gpuRing
			},
			nextRank: func() int {
				ranksMu.Lock()
				defer ranksMu.Unlock()
				return len(ranks)
			},
			start: func(rank int, n registry.NodeRecord) bool {
				if n.DaemonPort == 0 {
					n.DaemonPort = o.DaemonPort
				}
				ctx, cancel := context.WithCancel(o.Ctx)
				select {
				case added <- ddpAdded{rank: rank, host: ddpHostName(n), cancel: cancel}:
				case <-watchCtx.Done():
					cancel()
					return false
				}
				ranksMu.Lock()
				ranks = append(ranks, n)
				ranksMu.Unlock()
				fmt.Printf("[ddp] rank %d: starting on %s (%s:%d); it joins at the ring's next epoch\n",
					rank, ddpHostName(n), ddpExtractHost(n.SSHEndpoint), n.DaemonPort)
				launch(ctx, rank, n, true)
				return true
			},
		}
		go g.run(watchCtx)
	}

	firstErr := ddpAwaitRanks(o.Ctx, os.Stdout, errs, hosts, stopRank, added, func(rank int, err error) {
		// The script failed, so the run is lost: stop looking for machines
		// to add (ddpAwaitRanks stops any that slip through). A joiner's
		// crash is the join failing, as in ddpAwaitRanks.
		if rank < startRanks && app.IsPermanent(err) && !ddpMissedStart(err) {
			stopWatch()
		}
		// torch's own mesh (gloo/nccl) cannot lose a member; only the
		// daemon-backed sync can be told to carry on.
		if !daemonSync {
			return
		}
		ranksMu.Lock()
		world := len(ranks)
		ranksMu.Unlock()
		if err := ddpReportDown(leadHost, rank0.DaemonPort, jobSet, rank, world, err); err != nil {
			fmt.Fprintf(os.Stderr, "[ddp] could not tell the lead daemon rank %d is gone (%v); the ring waits 45 s per sync for it instead\n", rank, err)
		}
	})
	stopWatch()
	usage.Print(os.Stdout)
	if firstErr != nil {
		if os.Getenv("PIPEDPEER_DDP_BACKEND") == "gloo" || os.Getenv("PIPEDPEER_DDP_BACKEND") == "nccl" {
			fmt.Printf("[ddp] hint: if a rank timed out rendezvousing on %s:%d, the port range is likely firewalled — open it once with `sudo ufw allow 29500:29510/tcp` on the rank-0 machine, or pin an exact port with --ddp-port\n", masterAddr, masterPort)
		} else {
			fmt.Printf("[ddp] hint: sync runs on the lead daemon at %s:%d — check that every rank can curl that address, and `pipedpeer traffic` there for arriving /v1/ddp/sync posts\n", masterAddr, rank0.DaemonPort)
		}
	}
	return firstErr
}

// ddpPickMasterPort picks the DDP rendezvous port by probing the leader host
// live: an accepted dial means occupied (skip to the next), connection refused
// means nothing is listening there yet — safe to claim for rank 0's store. A
// dial timeout is ambiguous (firewall drop) and indistinguishable across the
// whole range, so return the base and let the shim's bounded rendezvous
// timeout surface it as a clear failure instead of hanging forever.
func ddpPickMasterPort(host string, base, span int) int {
	for p := base; p < base+span; p++ {
		c, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(p)), time.Second)
		if err != nil {
			return p
		}
		c.Close()
	}
	return base
}

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

// ddpLeadFirst reorders a ring so its lead is a node every rank can reach.
//
// Reordered rather than filtered: a node that cannot lead is still a
// perfectly good rank, and dropping it would shrink a ring for no reason.
// Returns false when nothing in the ring can lead, which has to be said
// rather than papered over - picking one anyway hangs every rank at the
// first barrier with no indication why.
func ddpLeadFirst(ring []registry.NodeRecord, selfMachine string) ([]registry.NodeRecord, bool) {
	for i, n := range ring {
		if ddpForwardedLead(n, selfMachine) {
			continue
		}
		if i == 0 {
			return ring, true
		}
		out := make([]registry.NodeRecord, 0, len(ring))
		out = append(out, ring[i])
		out = append(out, ring[:i]...)
		out = append(out, ring[i+1:]...)
		return out, true
	}
	return ring, false
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

// ddpExcluded reports whether n may not take a rank: this node under NoSelf
// (--remote), and any node on the machine --offload keeps work off, by its
// machine key.
func ddpExcluded(o ddpRunOptions, n registry.NodeRecord) bool {
	if o.NoSelf && o.SelfNodeID != "" && n.NodeID == o.SelfNodeID {
		return true
	}
	return heartbeat.SameMachine(n.Capabilities[heartbeat.MachineCapability], o.ExcludeMachine)
}

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
		if n.State != "healthy" {
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
		fmt.Printf("      Note: no peer answered the throughput probe; placement is " +
			"using advertised core counts, which ignore load and any cap on the peer\n")
	}
	for _, r := range plan.Rejected {
		fmt.Printf("      Not using %s: %s\n", shortID(r.Candidate.NodeID), r.Reason)
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
