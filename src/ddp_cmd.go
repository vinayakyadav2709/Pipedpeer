package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"

	"github.com/pipedpeer/pipedpeer/internal/app"
	"github.com/pipedpeer/pipedpeer/internal/coordinator"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
	"github.com/pipedpeer/pipedpeer/internal/say"
)

type ddpRunOptions struct {
	Ctx            context.Context // cancelling it stops every rank
	Nodes          int
	DaemonPort     int
	ScriptPath     string
	ScriptArgs     []string
	JobName        string
	Isolate        bool
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
// and every rank gets RANK/WORLD_SIZE plus PIPEDPEER_DDP=1, which makes the
// sitecustomize shim sync the model through the lead rank's daemon without
// touching user code.
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
	resReq := resourceest.EstimateForRun(o.ScriptPath, o.ScriptArgs, nil, userMem)

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
	if o.Ctx == nil {
		o.Ctx = context.Background()
	}
	ctx := o.Ctx
	wait := placementWait()
	decision := coord.FindNodeWithin(ctx, wait, time.Second, placementProgress("for rank 0 ", wait))
	rank0 := decision.ChosenNode
	if rank0.NodeID == "" {
		if why := decision.RejectionSummary(); why != "" {
			fmt.Fprint(os.Stderr, "[coordinator] nodes turned down:\n"+why)
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
		say.Printf("      Using %d rank(s) rather than %d: the rest would slow the ring down\n",
			len(ring), o.Nodes)
		o.Nodes = len(ring)
	}
	// The lead has to be a node every other rank can reach, and a peer
	// reached over the internet is registered at 127.0.0.1 on a port only
	// this daemon forwards. Reordering rather than dropping: such a node is
	// still a good rank, it just cannot be the one everybody syncs to. Slots
	// and shares move with it: they are per ring position.
	selfMachine := ddpSelfMachine(o.DaemonPort)
	lead := ddpLeadIndex(ring, selfMachine)
	if lead < 0 {
		return fmt.Errorf("no rank in this ring can lead it: every candidate is " +
			"reached through a local forwarder, so the address they would all " +
			"sync to points at themselves. A ring needs at least one node with " +
			"an address the others can use - this machine, or a peer on a " +
			"network they share")
	}

	ring, ringSlots = ddpToFront(ring, lead), ddpToFront(ringSlots, lead)
	if len(ringWeights) == len(ring) {
		ringWeights = ddpToFront(ringWeights, lead)
	}

	rank0, peers := ring[0], ring[1:o.Nodes]
	ringSlots = ringSlots[:o.Nodes]
	if len(ringWeights) >= o.Nodes {
		ringWeights = ddpNormaliseWeights(ringWeights[:o.Nodes])
	}

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
	say.Printf("=== Distributed training: %d ranks ===\n", o.Nodes)
	say.Printf("rank 0: %s (%s:%d)\n", rank0.NodeID[:min(8, len(rank0.NodeID))], masterAddr, rank0.DaemonPort)
	for i, n := range peers {
		where := ""
		if ddpSharesNode(ring[:o.Nodes], i+1) {
			// Two ranks on one machine is unusual enough that not saying so
			// would read as a mistake in the node list.
			where = fmt.Sprintf(" device %d", ringSlots[i+1])
		}
		say.Printf("rank %d: %s (%s:%d)%s\n", i+1, n.NodeID[:min(8, len(n.NodeID))],
			ddpExtractHost(n.SSHEndpoint), n.DaemonPort, where)
	}
	if uneven := ddpUneven(ringWeights); uneven {
		say.Printf("shares: %s — measured, so a slower rank takes less rather "+
			"than setting the pace for everyone\n", ddpSharePercents(ringWeights))
	}
	say.Printf("sync: daemon channel via %s:%d (no extra ports)\n\n", masterAddr, rank0.DaemonPort)

	// Each rank gets the environment its node can use: CUDA torch on an
	// NVIDIA node, the CPU build of the same torch version elsewhere. One
	// environment for the whole ring shipped the cu126 wheel set (~4 GB) to
	// every CPU-only rank of a ring a GPU machine leads.
	rankCUDA := func(n registry.NodeRecord) bool {
		return o.RequireGPU || (o.PreferGPU && hasCUDA(n.Capabilities))
	}
	var envMu sync.Mutex
	builtEnvs := map[bool]*app.Environment{}
	defer func() {
		for _, e := range builtEnvs {
			e.Close()
		}
	}()
	envFor := func(cuda bool) (*app.Environment, error) {
		envMu.Lock()
		defer envMu.Unlock()
		if e := builtEnvs[cuda]; e != nil {
			return e, nil
		}
		if len(builtEnvs) > 0 {
			say.Printf("\n[ddp] building the %s environment for ranks on %s nodes\n",
				map[bool]string{true: "CUDA", false: "CPU"}[cuda], map[bool]string{true: "NVIDIA", false: "CPU-only"}[cuda])
		}
		e, err := app.BuildEnvironment(ctx, absScript, app.EnvOptions{
			PythonVersion: o.PythonVersion,
			Pkgs:          o.Pkgs,
			Intercept:     true, // DDP transparency lives in the sitecustomize shim
			GPU:           cuda,
			Verbose:       say.Verbose,
		}, func(step int, title string) {
			if step == 1 {
				say.Printf("[%d/7] %s\n", step, title)
			} else {
				say.Printf("\n[%d/7] %s\n", step, title)
			}
		})
		if err != nil {
			return nil, err
		}
		builtEnvs[cuda] = e
		return e, nil
	}
	if _, err := envFor(rankCUDA(rank0)); err != nil {
		return err
	}
	for _, n := range peers {
		if _, err := envFor(rankCUDA(n)); err != nil {
			return err
		}
	}

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
				// For a script that sets up torch.distributed itself
				// (native DDP): the shim's own sync needs neither. Nothing
				// is probed or opened for it; such a script needs the port
				// reachable on the lead machine.
				fmt.Sprintf("MASTER_ADDR=%s", masterAddr),
				fmt.Sprintf("MASTER_PORT=%d", ddpNativeMasterPort),
				// The address THIS rank would reach us at, which is not
				// necessarily the one another rank would use.
				fmt.Sprintf("PIPEDPEER_SUBMITTER=%s",
					net.JoinHostPort(localIPFor(ddpExtractHost(n.SSHEndpoint)),
						strconv.Itoa(o.DaemonPort))),
				// Sync rides the lead rank's daemon on the one port that
				// already works between every pair of nodes; ranks never
				// open sockets of their own.
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
			rankEnvs = append(rankEnvs, ddpThreadEnv(o.Envs, ring, i)...)
			if o.MaxRanks >= 2 {
				// --ddp N: the user chose the ring, so the shim never shrinks
				// it or changes the sync interval on its own (C2).
				rankEnvs = append(rankEnvs, "PIPEDPEER_DDP_FIXED=1")
			}
			// One sync per machine for ranks sharing a node, or the
			// butterfly for a ring of 3+ machines (never both: every rank
			// must agree on where each round goes). A joiner gets neither:
			// the ring it joins is no longer the one these were built for.
			if !join {
				if local := ddpLocalRanks(ring); local[i] != "" {
					rankEnvs = append(rankEnvs, "PIPEDPEER_DDP_LOCAL="+local[i])
				} else if peers := ddpPeerURLs(ring, i, selfMachine); peers != "" && len(local) == 0 {
					rankEnvs = append(rankEnvs, "PIPEDPEER_DDP_PEERS="+peers)
				}
			}
			// One-step-delayed gradient overlap is opt-in until a benchmark
			// shows its loss (it applies each gradient a step late).
			if os.Getenv("PIPEDPEER_DDP_OVERLAP") == "1" {
				rankEnvs = append(rankEnvs, "PIPEDPEER_DDP_OVERLAP=1")
			}
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
				GPU:               rankCUDA(n),
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
			env, err := envFor(rankCUDA(n))
			if err != nil {
				up.ended(n.NodeID, ctx.Err() == nil, time.Now())
				errs <- ddpRankResult{rank: i, err: err}
				return
			}
			err = ddpRunWatched(ctx, opts.DaemonHost, opts.DaemonPort, o.DaemonPort, func(ctx context.Context) error {
				return app.RunTask(ctx, env, app.Task{Options: opts, Script: absScript, Label: fmt.Sprintf("rank-%d", i)})
			})
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
	var usage *app.Usage
	if say.Verbose {
		usage = app.StartUsage(usageNodes)
	}
	leadHost := ddpExtractHost(rank0.SSHEndpoint)

	// Machines that come back, or turn up, join the ring.
	added := make(chan ddpAdded)
	watchCtx, stopWatch := context.WithCancel(o.Ctx)
	defer stopWatch()
	{
		gpuKind := o.RequireGPU || o.PreferGPU
		gpuRing := gpuKind && ddpHasGPU(rank0)
		g := &ddpGrower{
			st:         up,
			daemonPort: o.DaemonPort,
			leadHost:   leadHost,
			leadPort:   rank0.DaemonPort,
			group:      jobSet,
			maxRanks:   o.MaxRanks,
			out:        ddpProgressOut(),
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
				say.Printf("[ddp] rank %d: starting on %s (%s:%d); it joins at the ring's next epoch\n",
					rank, ddpHostName(n), ddpExtractHost(n.SSHEndpoint), n.DaemonPort)
				launch(ctx, rank, n, true)
				return true
			},
		}
		go g.run(watchCtx)
	}

	// Failures go to stderr: stdout is the script's.
	firstErr := ddpAwaitRanks(o.Ctx, os.Stderr, errs, hosts, stopRank, added, func(rank int, err error) {
		ranksMu.Lock()
		world := len(ranks)
		ranksMu.Unlock()
		if err := ddpReportDown(leadHost, rank0.DaemonPort, jobSet, rank, world, err); err != nil {
			fmt.Fprintf(os.Stderr, "[ddp] could not tell the lead daemon rank %d is gone (%v); the ring waits 45 s per sync for it instead\n", rank, err)
		}
	})
	stopWatch()
	if usage != nil {
		usage.Print(os.Stdout)
	}
	if firstErr != nil {
		say.Printf("[ddp] hint: sync runs on the lead daemon at %s:%d — check that every rank can reach that address\n", masterAddr, rank0.DaemonPort)
	}
	return firstErr
}

// ddpThreadVars cap a rank's CPU threads. torch sizes its intra-op pool
// from OMP_NUM_THREADS, so no torch call is needed.
var ddpThreadVars = []string{"OMP_NUM_THREADS", "MKL_NUM_THREADS"}

// ddpThreadEnv gives ring member i an equal share of its machine's cores:
// max(1, cores / ranks on that machine). Without it every rank on a shared
// machine starts torch/OpenMP/MKL with all cores and they oversubscribe (M3:
// ranks-per-node exponent 1.21 where 1.0 was expected). A variable the user
// set with --env is left alone; a node reporting no core count gets nothing.
//
// ponytail: cores = the node's reported CPU count, which ignores a cgroup
// quota (only the node's daemon sees that, as PIPEDPEER_CPU_LIMIT). Have the
// daemon divide its limit too if capped multi-rank nodes matter.
func ddpThreadEnv(user []string, ring []registry.NodeRecord, i int) []string {
	n := ring[i]
	cores := n.Load.TotalCPUs
	if cores <= 0 {
		cores, _ = strconv.Atoi(n.Capabilities["cpu_cores"])
	}
	if cores <= 0 {
		return nil
	}
	machine := func(m registry.NodeRecord) string {
		if k := m.Capabilities[heartbeat.MachineCapability]; k != "" {
			return k
		}
		return m.NodeID
	}
	here := 0
	for _, m := range ring {
		if machine(m) == machine(n) {
			here++
		}
	}
	threads := strconv.Itoa(max(1, cores/here))
	var out []string
	for _, v := range ddpThreadVars {
		set := false
		for _, e := range user {
			if strings.HasPrefix(e, v+"=") {
				set = true
				break
			}
		}
		if !set {
			out = append(out, v+"="+threads)
		}
	}
	return out
}

// ddpNativeMasterPort is MASTER_PORT for scripts that start torch.distributed
// themselves (torch's own default). The shim's daemon sync uses no port.
const ddpNativeMasterPort = 29500
