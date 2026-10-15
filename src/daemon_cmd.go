package main

import (
	"context"
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/app"
	"github.com/pipedpeer/pipedpeer/internal/cgroups"
	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/daemonapi"
	"github.com/pipedpeer/pipedpeer/internal/discovery"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/internet"
	"github.com/pipedpeer/pipedpeer/internal/logging"
	"github.com/pipedpeer/pipedpeer/internal/natsbus"
	"github.com/pipedpeer/pipedpeer/internal/rendezvous"
	"github.com/pipedpeer/pipedpeer/internal/tlsid"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// daemonRendezvous is the introducer the daemon registers with: the flag, then
// the cluster saved by an earlier join.
func daemonRendezvous(flag string) string {
	addr, _ := clustercfg.Effective(flag)
	return addr
}

func runDaemon(args []string) {
	log := logging.WithComponent("daemon")

	var port int
	var maxConcurrent = -1
	var registryURL, sshEndpoint, natsURL, rendezvousAddr string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--port":
			if i+1 < len(args) {
				fmt.Sscanf(args[i+1], "%d", &port)
				i++
			}
		case "--max-concurrent":
			if i+1 < len(args) {
				fmt.Sscanf(args[i+1], "%d", &maxConcurrent)
				i++
			}
		case "--registry":
			if i+1 < len(args) {
				registryURL = args[i+1]
				i++
			}
		case "--ssh-endpoint":
			if i+1 < len(args) {
				sshEndpoint = args[i+1]
				i++
			}
		case "--rendezvous":
			if i+1 < len(args) {
				rendezvousAddr = args[i+1]
				i++
			}
		case "--nats-url":
			if i+1 < len(args) {
				natsURL = args[i+1]
				i++
			}
		}
	}
	if port == 0 {
		port = 38080
	}
	if registryURL == "" {
		registryURL = os.Getenv("PIPEDPEER_REGISTRY")
	}
	if natsURL == "" {
		natsURL = os.Getenv("PIPEDPEER_NATS_URL")
	}
	rendezvousAddr = daemonRendezvous(rendezvousAddr)
	if maxConcurrent < 0 {
		maxConcurrent = 0 // unlimited unless configured
		if v := os.Getenv("PIPEDPEER_MAX_CONCURRENT"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				maxConcurrent = n
			} else {
				log.Warn().Str("value", v).Msg("ignoring invalid PIPEDPEER_MAX_CONCURRENT")
			}
		}
	}

	nodeID, err := identity.GetOrCreate()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load identity")
	}

	server := daemonapi.New(nodeID.NodeID)
	server.EnablePersistence()
	// Both closure caches are bounded here rather than only when something is
	// transferred, so a node that has gone quiet still gives the disk back
	// and a lowered cap takes effect without waiting for the next job.
	server.BoundCaches()
	server.SetMaxConcurrentJobs(maxConcurrent)
	if v := os.Getenv("PIPEDPEER_SERIAL_CLOSURE_IMPORTS"); v == "1" {
		server.SetSerialClosureImports(true)
	}

	var bus *natsbus.Bus
	natsCfg := natsbus.Config{
		URL:      natsURL,
		Embedded: natsURL == "",
		Name:     "pipedpeer-daemon-" + nodeID.ShortID(),
	}
	bus, err = natsbus.New(natsCfg)
	if err != nil {
		log.Warn().Err(err).Msg("NATS unavailable, running HTTP-only")
	} else {
		if natsCfg.Embedded {
			log.Info().Str("url", bus.ClientURL()).Msg("started embedded NATS server")
		} else {
			log.Info().Str("url", natsURL).Msg("connected to external NATS")
		}
		if err := server.BindNATS(bus); err != nil {
			log.Warn().Err(err).Msg("failed to bind NATS subscriptions")
		}
	}

	var hbClient *heartbeat.Client
	sshEp := sshEndpoint
	if sshEp == "" {
		sshEp = fmt.Sprintf("root@%s:22", detectLocalIP())
	}
	if bus != nil {
		hbClient = heartbeat.NewNATSClient(bus, nodeID, sshEp, port)
	} else if registryURL != "" {
		hbClient = heartbeat.NewClient(registryURL, nodeID, sshEp, port)
	}
	if hbClient != nil {
		hbClient.SetJobCounter(server.ActiveJobs)
		hbClient.SetReservedMemFn(server.ReservedMem)
		_ = hbClient.Start()
	}

	advertiser := discovery.NewAdvertiser(discovery.ServiceInfo{
		NodeID:      nodeID.NodeID,
		DaemonPort:  port,
		SSHEndpoint: sshEp,
		Arch:        nodeID.Arch,
		Hostname:    nodeID.Hostname,
	})
	_ = advertiser.Start()

	// Teach the daemon who it is and how to find peers. Both feed the node
	// store, which is what the orchestrator reads when placing a task.
	server.SetSelfInfo(sshEp, port, heartbeat.Capabilities(nodeID))
	server.SetDiscoverFn(func() []daemonapi.NodeDiscovered {
		found := discovery.Discover(1 * time.Second)
		if len(found) == 0 {
			// ponytail: UDP broadcast first (free when the network allows
			// it); fall back to a direct /health sweep of the local subnet
			// only when it comes up empty (AP client isolation, multicast
			// filtering).
			found = discovery.DiscoverTCP(port, 3*time.Second)
		}
		out := make([]daemonapi.NodeDiscovered, 0, len(found))
		for _, n := range found {
			out = append(out, daemonapi.NodeDiscovered{
				NodeID:      n.NodeID,
				SSHEndpoint: n.SSHEndpoint,
				DaemonPort:  n.DaemonPort,
				Arch:        n.Arch,
				Hostname:    n.Hostname,
			})
		}
		return out
	})

	// Reap leases whose submitter never came back, so a crashed CLI does not
	// permanently consume a slot on this node's concurrency budget.
	server.StartSweeper()

	server.StartPeerPoller(10 * time.Second)

	// Multi-node interception: allow pool-map chunks to fan out to healthy
	// peers that share this closure. Local always participates, so a remote
	// node adds capacity but never slows a run down.
	server.EnablePoolSpill()

	// Internet mode: one address instead of four steps. The daemon registers
	// with a rendezvous, learns who else is in the cluster, opens a path to
	// each of them and registers that path as an ordinary node - so nothing
	// downstream has to know a peer is remote.
	//
	// Entirely optional and never fatal: a machine with no internet, or a
	// rendezvous that is down, still runs local jobs and still uses its LAN
	// peers.
	internetCtx, stopInternet := context.WithCancel(context.Background())
	defer stopInternet()
	if rendezvousAddr != "" {
		key, kerr := identity.Key()
		if kerr != nil {
			log.Warn().Err(kerr).Msg("no signing key: internet mode disabled")
		} else {
			cert, certErr := tlsid.EnsureCert()
			if certErr != nil {
				log.Warn().Err(certErr).Msg("internet mode needs a certificate for direct connections")
			}
			mgr := internet.New(internet.Config{
				Rendezvous: rendezvousAddr,
				Cluster:    rendezvous.ClusterID(authtoken.Current()),
				Secret:     authtoken.Current(),
				Key:        key,
				Cert:       &cert,
				DaemonPort: port,
				OnPeer: func(node, addr string) {
					host, portStr, err := net.SplitHostPort(addr)
					if err != nil {
						return
					}
					p, err := strconv.Atoi(portStr)
					if err != nil {
						return
					}
					if err := server.AddPeer(host, p); err != nil {
						log.Warn().Err(err).Str("peer", node).Msg("could not register relayed peer")
						return
					}
					log.Info().Str("peer", node).Str("at", addr).Msg("peer joined over the internet")
				},
				OnUnreachable: func(node, reason string) {
					// Recorded rather than relayed around. A peer with no
					// direct path is not offered work - the scheduler routes
					// around it - and the reason travels so a machine that
					// quietly stopped serving is visible instead of missing.
					log.Info().Str("peer", node).Str("reason", reason).
						Msg("peer has no direct path; not offering it work")
				},
				OnPeerGone: func(node, addr string) {
					host, portStr, err := net.SplitHostPort(addr)
					if err != nil {
						return
					}
					p, err := strconv.Atoi(portStr)
					if err != nil {
						log.Warn().Err(err).Str("peer", node).Str("addr", addr).Msg("could not parse departed peer address")
						return
					}
					if err := server.RemovePeer(host, p); err != nil {
						log.Warn().Err(err).Str("peer", node).Str("addr", addr).Msg("could not remove departed peer")
					}
					log.Info().Str("peer", node).Msg("peer left")
				},
				Log: func(format string, args ...any) {
					log.Info().Msgf("internet: "+format, args...)
				},
			})
			// So `pipedpeer nodes` can show how each peer is reached, and
			// a machine being routed around is visible rather than absent.
			server.SetPathReporter(mgr.Paths)
			server.SetUnreachableReporter(mgr.Unreachable)
			go mgr.Run(internetCtx)
			log.Info().Str("rendezvous", rendezvousAddr).Msg("internet mode enabled")
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		stopInternet()
		server.StopWarmWorkers()
		if hbClient != nil {
			hbClient.Stop()
		}
		advertiser.Stop()
		if bus != nil {
			bus.Close()
		}
		os.Exit(0)
	}()

	// The daemon lives until os.Exit, so the watchdog needs no cancel.
	cgroups.StartCeilingWatchdog(context.Background(), func(format string, args ...any) {
		log.Warn().Msgf(format, args...)
	})

	// Detached jobs started here finish here (results, record) when they end.
	go app.WatchDetached(context.Background(), log, port)

	log.Info().Int("port", port).Str("node_id", nodeID.ShortID()).
		Int("max_concurrent", maxConcurrent).Msg("daemon starting")
	if err := server.ListenAndServe(port); err != nil {
		log.Fatal().Err(err).Msg("daemon listen failed")
	}
}

// detectLocalIP returns the address peers should use to reach this machine.
// It is what the node advertises as its own endpoint and what a DDP rank 0
// hands out as MASTER_ADDR, so an address nobody can dial is worse than
// useless — every peer connects and hangs.
//
// Enumerating by interface rather than InterfaceAddrs matters: a /32 parked on
// lo (Tailscale leaves 198.18.x there) is not in 127.0.0.0/8, so IsLoopback
// passes it, and lo is interface index 1 so it is the first thing returned.
// That address then reached peers as this node's endpoint.
//
// Preference order is by how widely routable an address is: a LAN address
// beats a VPN one, because peers that can reach both should take the direct
// path.
// localIPFor is the address a given peer would see us at.
//
// detectLocalIP picks one address for everybody, preferring a private one -
// and a machine on both a home LAN and a Tailscale network has two, of which
// only one reaches the peer. Measured: a node advertised its stale LAN
// address, 192.168.0.201, to a peer that could only be reached over Tailscale.
// DDP worked anyway because the lead rank happened to be the reachable node;
// with the roles reversed the sync URL is an address the other rank cannot
// open, and the run hangs at the first barrier.
//
// The kernel already knows the answer. Connecting a UDP socket sends nothing
// - it only fixes the route - and the local address it picks is the source
// address a packet to that peer would carry.
//
// Falls back to detectLocalIP when there is no peer in question (plain
// self-advertisement) or the route cannot be resolved.
func localIPFor(target string) string {
	if target == "" {
		return detectLocalIP()
	}
	host := target
	if h, _, err := net.SplitHostPort(target); err == nil {
		host = h
	}
	if host == "" {
		return detectLocalIP()
	}
	ip, ok := routeSourceIP(host)
	if !ok {
		return detectLocalIP()
	}
	// Reaching a peer over loopback says nothing about how anybody else
	// reaches us, and advertising 127.0.0.1 to a second machine is the one
	// answer guaranteed to be wrong.
	if ip.IsLoopback() {
		return detectLocalIP()
	}
	return ip.String()
}

// routeSourceIP asks the kernel which of our addresses a packet to host would
// carry. This is the part that is per-peer; localIPFor is the policy around
// it, and separating them is what lets the mechanism be tested: asserting
// only that the result is a non-loopback address passes with the whole thing
// removed, because detectLocalIP returns one too.
//
// Bounded, because a hostname that does not resolve blocks in DNS for several
// seconds and this runs while a job is being placed. Falling back to the old
// answer quickly beats holding up the run to be exact.
func routeSourceIP(host string) (net.IP, bool) {
	// The port is immaterial: no packet is sent, and the route is chosen by
	// the destination address alone.
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.Dial("udp", net.JoinHostPort(host, "9"))
	if err != nil {
		return nil, false
	}
	defer conn.Close()
	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || addr.IP == nil || addr.IP.IsUnspecified() {
		return nil, false
	}
	return addr.IP, true
}

func detectLocalIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}

	var lan, other string
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
			if !ok {
				continue
			}
			ip := ipNet.IP.To4()
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
				continue
			}
			// 198.18.0.0/15 is the RFC 2544 benchmarking range: never a real
			// peer-reachable address, and exactly what gets parked on lo.
			if ip[0] == 198 && (ip[1] == 18 || ip[1] == 19) {
				continue
			}
			if ip.IsPrivate() && lan == "" {
				lan = ip.String()
			} else if other == "" {
				other = ip.String()
			}
		}
	}

	if lan != "" {
		return lan
	}
	if other != "" {
		return other
	}
	return "127.0.0.1"
}

// ensureUserBinOnPath puts the per-user bin directory on PATH if it is
// missing, so a crun installed there without root is found by every later
// exec.LookPath.
func ensureUserBinOnPath() {
	dir := userdir.Bin()
	for _, p := range filepath.SplitList(os.Getenv("PATH")) {
		if p == dir {
			return
		}
	}
	if _, err := os.Stat(dir); err != nil {
		return
	}
	os.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}
