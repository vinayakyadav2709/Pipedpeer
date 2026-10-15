package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/app"
	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/cgroups"
	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/daemonapi"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/discovery"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/internet"
	"github.com/pipedpeer/pipedpeer/internal/logging"
	"github.com/pipedpeer/pipedpeer/internal/rendezvous"
	"github.com/pipedpeer/pipedpeer/internal/tlsid"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// rememberEvery is how often the introducers that answered are written back
// to cluster.json, so a restart (or a machine that moved) still knows who to
// ask even if the address it joined with is gone.
const rememberEvery = 5 * time.Minute

// daemonCluster is what the daemon knows about its cluster: cluster.json,
// read by the daemon itself (B4: a daemon started by systemd or `start` has
// no flags to carry it), and the token (PIPEDPEER_TOKEN overrides the file).
func daemonCluster() (clustercfg.Config, string) {
	cfg, err := clustercfg.Load()
	if err != nil {
		log := logging.WithComponent("daemon")
		log.Warn().Err(err).Str("path", clustercfg.Path()).
			Msg("cluster config unreadable; running without introducers or roles")
		cfg = clustercfg.Config{}
	}
	return cfg, authtoken.Current()
}

// daemonRole is the C5 role this node advertises. Introducer-only wins over
// pool-only: it is the stricter of the two.
func daemonRole(cfg clustercfg.Config) string {
	switch {
	case cfg.NoJobs:
		return heartbeat.RoleIntroducerOnly
	case cfg.PoolOnly:
		return heartbeat.RolePoolOnly
	}
	return ""
}

// answeredIntroducers is the introducers worth remembering: ones that replied
// and are not this machine's own loopback introducer.
func answeredIntroducers(states []internet.IntroducerState) []string {
	var out []string
	for _, st := range states {
		if st.LastErr != "" || st.LastReply.IsZero() {
			continue
		}
		host, _, err := net.SplitHostPort(st.Addr)
		if err != nil {
			continue
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			continue
		}
		out = append(out, st.Addr)
	}
	return out
}

// startPprof serves net/http/pprof on loopback only, when PIPEDPEER_PPROF
// names a port (or host:port, whose host is ignored). For the lab and
// performance hunts; never reachable from another machine.
func startPprof(val string) {
	log := logging.WithComponent("daemon")
	p := val
	if _, port, err := net.SplitHostPort(val); err == nil {
		p = port
	}
	if _, err := strconv.Atoi(p); err != nil {
		log.Warn().Str("value", val).Msg("ignoring invalid PIPEDPEER_PPROF; want a port")
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	addr := net.JoinHostPort("127.0.0.1", p)
	go func() {
		log.Info().Str("addr", addr).Msg("pprof listening")
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Warn().Err(err).Str("addr", addr).Msg("pprof listener stopped")
		}
	}()
}

func runDaemon(args []string) {
	log := logging.WithComponent("daemon")

	port := daemonctl.DefaultPort
	maxConcurrent := 0 // unlimited unless configured
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--port":
			if i+1 < len(args) {
				if _, err := fmt.Sscanf(args[i+1], "%d", &port); err != nil {
					log.Fatal().Str("value", args[i+1]).Msg("invalid --port")
				}
				i++
			}
		case "--max-concurrent":
			if i+1 < len(args) {
				if _, err := fmt.Sscanf(args[i+1], "%d", &maxConcurrent); err != nil || maxConcurrent < 0 {
					log.Fatal().Str("value", args[i+1]).Msg("invalid --max-concurrent")
				}
				i++
			}
		}
	}
	if v := os.Getenv("PIPEDPEER_PPROF"); v != "" {
		startPprof(v)
	}

	cfg, token := daemonCluster()
	heartbeat.Role = daemonRole(cfg)
	heartbeat.Version = version

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

	advertiser := discovery.NewAdvertiser(discovery.ServiceInfo{
		NodeID:     nodeID.NodeID,
		DaemonPort: port,
		Arch:       nodeID.Arch,
		Hostname:   nodeID.Hostname,
	})
	if err := advertiser.Start(); err != nil {
		log.Warn().Err(err).Msg("LAN advertiser did not start; peers on this network find us only by subnet sweep")
	}

	// Teach the daemon who it is and how to find peers. Both feed the node
	// store, which is what the orchestrator reads when placing a task.
	server.SetSelfInfo(detectLocalIP(), port, heartbeat.Capabilities(nodeID))
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
				NodeID:     n.NodeID,
				Host:       n.Host,
				DaemonPort: n.DaemonPort,
				Arch:       n.Arch,
				Hostname:   n.Hostname,
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

	// Internet mode runs whenever this machine belongs to a cluster, LAN-only
	// clusters too: every member serves the cluster's introducer (answering
	// only this cluster id), so any member can introduce the others. The node
	// registers with every configured introducer, learns who else is in the
	// cluster, opens a direct path to each and registers it as an ordinary
	// node - nothing downstream has to know a peer is remote.
	//
	// Never fatal: a machine with no internet, or introducers that are down,
	// still runs local jobs and still uses its LAN peers.
	internetCtx, stopInternet := context.WithCancel(context.Background())
	defer stopInternet()
	if token == "" {
		log.Info().Msg("no cluster token: internet mode and introducer off (run `pipedpeer setup` or `connect`)")
	} else if key, kerr := identity.Key(); kerr != nil {
		log.Warn().Err(kerr).Msg("no signing key: internet mode disabled")
	} else {
		cert, certErr := tlsid.EnsureCert()
		if certErr != nil {
			log.Warn().Err(certErr).Msg("internet mode needs a certificate for direct connections")
		}
		mgr := internet.New(internet.Config{
			Introducers:     cfg.Introducers,
			ServeIntroducer: true,
			Cluster:         rendezvous.ClusterID(token),
			Secret:          token,
			Key:             key,
			Cert:            &cert,
			DaemonPort:      port,
			OnPeer: func(node, addr string) {
				host, p, ok := splitPeerAddr(addr)
				if !ok {
					log.Warn().Str("peer", node).Str("addr", addr).Msg("could not parse internet peer address")
					return
				}
				if err := server.AddPeer(host, p); err != nil {
					log.Warn().Err(err).Str("peer", node).Msg("could not register internet peer")
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
				host, p, ok := splitPeerAddr(addr)
				if !ok {
					log.Warn().Str("peer", node).Str("addr", addr).Msg("could not parse departed peer address")
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
		server.SetIntroducerStatus(func() []daemonapi.IntroducerState {
			states := mgr.Introducers()
			out := make([]daemonapi.IntroducerState, len(states))
			for i, st := range states {
				out[i] = daemonapi.IntroducerState(st)
			}
			return out
		})
		server.SetHinter(mgr.Hint)
		go mgr.Run(internetCtx)
		go func() {
			t := time.NewTicker(rememberEvery)
			defer t.Stop()
			for {
				select {
				case <-internetCtx.Done():
					return
				case <-t.C:
				}
				addrs := answeredIntroducers(mgr.Introducers())
				if len(addrs) == 0 {
					continue
				}
				if err := clustercfg.RememberIntroducers(addrs...); err != nil {
					log.Warn().Err(err).Strs("introducers", addrs).Msg("could not remember introducers in cluster config")
				}
			}
		}()
		log.Info().Strs("introducers", cfg.Introducers).Str("role", heartbeat.Role).Msg("internet mode enabled; serving the cluster introducer")
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		stopInternet()
		server.StopWarmWorkers()
		server.SavePoolStats()
		advertiser.Stop()
		os.Exit(0)
	}()

	// The daemon lives until os.Exit, so the watchdog needs no cancel.
	cgroups.StartCeilingWatchdog(context.Background(), func(format string, args ...any) {
		log.Warn().Msgf(format, args...)
	})

	// Detached jobs started here finish here (results, record) when they end.
	go app.WatchDetached(context.Background(), log, port)

	log.Info().Int("port", port).Str("node_id", nodeID.ShortID()).
		Int("max_concurrent", maxConcurrent).Str("role", heartbeat.Role).Msg("daemon starting")
	if err := server.ListenAndServe(port); err != nil {
		log.Fatal().Err(err).Msg("daemon listen failed")
	}
}

// splitPeerAddr splits the host:port internet mode hands over.
func splitPeerAddr(addr string) (string, int, bool) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, false
	}
	p, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, false
	}
	return host, p, true
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
