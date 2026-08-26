package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"net"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/nattype"
	"github.com/pipedpeer/pipedpeer/internal/rendezvous"
	"github.com/spf13/cobra"
)

// newNetCheckCmd reports whether this machine can be reached directly from
// another network, which decides how internet mode has to work here.
//
// The answer is not a property of pipedpeer, it is a property of the router in
// front of it, and it differs per network. Measuring it beats assuming it:
// building hole punching for networks that refuse it wastes the effort, and
// building only a relay for networks that would allow direct connections
// wastes the bandwidth - on a public server that is a bill.
func newNetCheckCmd() *cobra.Command {
	var servers []string
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "net-check",
		Short: "Report whether this machine can accept direct connections from the internet",
		Long: "Sends probes from one local port to two addresses on a public reflector and " +
			"compares the source address each of them saw. Run `pipedpeer rendezvous` on a " +
			"machine with a public address to provide the reflector.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(servers) == 0 {
				servers = nattype.DefaultSTUNServers
			}
			if len(servers) < 2 {
				return fmt.Errorf("need two --server addresses on the public side; " +
					"the test is a comparison, and with one there is nothing to compare")
			}
			fmt.Printf("Probing %s ...\n", strings.Join(servers, ", "))
			res, err := nattype.Probe(cmd.Context(), servers, timeout)
			if err != nil {
				return err
			}
			switch {
			case res.Blocked:
				fmt.Println("\n  No reflector answered.")
				fmt.Println("  Outbound UDP appears to be blocked here, which rules out every")
				fmt.Println("  direct transport. Check that the reflector is running and that")
				fmt.Println("  its port is open, then that this network permits outbound UDP.")
			case res.Mapping == nattype.EndpointIndependent:
				fmt.Printf("\n  Seen from outside as %s\n", res.External)
				fmt.Println("  This router presents the same address to every destination, so a")
				fmt.Println("  peer can at least be told where to send.")
				switch res.Filter {
				case nattype.FilterOpen:
					fmt.Println("  It also accepts packets from hosts it has never written to,")
					fmt.Println("  so an introduced peer's first packet will arrive.")
				case nattype.FilterRestricted:
					fmt.Println("  It only accepts packets from hosts it has already written to.")
					fmt.Println("  Simultaneous punching is supposed to work through that, and on")
					fmt.Println("  mobile carrier networks it often does not.")
				default:
					fmt.Println("  Whether it accepts a stranger's first packet was not tested;")
					fmt.Println("  a public STUN server cannot answer that. Point --server at a")
					fmt.Println("  `pipedpeer rendezvous` to find out.")
				}
				fmt.Println("\n  Mapping is only half the question. For the real answer, run")
				fmt.Println("  `pipedpeer net-punch` against a machine you want to reach.")
			default:
				fmt.Printf("\n  Not directly reachable (seen as %s)\n",
					strings.Join(res.Seen, ", "))
				fmt.Println("  This router allocates a different address per destination, so the")
				fmt.Println("  address a peer would be given was never valid for that peer.")
				fmt.Println("  Traffic to this machine has to be relayed.")
			}
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&servers, "server", nil,
		"reflector or STUN addresses, host:port (default: two public STUN servers)")
	cmd.Flags().DurationVar(&timeout, "timeout", 3*time.Second, "per-probe timeout")
	return cmd
}

// newNetPunchCmd tries to open a direct path to a peer across two home
// routers, which is the question internet mode turns on: if this works, the
// relay is a fallback rather than the design.
func newNetPunchCmd() *cobra.Command {
	var peer string
	var localPort int
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "net-punch",
		Short: "Open a direct connection to a peer behind another router",
		Long: "Learns this machine's external address for a fixed local port and prints it, " +
			"then, given the peer's, sends and listens until something gets through. Both " +
			"sides must run at roughly the same time: each one's outbound packets are what " +
			"lets its router accept the other's.",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, ext, err := nattype.BindAndDiscover(cmd.Context(), localPort, nil, 3*time.Second)
			if err != nil {
				return err
			}
			defer conn.Close()
			fmt.Printf("local port %d is seen from outside as %s\n", localPort, ext)
			if peer == "" {
				fmt.Println("no --peer given; run this on the other machine and pass each " +
					"the address the other printed")
				return nil
			}
			fmt.Printf("punching towards %s ...\n", peer)
			ok, took, st, err := nattype.Punch(cmd.Context(), conn, peer, timeout)
			if err != nil {
				return err
			}
			fmt.Printf("sent %d packet(s) (%d failed), received %d (%d from the peer)\n",
				st.Sent, st.SendErrs, st.Received, st.FromOther)
			if st.LastErr != "" {
				fmt.Printf("last send error: %s\n", st.LastErr)
			}
			if ok {
				fmt.Printf("\n  DIRECT PATH OPEN after %.1fs\n", took.Seconds())
				fmt.Println("  These two networks allow a direct connection, so traffic between")
				fmt.Println("  them does not have to be relayed.")
				return nil
			}
			fmt.Printf("\n  No direct path after %.0fs\n", took.Seconds())
			fmt.Println("  Either the far side was not punching at the same time, or one of")
			fmt.Println("  these routers will not allow it. Traffic would have to be relayed.")
			return fmt.Errorf("no direct path")
		},
	}
	cmd.Flags().StringVar(&peer, "peer", "", "the peer's external address, host:port")
	cmd.Flags().IntVar(&localPort, "local-port", 41641, "local UDP port (must match on both runs)")
	cmd.Flags().DurationVar(&timeout, "timeout", 20*time.Second, "how long to keep trying")
	return cmd
}

// newNetJoinCmd is the whole path end to end: ask a rendezvous where we
// appear from and who else is there, then open a direct connection to them.
//
// This is what internet mode does, reduced to one command so it can be checked
// on real networks rather than argued about.
func newNetJoinCmd() *cobra.Command {
	var server, node, token string
	var localPort int
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "net-join",
		Short: "Register with a rendezvous and connect directly to whoever else is there",
		RunE: func(cmd *cobra.Command, args []string) error {
			if server == "" {
				return fmt.Errorf("--rendezvous is required: something on a public " +
					"address has to hold the address book, because neither peer can " +
					"discover the address the other appears from")
			}
			if node == "" {
				id, err := identity.GetOrCreate()
				if err != nil {
					return fmt.Errorf("this machine has no node identity: %w", err)
				}
				node = id.ShortID()
			}
			if token == "" {
				token = authtoken.Current()
			}
			cluster := rendezvous.ClusterID(token)

			conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: localPort})
			if err != nil {
				return err
			}
			defer conn.Close()

			// Poll rather than register once and act on the answer. Whoever
			// arrives first would otherwise see an empty cluster and leave,
			// and the second would find an address whose owner stopped
			// punching a minute ago - which is exactly what happened: "peer
			// yeet at ... (last seen 43s ago)", 50 packets sent, nothing
			// back. Re-registering also refreshes the router's mapping, which
			// is what keeps the published address true.
			deadline := time.Now().Add(timeout)
			connected := map[string]bool{}
			var lastYou string
			var everSaw bool

			for time.Now().Before(deadline) {
				you, peers, err := rendezvous.Register(conn, server, cluster, node, 5*time.Second)
				if err != nil {
					return err
				}
				if you != lastYou {
					fmt.Printf("registered as %s in cluster %s; we appear from %s\n",
						node, cluster, you)
					lastYou = you
				}
				for _, p := range peers {
					if connected[p.Node] {
						continue
					}
					everSaw = true
					// A short attempt each round rather than one long one, so
					// every peer gets tried and the registration keeps being
					// refreshed while we work through them.
					ok, took, st, err := nattype.Punch(cmd.Context(), conn, p.Addr, punchRound)
					switch {
					case err != nil:
						return err
					case ok:
						connected[p.Node] = true
						fmt.Printf("  %s at %s: DIRECT after %.1fs (%d sent, %d received)\n",
							p.Node, p.Addr, took.Seconds(), st.Sent, st.Received)
					}
				}
				if everSaw && len(connected) == len(peers) && len(peers) > 0 {
					fmt.Printf("\nconnected directly to %d peer(s), no relay\n", len(connected))
					return nil
				}
			}

			if !everSaw {
				fmt.Println("nobody else registered before the timeout — run this on the " +
					"other machine at the same time")
				return nil
			}
			return fmt.Errorf("could not open a direct path to every peer within %s; "+
				"those would need a relay", timeout)
		},
	}
	cmd.Flags().StringVar(&server, "rendezvous", "", "rendezvous address book, host:port")
	cmd.Flags().StringVar(&node, "node", "", "this node's name (default: its node id)")
	cmd.Flags().StringVar(&token, "token", "", "cluster token (default: this machine's)")
	cmd.Flags().IntVar(&localPort, "local-port", 45641, "local UDP port")
	cmd.Flags().DurationVar(&timeout, "timeout", 60*time.Second,
		"how long to keep polling and punching")
	return cmd
}

// punchRound is how long to spend on one peer before going back to the
// rendezvous. Short, so every peer is tried each cycle and the registration
// stays fresh.
const punchRound = 4 * time.Second

// newRendezvousCmd runs the public half: a reflector that tells whoever asks
// where their packets appear to come from.
func newRendezvousCmd() *cobra.Command {
	var addrs []string
	var bookAddr string
	var ttl time.Duration

	cmd := &cobra.Command{
		Use:   "rendezvous",
		Short: "Run the public-side reflector that net-check probes",
		Long: "Answers every UDP packet with the source address it arrived from. Two " +
			"addresses are served because the test compares them; give two ports, or two " +
			"addresses on different interfaces if this host has more than one.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(addrs) == 0 {
				addrs = []string{":38443", ":38444"}
			}
			ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			errs := make(chan error, len(addrs)+1)
			// Bound as a pair so each can answer from the other, which is what
			// the filtering half of the test needs.
			var conns []net.PacketConn
			for _, a := range addrs {
				pc, err := net.ListenPacket("udp", a)
				if err != nil {
					return fmt.Errorf("reflector on %s: %w", a, err)
				}
				conns = append(conns, pc)
			}
			for i, pc := range conns {
				other := conns[(i+1)%len(conns)]
				if len(conns) == 1 {
					other = nil
				}
				go func(pc, other net.PacketConn) {
					fmt.Printf("reflector listening on %s\n", pc.LocalAddr())
					errs <- nattype.ReflectPair(ctx, pc, other)
				}(pc, other)
			}

			// The address book, on its own port. Peers register here and are
			// told where their cluster's other members appear from; they then
			// talk directly, so nothing but addresses passes through.
			if bookAddr != "" {
				pc, err := net.ListenPacket("udp", bookAddr)
				if err != nil {
					return fmt.Errorf("rendezvous address book: %w", err)
				}
				stop := make(chan struct{})
				go func() { <-ctx.Done(); close(stop) }()
				srv := rendezvous.NewServer(ttl)
				go func() {
					fmt.Printf("address book listening on %s (peers expire after %s)\n",
						bookAddr, ttl)
					errs <- srv.Serve(pc, stop)
				}()
			}
			select {
			case <-ctx.Done():
				return nil
			case err := <-errs:
				if err != nil {
					return err
				}
			}
			<-ctx.Done()
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&addrs, "listen", nil,
		"addresses to reflect on (default :38443 and :38444)")
	cmd.Flags().StringVar(&bookAddr, "book", ":38445",
		"address for the peer address book (empty to run reflectors only)")
	cmd.Flags().DurationVar(&ttl, "ttl", 2*time.Minute,
		"how long a peer stays listed without checking in")
	return cmd
}
